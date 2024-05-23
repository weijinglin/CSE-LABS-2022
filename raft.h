#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include <iostream>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

using std::this_thread::sleep_for;

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);

    // #define RAFT_LOG(fmt, args...)                                                                                   \
    // do {                                                                                                         \
    //     auto now =                                                                                               \
    //         std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
    //             std::chrono::system_clock::now().time_since_epoch())                                             \
    //             .count();                                                                                        \
    //     printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    // } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure

    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */
    int voteFor;
    // a log contains index and term and command
    std::vector<log_entry<command>> logs;
    /* ---- Volatile state on all server----  */
    int committed_index;
    int last_applied;
    /* ---- Volatile state on leader----  */
    std::vector<int> next_idx;
    std::vector<int> match_idx;

    // TODO(wjl) : that is the variable added to check for heartbeat
    bool is_beat;

    // the num of vote got
    int vote_num;

    // record the vote term(ensure only one vote per term)
    int vote_log;

    // replicas agree num
    std::vector<std::pair<int,bool>> rep_num;

    // snap_shot in memory and will be used in save_snapshot
    std::vector<char> snap_mem;

    // core to the idx of the snap_mem(used for compare for writing)
    int snap_idx;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
    std::unique_lock<std::mutex> lock(mtx);
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    committed_index = 0;
    last_applied = 0;
    // TODO(wjl) : use the -1 to represent the null vote the term
    voteFor = -1;
    // at this time , the next idx is assign to be 1(but may be buggy after failure recovery)
    next_idx.assign(clients.size(),1);
    match_idx.assign(clients.size(),0);

    // set the heartbeat code to false
    is_beat = false;

    // init the srand code
    srand(unsigned(time(0)));
    // the num of vote got
    vote_num = 1;
    // tha largest vote num have been seen 
    vote_log = 0;

    // initial snap_idx
    snap_idx = 0;

    // replicas num
    // skip the zero index
    // rep_num.push_back(std::pair<int,bool>(0,true));
    this->snap_mem.clear();

    storage->load_snap(this->current_term,this->logs,this->snap_mem);
    if(this->snap_mem.size() != 0){
        this->state->apply_snapshot(this->snap_mem);
    }
    if(this->logs.size() == 0){
        log_entry<command> init_;
        init_.idx = 0;
        init_.snap_idx = 0;
        init_.term = 0;

        logs.push_back(init_);
    }
    storage->restore(this->current_term,this->voteFor,this->logs);
    // TODO(wjl) : buggy in persistent(fixed)
    // this->committed_index = this->logs.size() - 1;
    this->last_applied = this->logs.at(0).snap_idx;
    this->committed_index = this->last_applied + this->logs.size() - 1;
    RAFT_LOG("commit is %d and last_applied is %d\n",this->committed_index,this->last_applied);
    // do some apply job
    for(int i = 0;i < logs.size() + this->logs.at(0).snap_idx;++i){
        rep_num.push_back(std::pair<int,bool>(1,true));
    }    
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    // TODO(wjl) : some trick : re_save the log here 
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    if(this->role != leader){
        // RAFT_LOG("append a log reject\n");
        return false;
    } else {
        RAFT_LOG("append a log append\n");
        term = current_term;
        log_entry<command> a_log;
        a_log.cmd = cmd;
        a_log.term = term;
        a_log.idx = this->logs.size();
        a_log.snap_idx = a_log.idx + this->logs.at(0).snap_idx;
        this->logs.push_back(a_log);
        // index = this->logs.size() - 1;
        index = a_log.snap_idx;
        // init the vote
        rep_num.push_back(std::pair<int,bool>{1,false});
        // TODO(wjl) : may need to add asyn call to append log
    }
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    // TODO(wjl) : 
    std::unique_lock<std::mutex> lock(mtx);
    printf("snapshot\n");
    for(int i = this->last_applied - this->logs.at(0).snap_idx + 1; i < this->committed_index - this->logs.at(0).snap_idx + 1; ++i){
        this->state->apply_log(this->logs.at(i).cmd);
    }

    this->last_applied = this->committed_index;

    std::vector<char> data_snap = this->state->snapshot();
    // this->state = new state_machine();
    // this->state->apply_snapshot(data_snap);
    int snap_idx = this->logs.at(this->last_applied).snap_idx;
    int snap_term = this->logs.at(this->last_applied).term;

    // persistent the snap_shot
    RAFT_LOG("call in save snapshot\n");
    this->storage->do_snapshot(data_snap,snap_idx,snap_term);

    // clear the log
    this->storage->clear();

    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    // that is the code used to vote for others
    std::unique_lock<std::mutex> lock(mtx);
    // TODO(wjl) : to judge whether the request can be voted or not
    // TODO(wjl) : <= may be buggy
    // TODO(wjl) : we assume it vote for itself first
    if(args.can_term <= this->current_term){
        reply.granted = false;
        reply.curr_term = this->current_term;
        RAFT_LOG("reject because of term\n");
        return 0;
    }
    // check for the data is out of data or not
    if(args.last_log_idx < this->committed_index || logs.at(this->committed_index - this->logs.at(0).snap_idx).term > args.last_log_term){
        reply.granted = false;
        reply.curr_term = this->current_term;
        // update the term
        this->current_term = args.can_term;
        RAFT_LOG("reject because of update and update term to %d\n",args.can_term);
        return 0;
    }

    if(vote_log >= args.can_term){
        reply.granted = false;
        reply.curr_term = this->current_term;
        return 0;
    } else {
        vote_log = args.can_term;
    }

    is_beat = true;

    RAFT_LOG("accept and vote\n");

    if(this->role == leader){
        this->role = follower;
        next_idx.assign(this->rpc_clients.size(),1);
        match_idx.assign(this->rpc_clients.size(),0);
    }

    // granted
    reply.granted = true;
    reply.curr_term = this->current_term;
    this->voteFor = args.can_id;
    this->current_term = args.can_term;

    // persistent
    // TODO
    storage->save(this->current_term,this->voteFor);

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    // this is the code used to collect the agreement and upgrade the role
    std::unique_lock<std::mutex> lock(mtx);
    if(reply.curr_term > this->current_term){
        RAFT_LOG("receive refuse and update term to %d\n",reply.curr_term);
        this->current_term = reply.curr_term;
    }
    // it has vote for itself
    if(reply.granted == true && this->role == candidate){
        // add the num of agreement
        vote_num++;
        RAFT_LOG("the vote num is %d is from %d\n",vote_num, target);
        if(vote_num >= (this->rpc_clients.size() + 1)/ 2){
            this->role = leader;
            vote_num = 1;
            RAFT_LOG("become leader\n");
            // start to broadcast the heartbeat 
            return;
        }
    } else if (this->role == leader || this->role == follower){
        vote_num = 1;
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    // RAFT_LOG("get append req\n");
    std::unique_lock<std::mutex> lock(mtx);
    if(arg.size == 0){
        // heartbeat
        // TODO(wjl) : may need add lock to protect the is_beat
        is_beat = true;
        reply.curr_term = this->current_term;
        reply.is_succ = true;
        // if follower is out of data , update the term of follower
        if(arg.term > this->current_term){
            this->current_term = arg.term;
            reply.is_succ = false;
            RAFT_LOG("reject append because of update and update term to %d\n",arg.term);
        }
        // can use for request transfer
        this->leader_id = arg.leader_id;
        if(this->leader_id != this->my_id && this->role == leader){
            this->role = follower;
            next_idx.assign(this->rpc_clients.size(),1);
            match_idx.assign(this->rpc_clients.size(),0);
        }

        // rule 5
        RAFT_LOG("log size is %d\n",this->logs.size());
        if(arg.leader_com_id > this->committed_index && this->logs.size() + this->logs.at(0).snap_idx > arg.leader_com_id){
            this->committed_index = (arg.leader_com_id > (this->logs.size() + this->logs.at(0).snap_idx - 1) ?
             (this->logs.size() + this->logs.at(0).snap_idx - 1) : arg.leader_com_id);
             RAFT_LOG("after ping and the commit is %d\n",this->committed_index);
            // persistent
            // prepare for the persistent entries
            // TODO
            std::vector<log_entry<command>> per_entries;
            for(int i = 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
                per_entries.push_back(this->logs.at(i));
            }
            storage->append_logs(per_entries);
        }
        
        return 0;
    } else{
        // part2
        // can be seen as a heartbeat
        is_beat = true;

        // rule 1
        if(arg.term < this->current_term){
            reply.curr_term = this->current_term;
            reply.is_succ = false;
            return 0;
        }

        // rule 5
        // TODO(wjl) ; understand why update commitIndex in this way
        if(arg.leader_com_id > this->committed_index && this->logs.size() + this->logs.at(0).snap_idx > arg.leader_com_id){
            this->committed_index = (arg.leader_com_id > (this->logs.size() + this->logs.at(0).snap_idx - 1) 
            ? (this->logs.size() + this->logs.at(0).snap_idx - 1) : arg.leader_com_id);
    
            // persistent
            // prepare for the persistent entries
            // TODO
            std::vector<log_entry<command>> per_entries;
            for(int i = 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
                per_entries.push_back(this->logs.at(i));
            }
            storage->append_logs(per_entries);
        }

        // rule 2 : check for consistency
        int log_size = this->logs.size();
        // prevent seg fault

        // TODO(wjl) : fix the bug (when arg.start_idx < this->logs.at(0).snap_idx)

        // normal version
        int break_point = -1;
        for(int i = (arg.start_idx - this->logs.at(0).snap_idx < 0 ? 0 : arg.start_idx - this->logs.at(0).snap_idx)
        ; i < (log_size > arg.start_idx - this->logs.at(0).snap_idx + arg.size 
        ? arg.start_idx - this->logs.at(0).snap_idx + arg.size : log_size);++i){
            if(this->logs.at(i).term != arg.entries.at(i - arg.start_idx + this->logs.at(0).snap_idx).term){
                break_point = i;
                break;
            }
        }

        if(break_point == -1){
            // the record in the logs is right, so need to push_back
            if(arg.start_idx - this->logs.at(0).snap_idx + arg.size > log_size){
                for(int i = 0;i < arg.start_idx + arg.size - this->logs.at(0).snap_idx - log_size;++i){
                    this->logs.push_back(arg.entries.at(log_size + i - arg.start_idx + this->logs.at(0).snap_idx));
                    rep_num.push_back(std::pair<int,bool>{1,false});
                }
            } else {
                for(int i = arg.start_idx - this->logs.at(0).snap_idx + arg.size - 1;i >= log_size;--i){
                    this->logs.pop_back();
                    rep_num.pop_back();
                }
            }
        } else {
            // need to fix and push(maybe)
            for(int i = break_point;i < arg.start_idx - this->logs.at(0).snap_idx + arg.size;++i){
                if(i < log_size){
                    this->logs[i] = arg.entries.at(i - arg.start_idx + this->logs.at(0).snap_idx);
                } else {
                    this->logs.push_back(arg.entries.at(i - arg.start_idx + this->logs.at(0).snap_idx));
                    rep_num.push_back(std::pair<int,bool>{1,false});
                }
            }
        }

        RAFT_LOG("get append req and commit is %d\n",this->committed_index);
    } 
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    // may not need in part1 ? 
    // RAFT_LOG("receive ping\n");
    std::unique_lock<std::mutex> lock(mtx);
    if(this->role == leader){
        if(arg.size == 0){
            if(reply.curr_term > this->current_term && reply.is_succ == false){
                // leader is out of data
                RAFT_LOG("fix term\n");
                this->role = follower;
                this->current_term = reply.curr_term;
                next_idx.assign(this->rpc_clients.size(),1);
                match_idx.assign(this->rpc_clients.size(),0);
                RAFT_LOG("reject handle because of update and update term to %d\n",reply.curr_term);
                // the leader is out of date , so it don't need to response back
                return;
            } else if(reply.is_succ){
                // have heartbeat response and check for update
                if(match_idx[node] < this->committed_index){
                    RAFT_LOG("a node wake and resend new append data match is %d and commit is %d node is %d\n", match_idx[node], this->committed_index,node);
                    // append_entries_args<command> arg;
                    // arg.term = this->current_term;
                    // arg.leader_id = my_id;
                    // // RAFT_LOG("commit log is commit is %d and match is %d to node %d\n",this->committed_index,match_idx[i],i);
                    // arg.leader_com_id = (this->committed_index > match_idx[node] ? match_idx[node] : this->committed_index);
                    // arg.prev_log_idx = this->logs.size() - 2;
                    // arg.prev_log_term = this->logs.at(arg.prev_log_idx).term;
                    // for(int j = match_idx[node] + 1;j < arg.prev_log_idx + 2;++j){
                    //     arg.entries.push_back(this->logs.at(j));
                    // }
                    // arg.size = this->logs.size() - match_idx[node] - 1;
                    // arg.start_idx = match_idx[node] + 1;
                    // thread_pool->addObjJob(this, &raft::send_append_entries, node, arg);
                    
                    // TODO(wjl) : we can try snap_shot here (when a node wake)
                    install_snapshot_args arg;
                    // apply first
                    for(int i = this->last_applied - this->logs.at(0).snap_idx + 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
                        this->state->apply_log(this->logs.at(i).cmd);
                    }
                    this->last_applied = this->committed_index;

                    // get snapshot
                    std::vector<char> data = this->state->snapshot();
                    if(data.size() == 0){
                        // to avoid some warm such as part5 which not implement snapshot but use this code
                        // TODO(wjl) ： add some logic to handle this case
                        return; 
                    }
                    arg.data.assign(data.begin(),data.end());
                    arg.term = this->current_term;
                    arg.leader_id = this->my_id;
                    arg.last_inc_idx = this->committed_index;
                    arg.last_inc_term = this->logs.at(this->committed_index - this->logs.at(0).snap_idx).term;
                    
                    thread_pool->addObjJob(this, &raft::send_install_snapshot, node, arg);
                    
                }
            }
            return;
        }
        if(reply.curr_term > this->current_term && reply.is_succ == false){
            // leader is out of data
            RAFT_LOG("fix term\n");
            this->role = follower;
            this->current_term = reply.curr_term;
            next_idx.assign(this->rpc_clients.size(),1);
            match_idx.assign(this->rpc_clients.size(),0);
            // the leader is out of date , so it don't need to response back
            return;
        } else if(reply.is_succ == false){
            // send back for overwrite
            RAFT_LOG("wrong in handle append\n");
            // append_entries_args<command> arg_back;
            // arg_back.term = this->current_term;
            // arg_back.size = arg.prev_log_idx + 2;
            // arg_back.leader_id = my_id;
            // arg_back.leader_com_id = arg.leader_com_id;
            // arg_back.prev_log_idx = arg.prev_log_idx;
            // arg_back.prev_log_term = arg.prev_log_term;
            // for(int i = 0;i < arg_back.size;++i){
            //     arg_back.entries.push_back(this->logs.at(i));
            // }
            // thread_pool->addObjJob(this, &raft::send_append_entries, node, arg_back);
        } else {
            // we can see rep_num use the logic_idx
            match_idx[node] = std::max(arg.start_idx + arg.size - 1,this->match_idx[node]);
            if(rep_num.at(arg.start_idx + arg.size - 1).first >= (this->rpc_clients.size() + 1)/2){
                // have committed
                return;
            }
            rep_num.at(arg.start_idx + arg.size - 1).first++;
            if(rep_num.at(arg.start_idx + arg.size - 1).first >= (this->rpc_clients.size() + 1)/2){
                // because the interleaving of program stream
                this->committed_index = std::max(arg.start_idx + arg.size - 1,this->committed_index);
                // persistent
                // prepare for the persistent entries
                // persistent
                // prepare for the persistent entries
                // TODO
                std::vector<log_entry<command>> per_entries;
                for(int i = 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
                    per_entries.push_back(this->logs.at(i));
                }
                storage->append_logs(per_entries);

                // TODO(wjl) : launch a ping to other nodes to update commit
                RAFT_LOG("commit ping\n");

                for(int i = 0;i < this->rpc_clients.size();++i){
                    if(this->my_id == i){
                        continue;
                    } else {
                        append_entries_args<command> arg;
                        arg.term = this->current_term;
                        arg.size = 0;
                        arg.leader_id = my_id;
                        // RAFT_LOG("all log is commit is %d and match is %d to node %d\n",this->committed_index,match_idx[i],i);
                        arg.leader_com_id = (this->committed_index > match_idx[i] ? match_idx[i] : this->committed_index);
                        arg.prev_log_idx = 0;
                        arg.prev_log_term = 0;
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                    }
                }


            }
            RAFT_LOG("commit make progress and commit index is %d and id is %d and rep_num is %d\n",this->committed_index,
            this->my_id,rep_num.at(arg.start_idx + arg.size - 1).first);
        }
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    // rule 1
    RAFT_LOG("receive a snapshot\n");
    std::unique_lock<std::mutex> lock(mtx);
    if(args.term < this->current_term){
        reply.curr_term = this->current_term;
        reply.is_succ = false;
        RAFT_LOG("snapshot because of update and update term to %d\n",reply.curr_term);
        return 0;
    } else {
        if(args.term > this->current_term){
            this->current_term = args.term;
            this->role = follower;// some adjust
            voteFor = -1;
            next_idx.assign(this->rpc_clients.size(),1);
            match_idx.assign(this->rpc_clients.size(),0);
            // TODO(wjl) : do persistent
        }
        // TODO(to pass part4 test3) : judge the num of logs in part4 test3

        reply.curr_term = this->current_term;
        reply.is_succ = true;

        // just apply
        // rule 2,3,4 -- ignore
        if(this->logs.at(0).snap_idx < args.last_inc_idx){
            // compare the last log
            RAFT_LOG("try to apply a snapshot\n");
            bool is_done = false;
            for(int i = 0;i < this->logs.size();++i){
                if(this->logs.at(i).snap_idx == args.last_inc_idx && args.last_inc_term == this->logs.at(i).term){
                    is_done = true;
                    // remain the last logs
                    // update tht logs
                    std::vector<log_entry<command>> temp_logs;
                    log_entry<command> pre_log;
                    
                    // that will be used in next iteration
                    pre_log.snap_idx = this->logs.at(i).snap_idx;
                    pre_log.term = this->logs.at(i).term;
                    temp_logs.push_back(pre_log);

                    for(int j = 0; j + i + 1 < this->logs.size();++j){
                        log_entry<command> a_log;
                        a_log.cmd = this->logs.at(i + j + 1).cmd;
                        a_log.idx = j + 1;
                        a_log.snap_idx = this->logs.at(i + j + 1).snap_idx;
                        a_log.term = this->logs.at(i + j + 1).term;
                        temp_logs.push_back(a_log);
                    }

                    // update the snap_shot in memory
                    // this.state.apply_snapshot(args.data);
                    int log_size_pre = this->logs.size();
                    this->snap_mem.assign(args.data.begin(),args.data.end());
                    this->logs.assign(temp_logs.begin(),temp_logs.end());
                    RAFT_LOG("done and prev_size is %d and now size is %d\n",log_size_pre,this->logs.size());
                    // use logic idx to stored in committed idx
                    this->committed_index = std::max(args.last_inc_idx,this->committed_index);
                    if(this->last_applied > args.last_inc_idx){
                        // don't need to do update
                    } else {
                        this->state->apply_snapshot(this->snap_mem);
                        this->last_applied = args.last_inc_idx;
                        // do persistent job
                        RAFT_LOG("do persistent job done!\n");
                        this->storage->do_snapshot(this->snap_mem,args.last_inc_idx,args.term);
                        this->storage->clear();
                    }

                    return 0;
                }
            }

            if(!is_done){
                // delete all logs and save the snapshot because the last log is not consistent
                RAFT_LOG("is not done\n");
                this->snap_mem.assign(args.data.begin(),args.data.end());
                log_entry<command> pre_log;
                pre_log.idx = 0;
                pre_log.snap_idx = args.last_inc_idx;
                pre_log.term = args.term;

                std::vector<log_entry<command>> temp_logs;
                temp_logs.push_back(pre_log);
                int temp_size = this->logs.size();
                this->logs.assign(temp_logs.begin(),temp_logs.end());

                RAFT_LOG("done and prev_size is %d and now size is %d\n",temp_size,this->logs.size());

                this->state->apply_snapshot(this->snap_mem);

                // this->committed_index = std::max(args.last_inc_idx,this->committed_index);
                this->committed_index =  args.last_inc_idx;
                this->last_applied = args.last_inc_idx;

                // update the rep_num
                // first , get the num of logs
                // because the state of now totally equal to the snapshot
                int log_num = args.last_inc_idx + 1;
                rep_num.assign(log_num,{1,true});
            
                // persistent
                RAFT_LOG("do persistent job !done!\n");
                this->storage->do_snapshot(this->snap_mem,args.last_inc_idx,args.last_inc_term);
                this->storage->clear();
            }

        } else{
            // just ignore
            RAFT_LOG("old snapshot!!!\n");
        }
    

    }

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    if(reply.curr_term > this->current_term){
        this->role = follower;
        next_idx.assign(this->rpc_clients.size(),1);
        match_idx.assign(this->rpc_clients.size(),0);
        this->current_term = reply.curr_term;
        RAFT_LOG("handle snapshot because of update and update term to %d\n",reply.curr_term);
        return;
    } else if(reply.is_succ){
        // match_idx store the logic idx
        this->match_idx[node] = std::max(arg.last_inc_idx,this->match_idx[node]);
        // TODO(wjl) : because the snapshot has been applied, so the committed idx and applied idx don't need to be modified
        // just fix match fix last_inc_idx
        RAFT_LOG("send a snapshot to the new waked node and it accepted and match_idx of node%d is %d\n",node,arg.last_inc_idx);
        if(rep_num.at(arg.last_inc_idx).first >= (this->rpc_clients.size() + 1) / 2){
            return;
        } else {
            rep_num.at(arg.last_inc_idx).first++;
            if(rep_num.at(arg.last_inc_idx).first >= (this->rpc_clients.size() + 1)/2){
                // because the interleaving of program stream
                this->committed_index = std::max(arg.last_inc_idx,this->committed_index);

                RAFT_LOG("try ro persistent\n");


                // persistent
                // prepare for the persistent entries
                // persistent
                // prepare for the persistent entries
                // TODO
                std::vector<log_entry<command>> per_entries;
                for(int i = 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
                    per_entries.push_back(this->logs.at(i));
                }
                storage->append_logs(per_entries);


            }
        }
    } else {
        // is_succ is false ; the case is that the term is less then the node sended but update after sending
        // TODO(wjl) : resend
        // send snapshot
        install_snapshot_args arg;
        // apply first
        for(int i = this->last_applied - this->logs.at(0).snap_idx + 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
            this->state->apply_log(this->logs.at(i).cmd);
        }
        this->last_applied = this->committed_index;

        // get snapshot
        std::vector<char> data = this->state->snapshot();
        arg.data.assign(data.begin(),data.end());
        arg.term = this->current_term;
        arg.leader_id = this->my_id;
        arg.last_inc_idx = this->committed_index;
        arg.last_inc_term = this->logs.at(this->committed_index - this->logs.at(0).snap_idx).term;

        RAFT_LOG("be rejected and resend snapshot to %d and commit is %d\n",node,this->committed_index);
        
        thread_pool->addObjJob(this, &raft::send_install_snapshot, node, arg);
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.


    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        // that is time used for heartbeat time period and election timeout
        unsigned int sleep_time = rand() % 150 + 150;
        sleep_for(std::chrono::milliseconds(sleep_time));

        // add the judge of candidate is to deal with the split vote
        if(role == follower || role == candidate){
            // TODO(wjl) : may need to add a lock to protect the variable of is_beat
            mtx.lock();
            // printf("elect\n");
            if(!is_beat){
                RAFT_LOG("no beat and become candidate\n");
                this->role = candidate;
                // vote for itself
                voteFor = my_id;
                // add the current term
                this->current_term++;
                vote_log = this->current_term;
                vote_num = 1;

                // persistent
                // TODO
                storage->save(this->current_term,this->voteFor);

                for(int i = 0;i < this->rpc_clients.size();++i){
                    if(i == this->my_id){
                        continue;   
                    } else {
                        request_vote_args arg;
                        arg.can_id = this->my_id;
                        arg.can_term = this->current_term;
                        arg.last_log_idx = this->committed_index;
                        arg.last_log_term = logs[this->committed_index - this->logs.at(0).snap_idx].term;
                        // this->send_request_vote(i,arg);
                        thread_pool->addObjJob(this, &raft::send_request_vote, i, arg);
                    }
                }
            } else {
                // pass heartbeat check
                is_beat = false;
            }
            mtx.unlock();
        }
    }    

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        unsigned int _sleep_time = 1;
        sleep_for(std::chrono::milliseconds(_sleep_time));

        if(this->role == leader){
            // send to last log to all followers
            mtx.lock();
            if(this->logs.size() > 1 && this->logs.size() + this->logs.at(0).snap_idx > this->committed_index + 1 && rep_num.at(this->logs.size() + this->logs.at(0).snap_idx - 1).second == false){
                // printf("commit\n");
                RAFT_LOG("try to commit or send snapshot, term is %d\n",this->logs.back().term);
                for(int i = 0;i < this->rpc_clients.size();++i){
                    if(this->my_id == i){
                        continue;
                    } else if(this->match_idx[i] >= this->logs.at(0).snap_idx && this->logs.at(this->committed_index - this->logs.at(0).snap_idx).snap_idx < match_idx[i] + 50){
                        append_entries_args<command> arg;
                        arg.term = this->current_term;
                        arg.leader_id = my_id;
                        // RAFT_LOG("commit log is commit is %d and match is %d to node %d\n",this->committed_index,match_idx[i],i);
                        arg.leader_com_id = (this->committed_index > match_idx[i] ? match_idx[i] : this->committed_index);
                        // prev_idx is logic idx
                        arg.prev_log_idx = this->logs.size() - 2 + this->logs.at(0).snap_idx;
                        arg.prev_log_term = this->logs.at(arg.prev_log_idx - this->logs.at(0).snap_idx).term;
                        for(int j = match_idx[i] - this->logs.at(0).snap_idx + 1;j < arg.prev_log_idx - this->logs.at(0).snap_idx + 2;++j){
                            arg.entries.push_back(this->logs.at(j));
                        }
                        // arg.size = this->logs.size() - match_idx[i] - 1;
                        arg.size = arg.entries.size();
                        // start_idx is the logic idx
                        arg.start_idx = match_idx[i] + 1;

                        RAFT_LOG("send entries to %d and commit is %d and match is %d\n",i,arg.leader_com_id,match_idx[i]);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                        
                    } else {
                        // send snapshot
                        install_snapshot_args arg;
                        // apply first
                        for(int i = this->last_applied - this->logs.at(0).snap_idx + 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
                            this->state->apply_log(this->logs.at(i).cmd);
                        }
                        this->last_applied = this->committed_index;

                        // get snapshot
                        std::vector<char> data = this->state->snapshot();
                        arg.data.assign(data.begin(),data.end());
                        arg.term = this->current_term;
                        arg.leader_id = this->my_id;
                        arg.last_inc_idx = this->committed_index;
                        arg.last_inc_term = this->logs.at(this->committed_index - this->logs.at(0).snap_idx).term;

                        RAFT_LOG("send snapshot to %d and commit is %d\n",i,this->committed_index);
                        
                        thread_pool->addObjJob(this, &raft::send_install_snapshot, i, arg);
                    }
                }
                rep_num.at(this->logs.size() + this->logs.at(0).snap_idx - 1).second = true;
                // printf("finish commit\n");
            }
            mtx.unlock();
        }
    }    


    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        // printf("try ro append a log\n");
        // send heartbeat every 100ms
        unsigned time_period = 1;
        sleep_for(std::chrono::milliseconds(time_period));

        //do some change to state machine
        if(this->last_applied < this->committed_index){
            mtx.lock();
            printf("apply\n");

            for(int i = last_applied - this->logs.at(0).snap_idx + 1;i < this->committed_index - this->logs.at(0).snap_idx + 1;++i){
                this->state->apply_log(this->logs.at(i).cmd);
            }
            this->last_applied = this->committed_index;
            mtx.unlock();
        }
    }    

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.


    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        if(this->role == leader){
            // heartbeat check
            mtx.lock();
            RAFT_LOG("ping\n");

            for(int i = 0;i < this->rpc_clients.size();++i){
                if(this->my_id == i){
                    continue;
                } else {
                    append_entries_args<command> arg;
                    arg.term = this->current_term;
                    arg.size = 0;
                    arg.leader_id = my_id;
                    // RAFT_LOG("all log is commit is %d and match is %d to node %d\n",this->committed_index,match_idx[i],i);
                    arg.leader_com_id = (this->committed_index > match_idx[i] ? match_idx[i] : this->committed_index);
                    arg.prev_log_idx = 0;
                    arg.prev_log_term = 0;
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                }
            }
            // printf("finish ping\n");
            mtx.unlock();
        }

        // send heartbeat every 100ms
        unsigned time_period = 100;
        sleep_for(std::chrono::milliseconds(time_period));
    }    


    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h