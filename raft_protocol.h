#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    // Lab3: Your code here
    int can_term;
    int can_id;
    int last_log_idx;
    int last_log_term;
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    // Lab3: Your code here
    int curr_term;
    bool granted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command>
class log_entry {
public:
    // Lab3: Your code here
    int term;
    command cmd;
    int snap_idx;
    int idx;

    log_entry(){}
    log_entry(int _term,command _cmd,int _snap, int _idx):term(_term),cmd(_cmd),snap_idx(_snap),idx(_idx){}
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Lab3: Your code here
    m << entry.term;
    m << entry.cmd;
    m << entry.snap_idx;
    m << entry.idx;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Lab3: Your code here
    u >> entry.term;
    u >> entry.cmd;
    u >> entry.snap_idx;
    u >> entry.idx;
    return u;
}

template <typename command>
class append_entries_args {
public:
    // Your code here
    // leader's term 
    int term;
    // leader_id
    int leader_id;
    // the index of log prev of the newly append log
    int prev_log_idx;
    // the term of log prev of the newly append log
    int prev_log_term;
    // represent for the size of entries (0 for heartbeat)
    int size;
    // entries (may more the one for efficiency)
    std::vector<log_entry<command>> entries;
    // leader commit id
    int leader_com_id;
    // start index
    int start_idx;
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Lab3: Your code here
    m << args.term;
    m << args.leader_id;
    m << args.prev_log_idx;
    m << args.prev_log_term;
    m << args.size;
    for(int i = 0;i < args.size;++i){
        // int entry_size = args.entries[i].first.size();
        // char* buf = new char[entry_size];
        // args.entries[i].first.serialize(buf,entry_size);
        // for(int i = 0;i < entry_size;++i){
        //     m << buf[i];
        // }

        // m << args.entries[i].first;
        // m << args.entries[i].second;

        m << args.entries.at(i);

        // delete [] buf;
    }
    m << args.leader_com_id;
    m << args.start_idx;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Lab3: Your code here
    u >> args.term;
    u >> args.leader_id;
    u >> args.prev_log_idx;
    u >> args.prev_log_term;
    u >> args.size;
    for(int i = 0;i < args.size;++i){
        // command cmd;
        args.entries.push_back(log_entry<command>());
    }
    for(int i = 0;i < args.size;++i){
        // int entry_size = args.entries[i].first.size();
        // char* buf = new char[entry_size];
        // for(int i = 0;i < entry_size;++i){
        //     u >> buf[i];
        // }
        // args.entries[i].first.deserialize(buf,entry_size);
        // u >> args.entries[i].first;
        // u >> args.entries[i].second;

        u >> args.entries[i];

        // delete [] buf;
    }
    u >> args.leader_com_id;
    u >> args.start_idx;
    return u;
}

class append_entries_reply {
public:
    // Lab3: Your code here
    // current term
    int curr_term;
    bool is_succ;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Lab3: Your code here
    // leader's term
    int term;
    // leader's id
    int leader_id;
    // last included index
    int last_inc_idx;
    // last_inc_idx's term
    int last_inc_term;
    // unnecessary for offset(because we send it in a time)
    // data to be sended
    std::vector<char> data;
    // unnecessary for done
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &u, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Lab3: Your code here
    // the follower's term , for leader to update itself
    int curr_term;
    bool is_succ;
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h