#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args) {
    // Lab3: Your code here
    m << args.can_id;
    m << args.can_term;
    m << args.last_log_idx;
    m << args.last_log_term;
    return m;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
    // Lab3: Your code here
    u >> args.can_id;
    u >> args.can_term;
    u >> args.last_log_idx;
    u >> args.last_log_term;
    return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
    // Lab3: Your code here
    m << reply.curr_term;
    m << reply.granted;
    return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
    // Lab3: Your code here
    u >> reply.curr_term;
    u >> reply.granted;
    return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &args) {
    // Lab3: Your code here
    m << args.curr_term;
    m << args.is_succ;
    return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args) {
    // Lab3: Your code here
    m >> args.curr_term;
    m >> args.is_succ;
    return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
    // Lab3: Your code here
    m << args.term;
    m << args.leader_id;
    m << args.last_inc_term;
    m << args.last_inc_idx;
    int size = args.data.size();
    m << size;
    for(int i = 0;i < args.data.size();++i){
        m << args.data.at(i);
    }
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
    // Lab3: Your code here
    u >> args.term;
    u >> args.leader_id;
    u >> args.last_inc_term;
    u >> args.last_inc_idx;
    int size;
    u >> size;
    for(int i = 0;i < size;++i){
        char buf;
        u >> buf;
        args.data.push_back(buf);
    }
    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    m << reply.curr_term;
    m << reply.is_succ;
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
    // Lab3: Your code here
    u >> reply.curr_term;
    u >> reply.is_succ;
    return u;
}