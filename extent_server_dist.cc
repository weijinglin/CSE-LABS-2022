#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const {
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0) {
        return this->raft_group->nodes[0];
    } else {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id) {
    // Lab3: your code here
    printf("try to create\n");
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_CRT;
    cmd.type = type;
    // TODO(wjl) : there don't do initial work to teh variable
    cmd.res = std::make_shared<chfs_command_raft::result>();
    std::unique_lock<std::mutex> lock(cmd.res->mtx);

    auto now = std::chrono::system_clock::now();
    chfs_raft* raf = this->leader();
    int term,index;
    raf->new_command(cmd,term,index);
    if (!cmd.res->done) {
        ASSERT(
            cmd.res->cv.wait_until(lock, now + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
    }
    id = cmd.res->id;
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &) {
    // Lab3: your code here
    printf("try to put and id is %d and buf is %s\n",id,buf.c_str());
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_PUT;
    cmd.id = id;
    cmd.buf = "";
    for(int i = 0;i < buf.length();++i){
        cmd.buf.append(1,buf.at(i));
    }

    // TODO(wjl) : there don't do initial work to teh variable
    cmd.res = std::make_shared<chfs_command_raft::result>();
    // printf("test for res is %d\n",cmd.res->id);
    // printf("test for res is %d\n",cmd.res->done);

    std::unique_lock<std::mutex> lock(cmd.res->mtx);

    auto now = std::chrono::system_clock::now();
    chfs_raft* raf = this->leader();
    int term,index;
    raf->new_command(cmd,term,index);
    if (!cmd.res->done) {
        ASSERT(
            cmd.res->cv.wait_until(lock, now + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
    }
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf) {
    // Lab3: your code here
    printf("try to get and id is %d\n",id);
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_GET;
    cmd.id = id;
    // TODO(wjl) : there don't do initial work to teh variable
    cmd.res = std::make_shared<chfs_command_raft::result>();
    std::unique_lock<std::mutex> lock(cmd.res->mtx);

    auto now = std::chrono::system_clock::now();
    chfs_raft* raf = this->leader();
    int term,index;
    raf->new_command(cmd,term,index);
    if (!cmd.res->done) {
        ASSERT(
            cmd.res->cv.wait_until(lock, now + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
    }

    buf = cmd.res->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    // Lab3: your code here
    printf("try to getattr and id is %d\n",id);
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_GETA;
    cmd.id = id;
    // TODO(wjl) : there don't do initial work to the variable
    cmd.res = std::make_shared<chfs_command_raft::result>();
    std::unique_lock<std::mutex> lock(cmd.res->mtx);

    auto now = std::chrono::system_clock::now();
    chfs_raft* raf = this->leader();
    int term,index;
    raf->new_command(cmd,term,index);
    if (!cmd.res->done) {
        ASSERT(
            cmd.res->cv.wait_until(lock, now + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
    }

    a = cmd.res->attr;
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &) {
    // Lab3: your code here
    printf("try to remove and id is %d\n",id);
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_RMV;
    cmd.id = id;
    // TODO(wjl) : there don't do initial work to teh variable
    cmd.res = std::make_shared<chfs_command_raft::result>();
    std::unique_lock<std::mutex> lock(cmd.res->mtx);

    auto now = std::chrono::system_clock::now();
    chfs_raft* raf = this->leader();
    int term,index;
    raf->new_command(cmd,term,index);
    if (!cmd.res->done) {
        ASSERT(
            cmd.res->cv.wait_until(lock, now + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
    }
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist() {
    delete this->raft_group;
}