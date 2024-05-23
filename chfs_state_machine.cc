#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    // TODO(wjl) : it's the size don't contain res(because it doesn't need to marhsall and unmarshall)
    // the last 4 is represent for the size of the command used in serial and deserial
    return sizeof(this->cmd_tp) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + buf.length() + 4;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    if(size != this->size()){
        return;
    }
    // TODO(wjl) : here assume the size of a enum variable is 4 bytes
    int enum_num = cmd_tp;
    buf_out[0] = (enum_num >> 24) & 0xff;
    buf_out[1] = (enum_num >> 16) & 0xff;
    buf_out[2] = (enum_num >> 8) & 0xff;
    buf_out[3] = enum_num & 0xff;

    buf_out[4] = (type >> 24) & 0xff;
    buf_out[5] = (type >> 16) & 0xff;
    buf_out[6] = (type >> 8) & 0xff;
    buf_out[7] = type & 0xff;

    buf_out[8] = (id >> 56) & 0xff;
    buf_out[9] = (id >> 48) & 0xff;
    buf_out[10] = (id >> 40) & 0xff;
    buf_out[11] = (id >> 32) & 0xff;
    buf_out[12] = (id >> 24) & 0xff;
    buf_out[13] = (id >> 16) & 0xff;
    buf_out[14] = (id >> 8) & 0xff;
    buf_out[15] = id & 0xff;

    buf_out[16] = (size >> 24) & 0xff;
    buf_out[17] = (size >> 16) & 0xff;
    buf_out[18] = (size >> 8) & 0xff;
    buf_out[19] = size & 0xff;


    for(int i = 0;i < buf.length();++i){
        buf_out[20 + i] = buf.at(i);
    }
    // TODO(wjl) ; no serial res(reference for document)
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    int enum_num = 0;
    enum_num = (buf_in[0] & 0xff) << 24;
    enum_num |= (buf_in[1] & 0xff) << 16;
    enum_num |= (buf_in[2] & 0xff) << 8;
    enum_num |= buf_in[3] & 0xff;
    switch (enum_num)
    {
    case 0:
    {
        cmd_tp = CMD_NONE;
        break;
    }
    case 1:
    {
        cmd_tp = CMD_CRT;
        break;
    }
    case 2:
    {
        cmd_tp = CMD_PUT;
        break;
    }
    case 3:
    {
        cmd_tp = CMD_GET;
        break;
    }
    case 4:
    {
        cmd_tp = CMD_GETA;
        break;
    }
    case 5:
    {
        cmd_tp = CMD_RMV;
        break;
    }
    
    default:
        break;
    }

    type = (buf_in[4] & 0xff) << 24;
    type |= (buf_in[5] & 0xff) << 16;
    type |= (buf_in[6] & 0xff) << 8;
    type |= buf_in[7] & 0xff;

    id = (buf_in[8] & 0xff) << 56;
    id |= (buf_in[9] & 0xff) << 48;
    id |= (buf_in[10] & 0xff) << 40;
    id |= (buf_in[11] & 0xff) << 32;
    id = (buf_in[12] & 0xff) << 24;
    id |= (buf_in[13] & 0xff) << 16;
    id |= (buf_in[14] & 0xff) << 8;
    id |= buf_in[15] & 0xff;

    int len;
    len = (buf_in[16] & 0xff) << 24;
    len |= (buf_in[17] & 0xff) << 16;
    len |= (buf_in[18] & 0xff) << 8;
    len |= buf_in[19] & 0xff;

    // TODO(wjl) : assume at this point , buf is ""
    for(int i = 0;i < size - 20;++i){
        buf.append(1,buf_in[20 + i]);
    }
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    // printf("<< beg\n");

    int size = cmd.size();
    char* buf_out = new char[size];
    cmd.serialize(buf_out,size);
    for(int i = 0;i < size;++i){
        m << buf_out[i];
    }
    delete [] buf_out;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    // printf(">> beg\n");
    char* buf_in_buf = new char[20];
    for(int i = 0;i < 20;++i){
        u >> buf_in_buf[i];
    }

    // parse the size
    int len;
    len = (buf_in_buf[16] & 0xff) << 24;
    len |= (buf_in_buf[17] & 0xff) << 16;
    len |= (buf_in_buf[18] & 0xff) << 8;
    len |= buf_in_buf[19] & 0xff;

    char* buf_in = new char[len];
    for(int i = 0;i < 20;++i){
        buf_in[i] = buf_in_buf[i];
    }

    for(int i = 20;i < len;++i){
        u >> buf_in[i];
    }

    cmd.deserialize(buf_in,len);
    cmd.res = std::make_shared<chfs_command_raft::result>();
    delete [] buf_in;

    if(cmd.res.get() == nullptr){
        printf("hit\n");
    }

    delete [] buf_in_buf;
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    std::unique_lock<std::mutex> lock(mtx);
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    if(chfs_cmd.res.get() == nullptr){
        chfs_cmd.res = std::make_shared<chfs_command_raft::result>();
    }
    printf("apply log\n");
    // Lab3: Your code here
    switch (chfs_cmd.cmd_tp)
    {
    case chfs_command_raft::CMD_CRT:
    {
        /* code */
        es.create(chfs_cmd.type,chfs_cmd.res->id);
        chfs_cmd.res->done = true;
        break;
    }
    case chfs_command_raft::CMD_GET:
    {
        es.get(chfs_cmd.id,chfs_cmd.res->buf);
        chfs_cmd.res->done = true;
        break;
    }
    case chfs_command_raft::CMD_GETA:
    {
        es.getattr(chfs_cmd.id,chfs_cmd.res->attr);
        chfs_cmd.res->done = true;
        break;
    }
    case chfs_command_raft::CMD_PUT:
    {
        int temp;
        es.put(chfs_cmd.id,chfs_cmd.buf,temp);
        chfs_cmd.res->done = true;
        break;
    }
    case chfs_command_raft::CMD_RMV:
    {
        int temp;
        es.remove(chfs_cmd.id,temp);
        chfs_cmd.res->done = true;
        break;
    }
    
    
    default:
        break;
    }

    chfs_cmd.res->cv.notify_all();
    return;
}


