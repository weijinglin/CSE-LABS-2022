#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>

#include <fstream>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here

    void save(int term, int voteFor);
    void append_logs(std::vector<log_entry<command>> &entries);
    void do_snapshot(std::vector<char> snap,int snap_idx,int term);
    void load_snap(int &term, std::vector<log_entry<command>> &entries, std::vector<char> &char_vec);
    void restore(int &term, int &voteFor, std::vector<log_entry<command>> &entries);

    void clear();
private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string dir_path;

    std::string _vote;
    std::string _logs;
    std::string _snap;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) :dir_path(dir) {
    // Lab3: Your code here
    _vote = "vote_data";
    _logs = "logs_data";
    _snap = "snap_data";
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

template <typename command>
void raft_storage<command>::save(int term, int voteFor){
    std::unique_lock<std::mutex> lock(mtx);
    std::string file_path = this->dir_path + "/" + this->_vote;
    std::ofstream outFile(file_path, std::ios::out | std::ios::binary | std::ios::trunc);

    outFile.write((char*)&term, sizeof(term));
    outFile.write((char*)&voteFor, sizeof(voteFor));

    return;
}

template <typename command>
void raft_storage<command>::append_logs(std::vector<log_entry<command>> &entries){
    std::unique_lock<std::mutex> lock(mtx);
    std::string file_path = this->dir_path + "/" + this->_logs;
    std::ofstream outFile(file_path, std::ios::out | std::ios::binary | std::ios::trunc);

    // persistent
    for(auto entry : entries){
        int length = entry.cmd.size() + 4 + 4 + 4;// 1st for term , 2nd for snap_idx, 3rd for idx
        outFile.write((char*)&length, sizeof(length));
        outFile.write((char*)&(entry.term), sizeof(entry.term));
        outFile.write((char*)&(entry.snap_idx), sizeof(entry.snap_idx));
        outFile.write((char*)&(entry.idx), sizeof(entry.idx));


        char* buf = new char[length - 12];
        entry.cmd.serialize(buf,length - 12);

        outFile.write(buf,length - 12);
        delete [] buf;
    }
    
    return;
}

template <typename command>
void raft_storage<command>::restore(int &term, int &voteFor, std::vector<log_entry<command>> &entries){
    std::unique_lock<std::mutex> lock(mtx);
    // load for vote
    printf("go into restore\n");
    std::string vote_path = this->dir_path + "/" + this->_vote;
    std::string log_path = this->dir_path + "/" + this->_logs;
    std::ifstream inFile(vote_path,std::ios::in|std::ios::binary);

    inFile.read((char *)&term, sizeof(term));
    inFile.read((char *)&voteFor, sizeof(voteFor));
    inFile.close();

    std::ifstream logFile(log_path,std::ios::in|std::ios::binary);

    if(logFile.is_open() == false){
        printf("open error\n");
        return;
    }
    int len;
    while(logFile.read((char*)&len,4)){
        printf("hit once\n");
        int _term,_snap,_idx;
        logFile.read((char*)&_term,4);
        logFile.read((char*)&_snap,4);
        logFile.read((char*)&_idx,4);
        char *buf = new char[len - 12];
        logFile.read(buf,len - 12);
        command cmd;
        cmd.deserialize(buf,len - 12);
        // entries.push_back(std::pair<command,int>{cmd,_term});
        log_entry<command> _in(term,cmd,_snap,_idx);
        entries.push_back(_in);
        delete [] buf;
    }
}

template <typename command>
void raft_storage<command>::do_snapshot(std::vector<char> snap,int snap_idx,int term){
    std::unique_lock<std::mutex> lock(mtx);
    std::string file_path = this->dir_path + "/" + this->_snap;
    std::ofstream outFile(file_path, std::ios::out | std::ios::binary | std::ios::trunc);

    int size = snap.size();
    char* buf = new char[snap.size()];
    for(int i = 0;i < size;++i){
        buf[i] = snap.at(i);
    }
    
    outFile.write((char*)&size,4);
    outFile.write(buf,snap.size());
    outFile.write((char*)&snap_idx,sizeof(snap_idx));
    outFile.write((char*)&term,sizeof(term));

    delete [] buf;
    return;
}

template <typename command>
void raft_storage<command>::load_snap(int &term, std::vector<log_entry<command>> &entries, std::vector<char> &char_vec){
    std::unique_lock<std::mutex> lock(mtx);
    std::string snap_path = this->dir_path + "/" + this->_snap;
    std::ifstream inFile(snap_path,std::ios::in|std::ios::binary);

    int size;
    if(!inFile){
        return;
    }
    if(!(inFile.read((char*)&size,4))){
        return;
    }
    char *buf = new char[size];
    inFile.read(buf,size);
    log_entry<command> a_log;
    inFile.read((char*)&(a_log.snap_idx),4);
    inFile.read((char*)&(a_log.term),4);
    entries.push_back(a_log);
    term = a_log.term;
    for(int i = 0;i < size;++i){
        char_vec.push_back(buf[i]);
    }
}

template <typename command>
void raft_storage<command>::clear(){
    std::unique_lock<std::mutex> lock(mtx);
    std::string file_path = this->dir_path + "/" + this->_logs;
    std::ofstream outFile(file_path, std::ios::out | std::ios::binary | std::ios::trunc);
}

#endif // raft_storage_h