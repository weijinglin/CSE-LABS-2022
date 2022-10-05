// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

// function : delete a directory
int 
chfs_client::rmdir(inum parent,const char* name)
{
    int r = OK;
    std::list<dirent> list;
    inum c_ino;
    bool is_exist;
    lookup(parent,name,is_exist,c_ino);
    if(!is_exist){
        return r;
    }else{
        extent_protocol::attr a;
        ec->getattr(c_ino,a);
        if(a.type != extent_protocol::T_DIR){
            return NOENT;
        }else{
            readdir(c_ino,list);
            for(auto item : list){
                extent_protocol::attr st;
                ec->getattr(item.inum,st);
                if(st.type == extent_protocol::T_DIR){
                    rmdir(c_ino,item.name.c_str());
                }else{
                    ec->remove(item.inum);
                }
            }
            return OK;
        }
    }
}

// function : read a symbol link
int
chfs_client::read_link(inum ino, std::string &buf)
{
    int r = OK;

    if(ec->get(ino,buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }

    printf("readlink okk\n");
    return r;
}

// function: create a symbol link
int 
chfs_client::create_sym(inum parent, const char *name, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool is_exist;
    lookup(parent,name,is_exist,ino_out);
    if(is_exist){
        return chfs_client::EXIST;
    }else{
        // try to create new file
        // eid here is same as inum
        ec->create(extent_protocol::T_SYM, ino_out);

        std::string inum = filename(ino_out);

        std::string co_name = name;

        std::string atomic = "/";

        std::string input_buf = name + atomic + inum + atomic;

        // write dir
        // read previous file content
        std::string buf;
        ec->get(parent,buf);

        input_buf = buf + input_buf;

        // write back
        ec->put(parent,input_buf);
        
        printf("create symbol success\n");
    }

    return r;
}

bool chfs_client::is_sym(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYM) {
        printf("isfile: %lld is a sym\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a sym\n", inum);
    return false;
}

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // return ! isfile(inum);
    if(isfile(inum)){
        return false;
    }else if(is_sym(inum)){
        return false;
    }else{
        return true;
    }
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int chfs_client::get_sym(inum inum,fileinfo &fin)
{
    int r = OK;

    printf("getsym %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getsym %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    // get the basic attribute of inode
    extent_protocol::attr attr;
    ec->getattr(ino,attr);

    std::string buf;
    ec->get(ino,buf);

    if(size == attr.size){
        return r;
    }else if(size < attr.size){
        std::string input_buf = buf.substr(0,size);
        ec->put(ino,input_buf);
        return r;
    }else{
        std::string input_buf = buf;
        input_buf.append(size - attr.size,0);
        ec->put(ino,input_buf);
        return r;
    }

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool is_exist;
    lookup(parent,name,is_exist,ino_out);
    if(is_exist){
        return chfs_client::EXIST;
    }else{
        // try to create new file
        // eid here is same as inum
        ec->create(extent_protocol::T_FILE, ino_out);

        std::string inum = filename(ino_out);

        std::string co_name = name;

        std::string atomic = "/";

        std::string input_buf = name + atomic + inum + atomic;

        // write dir
        // read previous file content
        std::string buf;
        ec->get(parent,buf);

        input_buf = buf + input_buf;

        // write back
        ec->put(parent,input_buf);
        
        printf("create simple file success\n");
    }

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool is_exist;
    lookup(parent,name,is_exist,ino_out);

    if(is_exist){
        return chfs_client::EXIST;
    }else{
        // try to create new file
        // eid here is same as inum
        ec->create(extent_protocol::T_DIR, ino_out);

        std::string inum = filename(ino_out);

        std::string co_name = name;

        std::string atomic = "/";

        std::string input_buf = name + atomic + inum + atomic;

        // write dir
        // read previous file content
        std::string buf;
        ec->get(parent,buf);

        input_buf = buf + input_buf;

        // write back
        ec->put(parent,input_buf);

        printf("create directory file success\n");
    }

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    std::list<dirent> read_list;
    readdir(parent,read_list);

    for(auto item : read_list){
        const char* buf = item.name.c_str();
        if(strcmp(buf,name) == 0){
            found = true;
            ino_out = item.inum;
            return r;
        }
    }

    found = false;

    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    // design:
    // my defined format : use '/' to divide name and node
    // and use '/' to divide between entry

    // first , read from the dir
    std::string buf;
    ec->get(dir,buf);

    int fir_pos = -1;
    int sec_pos = -1;
    std::string name;
    std::string inum;
    // parse and fill the list
    while(true){
        // there can plus one base on the file name can't be null 
        fir_pos = buf.find('/',sec_pos + 1);
        // check if it is ending
        if(fir_pos == buf.npos){
            break;
        }

        name = buf.substr(sec_pos + 1,fir_pos - sec_pos - 1);

        // find next '/'
        sec_pos = buf.find('/',fir_pos + 1);

        if(sec_pos == buf.npos){
            printf("wrong in readdir\n");
        }

        inum = buf.substr(fir_pos + 1,sec_pos - fir_pos - 1);

        chfs_client::inum pa_inum = n2i(inum);
        
        // add to list
        list.push_back({name,pa_inum});

        // the end condition
        if(sec_pos > buf.length()){
            break;
        }

    }

    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    // first , get the core buffer
    std::string buf;
    ec->get(ino,buf);

    // try to trunc the core data
    if(off + size <= buf.length()){
        data = buf.substr(off,size);
    }else if(off < buf.length()){
        data = buf.substr(off,size - off);
    }else{
        data = "";
    }


    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    // first , construct the core buffer to write in
    std::string read_buf;
    ec->get(ino,read_buf);
    int tot_length = read_buf.length();

    if(size + off <= read_buf.length()){
        // modify directly
        printf("first situlation\n");
        for(int i = 0;i < size;++i){
            read_buf[i + off] = data[i];
        }
        bytes_written = size;
    }else if(off <= read_buf.length()){
        printf("second situlation\n");
        for(int i = 0;i < tot_length - off;++i){
            read_buf[i + off] = data[i];
        }

        for(int i = 0;i < off + size - tot_length;++i){
            read_buf.append(1,data[i + tot_length - off]);
        }
        bytes_written = size;
    }else{
        // off > tot_length
        printf("third situlation\n");
        read_buf.append(off-tot_length,0);

        for(int i = 0;i < size;++i){
            read_buf.append(1,data[i]);
        }
        // bytes_written = size + off - tot_length;
        bytes_written = size;
    }

    // second , write to disk
    ec->put(ino,read_buf);

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    // first , get the inum
    std::list<dirent> list;
    readdir(parent,list);
    
    inum child_node;
    bool is_find = false;

    for(auto item : list){
        if(strcmp(item.name.c_str(),name) == 0){
            // find the file
            child_node = item.inum;
            is_find = true;
        }
    }

    if(!is_find){
        r = chfs_client::NOENT;
        return r;
    }

    // second , remove the file
    ec->remove(child_node);

    // third , update the parent node;
    std::string in_buf;
    std::string atomic = "/";
    for(auto item : list){
        if(strcmp(item.name.c_str(),name) != 0){
            in_buf = in_buf + item.name + atomic + filename(item.inum) + atomic;
        }
    }

    // write back to disk
    ec->put(parent,in_buf);

    return r;
}

