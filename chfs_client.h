#ifndef chfs_client_h
#define chfs_client_h

#include <string>
//#include "chfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include<chrono>

// a timer used to trace the performance
class file_timer
{
private:
    std::chrono::steady_clock::time_point _begin;
    std::chrono::steady_clock::time_point _end;
public:
    file_timer()
	{
		_begin = std::chrono::steady_clock::time_point();
		_end = std::chrono::steady_clock::time_point();
	}
    
	virtual ~file_timer(){};  
    
    // update the begin timestamp
    void UpDate()
    {
        _begin = std::chrono::steady_clock::now();
    }

	// get the time period from begin
    double GetSecond()
    {
        _end = std::chrono::steady_clock::now();
        std::chrono::duration<double> temp = std::chrono::duration_cast<std::chrono::duration<double>>(_end - _begin);
       	return temp.count();
    }
};

class chfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    chfs_client::inum inum;
  };
  struct syminfo {
    std::string slink;
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

 public:
  chfs_client(std::string);

  bool isfile(inum);
  bool isdir(inum);
  bool is_sym(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  // the pos fixed in merge
  int get_sym(inum, fileinfo&);

  // the code merged
  int getsymlink(inum, syminfo&);


  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  int create_sym(inum parent, const char *name, inum &ino_out);
  int read_link(inum ino, std::string &buf);
  int rmdir(inum parent,const char* name);
  
  /** you may need to add symbolic link related methods here.*/

};

#endif 
