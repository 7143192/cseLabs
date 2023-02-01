// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;

 public:
  unsigned long long txid = 0;
  extent_server();
  
  static bool compare_txid(chfs_command& cmd1, chfs_command& cmd2) {
      return cmd1.id < cmd2.id;
  }

  int create(uint32_t type, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
  //functions that are used when the system restarts.
  int create_restart(uint32_t type, extent_protocol::extentid_t& id);
  int put_restart(extent_protocol::extentid_t id, std::string, int &);
  int remove_restart(extent_protocol::extentid_t id, int &);
  // Your code here for lab2A: add logging APIs
  void addBasicLog(chfs_command* cmd);//the func used to add the BEGIN and COMMIT log in the logfile.
  void addPutLog(chfs_command* cmd);//add put redo-undo log
  void redo_restart(std::vector<chfs_command> cmds);
};

#endif 







