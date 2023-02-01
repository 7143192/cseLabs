// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>
#include "extent_server.h"
#include "persister.h"

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here
  //txid = 0;
  // Your code here for Lab2A: recover data on startup
  /* 
  _persister->restore_checkpoint();
  std::vector<chfs_command> commands = _persister->getCheckpointList();
  _persister->restore_logdata();
  std::vector<chfs_command> cmds = _persister->getLogList();
  //_persister->clearLogFile();
  for(uint32_t i = 0; i < cmds.size(); ++i) {
      //printf("i = %d\n", i);
      printf("cmds[i]->type = %d\n", cmds[i].type);
      commands.push_back(cmds[i]);
  }
  if(commands.size() == 0) return ;
  txid = commands[commands.size() - 1].id;
  */
  _persister->restore_checkpoint(); 
  _persister->restore_logdata();
  std::vector<chfs_command> commands = _persister->getLogList();
  //_persister->clearLogFile();
  if(commands.size() == 0) return ;
  txid = commands[commands.size() - 1].id;
  
  //redo_restart(commands);
  //sort(commands.begin(), commands.end(), compare_txid);//sort the restored commands by txid.
  //txid = commands[commands.size() - 1].id + 1;
  /*for(size_t i = 0; i < commands.size(); ++i) {//redo-only for part1
      chfs_command cmd = commands[i];
      if(cmd.type == chfs_command::CMD_CREATE) create_restart(cmd.file_type, cmd.extent_id);
      else {
	  int param;//??
          if(cmd.type == chfs_command::CMD_PUT) put_restart(cmd.extent_id, cmd.buf, param);
	  else {
	      if(cmd.type == chfs_command::CMD_REMOVE) remove_restart(cmd.extent_id, param);
	      else {
	          if(cmd.type == chfs_command::CMD_GET) {
		      std::string buf;
		      get(cmd.extent_id, buf);
		  }
	      }
	  }
      }
  }*/
  uint32_t i = 0;
  printf("size = %d\n", commands.size());
  while(i != commands.size()) {
      if(commands[i].type == chfs_command::CMD_BEGIN) {
	  printf("i = %d\n", i);
          unsigned long long id = commands[i].id;
	  uint32_t j = 0;
	  bool commited = false;
	  for(j = i; commands[j].id == id; ++j) {
	      if(commands[j].type == chfs_command::CMD_COMMIT) {
	          commited = true;
		  break;
	      }
	  }
	  if(commands[j].id != id) j--;
	  printf("j = %d\n", j);
	  for(uint32_t k = i; k < j; ++k) {
	      chfs_command cmd = commands[k];
	      int param;
	      if(cmd.type == chfs_command::CMD_CREATE) {
	          if(commited) create_restart(cmd.file_type, cmd.extent_id);
		  else continue;//TODO:handle the undo of the create
	      }
	      if(cmd.type == chfs_command::CMD_PUT) {
	          if(commited) put_restart(cmd.extent_id, cmd.buf, param);
		  //else put_restart(cmd.extent_id, cmd.old_buf, param);
		  else continue;
	      }
	      if(cmd.type == chfs_command::CMD_REMOVE) {
	          if(commited) remove_restart(cmd.extent_id, param);
		  else continue;
	      }
	  }
          i = j + 1;
	  //if(i == j) break;
      }
      //else break;
  }
  _persister->clearLogEntry();
  /*
  _persister->restore_checkpoint();
  std::vector<chfs_command> cmds = _persister->getCheckpointList();
  for(uint32_t i = 0; i < commands.size(); ++i) {
      cmds.push_back(commands[i]);
  }
  if(cmds.size() == 0) return ;
  txid = cmds[cmds.size() - 1].id;
  redo_restart(cmds);
  */
}
  
void extent_server::redo_restart(std::vector<chfs_command> commands)
{
    uint32_t i = 0;
    printf("size = %d\n", commands.size());
    while(i != commands.size()) {
        if(commands[i].type == chfs_command::CMD_BEGIN) {
	    printf("i = %d\n", i);
            unsigned long long id = commands[i].id;
	    uint32_t j = 0;
	    bool commited = false;
	    for(j = i; commands[j].id == id; ++j) {
	        if(commands[j].type == chfs_command::CMD_COMMIT) {
	            commited = true;
		    break;
	        }
	    }
	    if(commands[j].id != id) j--;
	    printf("j = %d\n", j);
	    for(uint32_t k = i; k < j; ++k) {
	        chfs_command cmd = commands[k];
	        int param;
	        if(cmd.type == chfs_command::CMD_CREATE) {
	            if(commited) create(cmd.file_type, cmd.extent_id);
		    else continue;//TODO:handle the undo of the create
	        }
	        if(cmd.type == chfs_command::CMD_PUT) {
	            if(commited) put(cmd.extent_id, cmd.buf, param);
		    //else put_restart(cmd.extent_id, cmd.old_buf, param);
		    else continue;
	        }
	        if(cmd.type == chfs_command::CMD_REMOVE) {
	            if(commited) remove(cmd.extent_id, param);
		    else continue;
	        }
	    }
            i = j + 1;
	    //if(i == j) break;
        }
      //else break;
   }
}

/*static bool compare_txid(chfs_command cmd1, chfs_command cmd2) {
    return cmd1.id < cmd2.id;
}*/

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  chfs_command* cmd = new chfs_command(chfs_command::CMD_CREATE, txid, type, id);
  _persister->append_log(*cmd);
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);
  //chfs_command* cmd = new chfs_command(chfs_command::CMD_CREATE, txid, type, id);
  //_persister->append_log(*cmd);
  return extent_protocol::OK;
}

int extent_server::create_restart(uint32_t type, extent_protocol::extentid_t &id) {
    printf("extent_server: create inode when restart\n");
    id = im->alloc_inode(type);
    return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  uint32_t size = buf.size();
  im->write_file(id, cbuf, size);
  printf("the buf writen in the put function:%s\n", buf);
  //chfs_command* cmd = new chfs_command(chfs_command::CMD_PUT,txid, id, "", 0, buf, size);
  //_persister->append_log(*cmd);
  return extent_protocol::OK;
}

int extent_server::put_restart(extent_protocol::extentid_t id, std::string buf, int&) {
    id &= 0x7fffffff;
    const char* cbuf = buf.c_str();
    uint32_t size = buf.size();
    printf("put-buf when restart: %s\n", buf.data());
    printf("put-size when restart:%d\n", size);
    im->write_file(id, cbuf, size);
    return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }
  //chfs_command* cmd = new chfs_command(chfs_command::CMD_GET, txid, id);
  //_persister->append_log(*cmd);
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  chfs_command* cmd = new chfs_command(chfs_command::CMD_REMOVE, txid, id);
  _persister->append_log(*cmd);
  im->remove_file(id);
  //chfs_command* cmd = new chfs_command(chfs_command::CMD_REMOVE, txid, id);
  //_persister->append_log(*cmd);
  return extent_protocol::OK;
}

int extent_server::remove_restart(extent_protocol::extentid_t id, int&) {
    printf("extent_server: write %lld when restart\n", id);
    id &= 0x7fffffff;
    im->remove_file(id);
    return extent_protocol::OK;
}

void extent_server::addBasicLog(chfs_command* cmd) {
    //txid = cmd->id;
    if(cmd->type == chfs_command::CMD_BEGIN) txid++;
    cmd->id = txid;
    _persister->append_log(*cmd);
}

void extent_server::addPutLog(chfs_command* cmd) {
    //txid = cmd->id;
    cmd->id = txid;
    _persister->append_log(*cmd);
}
