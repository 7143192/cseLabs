// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  /*the next 6 lines added in the lab2B*/
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }

  //es = new extent_server();
  //txid = 0;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create, type, id);
  //es->txid = txid;
  //ret = es->create(type, id);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::get, eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here

  int r;
  //es->txid = txid;
  //ret = es->put(eid, buf, r);
  ret = cl->call(extent_protocol::put, eid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here

  int r;
  //es->txid = txid;
  //ret = es->remove(eid, r);
  ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
}

//log APIs here
int
extent_client::addBasicLog(int type, unsigned long long txid) {
    //chfs_command* cmd = new chfs_command(type, txid);
    //es->addBasicLog(cmd);
    extent_protocol::status ret = extent_protocol::OK;
    int r;
    ret = cl->call(extent_protocol::addBasicLog, txid, type, r);
    return ret;
}

void
extent_client::addPutLog(unsigned long long txid, unsigned long long extent_id,
		std::string old_buf, uint32_t old_size, std::string buf, uint32_t size) 
{
    chfs_command* cmd = new chfs_command(chfs_command::CMD_PUT, txid, extent_id, old_buf, old_size, buf, size);
    //es->addPutLog(cmd);
}


