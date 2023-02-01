// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client()
{
  es = new extent_server();
  txid = 0;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  //es->txid = txid;
  ret = es->create(type, id);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = es->get(eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = es->getattr(eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  //es->txid = txid;
  ret = es->put(eid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  //es->txid = txid;
  ret = es->remove(eid, r);
  return ret;
}

//log APIs here
void 
extent_client::addBasicLog(chfs_command::cmd_type type, unsigned long long txid) {
    chfs_command* cmd = new chfs_command(type, txid);
    es->addBasicLog(cmd);
}

void
extent_client::addPutLog(unsigned long long txid, unsigned long long extent_id,
		std::string old_buf, uint32_t old_size, std::string buf, uint32_t size) 
{
    chfs_command* cmd = new chfs_command(chfs_command::CMD_PUT, txid, extent_id, old_buf, old_size, buf, size);
    es->addPutLog(cmd);
}


