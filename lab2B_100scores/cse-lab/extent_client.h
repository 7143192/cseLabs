// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;

 public:
  extent_client(std::string dst);//added in the Lab2B
  unsigned long long txid;
  extent_client();

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  //Log APIS here
  int addBasicLog(int type, unsigned long long txid);
  void addPutLog(unsigned long long txid, unsigned long long extent_id, std::string old_buf, 
		  uint32_t old_size, std::string buf, uint32_t size);
};

#endif 

