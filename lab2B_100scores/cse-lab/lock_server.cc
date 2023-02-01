// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
    mutex = PTHREAD_MUTEX_INITIALIZER;
    //pthread_mutex_init(&mutex, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mutex);
  if(lock_state.find(lid) != lock_state.end()) {
      if(lock_state[lid] == 0) lock_state[lid] = 1;//the lock is free, acquire it directly
      else {
          while(lock_state[lid] == 1) {
	      pthread_cond_wait(&lock_cond[lid], &mutex);
	  }
	  lock_state[lid] = 1;//acquire the newly-released lock
      } 
  }
  else {//the lock is new for the map
      lock_state[lid] = 1;//acquire the newly-created lock
      pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
      //pthread_cond_init(&cond, NULL);
      lock_cond[lid] = cond;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mutex);
  if(lock_state.find(lid) == lock_state.end()) {
      pthread_mutex_unlock(&mutex);
      return ret;
  }//the lock declared by the lid doesn't exist in the lock-list
  lock_state[lid] = 0;//free the lock declared by lid
  pthread_cond_signal(&lock_cond[lid]);//signal the corresponding cv 
  pthread_mutex_unlock(&mutex);
  return ret;
}
