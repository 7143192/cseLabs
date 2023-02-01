#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <string>
#include <fstream>
#include <iostream>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    void PersistMetaData(int term, int voteFor);//used to write the voting term and the vote target into the node's metadata.bin file.
    void PersistLog(std::vector<log_entry<command>> logs);//used to persist the log into this node's log.bin file.
    int Restore(int &term, int &voteFor, std::vector<log_entry<command>> &log, std::vector<char>& snapshot_data);//restore the data from the persisted files.
    //void AppendLog(log_entry<command> log);//append a new log to this node's log.bin file.
    void PersistSnapShot(std::vector<char>& data);//persist the snapshot data into the file snapshot.bin
private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string metadata;//metadata file name
    std::string log_name;//log file name
    std::string snapshot_name;
    int buf_s;//buf-size
    char* buf;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    metadata = dir + "/metadata.bin";
    log_name = dir + "/log.bin";
    snapshot_name = dir + "/snapshot.bin";
    buf_s = 4;//according the serialize function provided by the raft_test_utils.h file.
    buf = new char[buf_s];
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
    delete []buf;
    buf_s = 0;
}

template <typename command>
void raft_storage<command>::PersistMetaData(int term, int voteFor) {
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    std::ofstream file;
    file.open(metadata, std::ios::out | std::ios::trunc | std::ios::binary);//clear the old files.
    file.write((char*)(&term), sizeof(int));
    file.write((char*)(&voteFor), sizeof(int));
    file.close();
    lock.unlock();
    return ;
}

template <typename command>
void raft_storage<command>::PersistLog(std::vector<log_entry<command>> logs) {
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    if(logs.size() == 0) {
	lock.unlock();
	return ;
    }
    std::ofstream file;
    file.open(log_name, std::ios::out | std::ios::trunc | std::ios::binary);//clear the old files.
    int size = logs.size();
    file.write((char*)(&size), sizeof(int));//write the log-entry-num into the file first.
    //std::vector<log_entry<command>>::iterator it = logs.begin();
    int i = 0;
    while(i != logs.size()) {
        log_entry<command> entry = logs[i];
	file.write((char*)(&(entry.term)), sizeof(int));
	file.write((char*)(&(entry.index)), sizeof(int));
	int s = entry.cmd.size();
	char* buf1 = new char[s];
	file.write((char*)(&s), sizeof(int));//write the size of the serialize result into the file first.
	entry.cmd.serialize(buf1, s);
	file.write(buf1, s);//write a serialized log-command into the file
	//delete []buf1;
	i++;
    }
    file.close();
    lock.unlock();
    return ;
}

template <typename command>
int raft_storage<command>::Restore(int &term, int &voteFor, std::vector<log_entry<command>> &log, std::vector<char>& snapshot_data) {
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    //read meatadata here.
    std::ifstream file1(metadata, std::ios::in | std::ios::binary);
    if(!file1.is_open()) {
	lock.unlock();
	return -1;//open file failed, not existed
    }
    file1.peek();
    if(file1.eof()) {
        file1.close();
	lock.unlock();
	return -1;//there is no meatdata.
    }
    file1.seekg(0, std::ios::beg);
    file1.read((char*)(&term), sizeof(int));
    file1.read((char*)(&voteFor), sizeof(int));
    file1.close();
    //restore log-entries info here.
    std::ifstream file2(log_name, std::ios::in | std::ios::binary);
    if(!file2.is_open()) {
	lock.unlock();
	return -1;
    }
    file2.peek();
    if(file2.eof()) {
        file2.close();
	lock.unlock();
	return -1;//log file is empty
    }
    file2.seekg(0, std::ios::beg);
    int size;
    file2.read((char*)(&size), sizeof(int));//get the log-entries-num first.
    log.clear();
    for(int i = 0; i < size; ++i) {
        int term;
	int index;
	file2.read((char*)(&term), sizeof(int));
	file2.read((char*)(&index), sizeof(int));
	log_entry<command> entry = log_entry<command>();
	entry.term = term;
	entry.index = index;
	command cmd;
	int cmd_size;
	file2.read((char*)(&cmd_size), sizeof(int));
	char* buf1 = new char[cmd_size];
	file2.read(buf1, cmd_size);//read the serialized entry data from the file.
	cmd.deserialize(buf1, cmd_size);
	entry.cmd = cmd;
	log.push_back(entry);
    }
    file2.close();
    //resotre the snapshot data
    std::ifstream file3(snapshot_name, std::ios::in | std::ios::binary);
    if(!file3.is_open()) {
	lock.unlock();
	return -1;
    }
    file3.peek();
    if(file3.eof()) {
        file3.close();
	lock.unlock();
	return -1;
    }
    file3.seekg(0, std::ios::beg);
    int snapshot_size;
    file3.read((char*)(&snapshot_size), sizeof(int));//get the snapshot data length first.
    char* s_buf = new char[snapshot_size];
    file3.read(s_buf, snapshot_size);
    for(int i = 0; i < snapshot_size; ++i) snapshot_data.push_back(s_buf[i]);
    file3.close();
    lock.unlock();
}

template <typename command>
void raft_storage<command>::PersistSnapShot(std::vector<char>& data) {
    //beacause we need to discard the old entries, so we just rewrite the snapshot.bin every time we update a snapshot.
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    std::ofstream file;
    file.open(snapshot_name, std::ios::out | std::ios::trunc | std::ios::binary);
    int size = data.size();
    file.write((char*)(&size), sizeof(int));
    std::string s(size, '0');
    for(int i = 0; i < size; ++i) s[i] = data[i];
    file.write(s.c_str(), size);
    file.close();
    lock.unlock();
}
#endif // raft_storage_h
