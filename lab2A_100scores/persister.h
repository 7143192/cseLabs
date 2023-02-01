#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 131072

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
	CMD_PUT,
	CMD_REMOVE,
	CMD_GET
	//CMD_PUT_UNDO//used to notify a put_undo operation
    };//add some new cmd type here(just the write cmd for now).

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    uint32_t file_type;//record the file type of CREATE
    unsigned long long extent_id;//record the param extent id
    std::string buf;//record the content of the PUT
    uint32_t buf_size;//record the buf size
    std::string old_buf;//record the old value before a put operation, which is used to do the UNDO op.
    uint32_t old_size;

    // constructor
    chfs_command() {}
    chfs_command(cmd_type Type, txid_t Id) {
        this->type = Type;
	this->id = Id;
    }//constructor for the CMD_BEGIN and CMD_COMMIT
    chfs_command(cmd_type Type, txid_t Id, uint32_t fileType, unsigned long long extentId) {
        this->type = Type;
	this->id = Id;
	this->file_type = fileType;
	this->extent_id = extentId;
    }//constructor for the CMD_CREATE
    chfs_command(cmd_type Type, txid_t Id, unsigned long long extentId, std::string old, uint32_t old_s, std::string str, uint32_t got_size) {
        this->type = Type;
	this->id = Id;
	this->extent_id = extentId;
	this->old_buf = old;
	this->old_size = old_s;
	this->buf = str;
	this->buf_size = got_size;
    }//constructor for the CMD_PUT
    chfs_command(cmd_type Type, txid_t Id, unsigned long long extentId) {
        this->type = Type;
	this->id = Id;
	this->extent_id = extentId;
    }//constructor for the CMD_REMOVE and CMD_GET

    uint64_t size() const {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t);
        return s;
    }

    std::string CmdToStr() {
        return "";
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const command& log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();
    std::vector<command> getLogList();//get the private vetor in the persister object.
    std::vector<command> getCheckpointList();//get the private restored checkpoint entries of the object.
    uint32_t getFileSize(std::string dir);//the function used to get the size of the file @dir.
    void getData(std::ifstream& file, std::vector<command>& vec, uint32_t size);
    void clearLogFile();
    void clearLogEntry();
private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;
    
    // restored log data
    std::vector<command> log_entries;
    // restored checkpoint data
    std::vector<command> checkpoint_entries;
};

template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A
    log_entries.clear();//claear the restored data
    checkpoint_entries.clear();
    //std::ofstream file(file_path_checkpoint, std::ios::binary | std::ios::trunc);
    //file.close();
    //std::ofstream file(this->file_path_logfile, std::ios::binary | std::ios::trunc);
    //if(!file.is_open()) return ;
    //file.close();
}

template<typename command>
void persister<command>::clearLogEntry()
{
    log_entries.clear();
}

template<typename command>
void persister<command>::clearLogFile()
{
    std::ifstream file0(this->file_path_logfile, std::ios::binary | std::ios::in);
    if(!file0.is_open()) return ;
    file0.close();
    std::ofstream file(this->file_path_logfile, std::ios::out | std::ios::trunc);
    file.close();
}

template<typename command>
uint32_t persister<command>::getFileSize(std::string dir)
{
    std::ifstream file(dir, std::ios::binary);
    if(!file.is_open()) return 0;//file doesn't exist
    file.seekg(0, std::ios::beg);
    uint32_t size0 = file.tellg();
    file.seekg(0, std::ios::end);
    uint32_t ans = file.tellg() - size0;
    file.close();
    return ans;
}

template<typename command>
void persister<command>::append_log(const command& log) {
    // Your code here for lab2A
     
    uint32_t prev_size = getFileSize(this->file_path_logfile);
    if(prev_size >= MAX_LOG_SZ) checkpoint();//if the current(before the new insert) size exceeds, checkpoint it.
    
    std::ofstream file;
    file.open(this->file_path_logfile, std::ios::binary | std::ios::app);//open the logfile int the binary and appending mode.
    //file.seekp(0, std::ios::end);
    //std::streampos fsize = 0;
    chfs_command* cmd = (chfs_command*)&log;
    printf("cmd->id = %d\n", cmd->id);
    file.write((char*)(&(cmd->type)),sizeof(cmd->type));//write the cmd type to the disk
    file.write((char*)(&(cmd->id)), sizeof(cmd->id));//write the cmd txid to the disk.
    if(cmd->type == chfs_command::CMD_CREATE) {
        file.write((char*)(&(cmd->file_type)), sizeof(uint32_t));
	file.write((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
    }
    else {
        if(cmd->type == chfs_command::CMD_PUT) {
	    file.write((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
	    //auto size = cmd.buf.size();
	    file.write((char*)(&(cmd->old_size)), sizeof(uint32_t));
	    char* old_str = new char[cmd->old_size + 1];
	    old_str[cmd->old_size] = 0;
	    for(uint32_t i = 0; i < cmd->old_size; ++i) old_str[i] = (cmd->old_buf)[i];
	    file.write(old_str, cmd->old_size);//write the old buf before the put into the file. 
	    file.write((char*)(&(cmd->buf_size)), sizeof(uint32_t));//write the buf size to the log file too.
	    char* str = new char[cmd->buf_size + 1];
	    str[cmd->buf_size] = 0;
	    for(uint32_t i = 0; i < cmd->buf_size; ++i) str[i] = (cmd->buf)[i];
	    file.write(str, cmd->buf_size);//
	}
	else {
	    if(cmd->type == chfs_command::CMD_REMOVE || cmd->type == chfs_command::CMD_GET) {
	        file.write((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
	    }
	}
    }
    file.close();

    //uint32_t new_size = getFileSize(this->file_path_logfile);
    //if(new_size >= MAX_LOG_SZ) checkpoint();//after the new insert, make sure the logfile size doesn't the max size.
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A
    // check the size every time appending a new log, if the size exceeds the limit, put the previous log into the checkpoint file.
    std::ofstream checkpoint_file(this->file_path_checkpoint, std::ios::binary | std::ios::app);
    //std::ifstream log_file(file_path_logfile, std::ios::binary);
    restore_logdata();//resore the current log entries
    for(size_t i = 0; i < (this->log_entries).size(); ++i) {
        chfs_command* cmd = (chfs_command*)(&(log_entries[i]));
	checkpoint_file.write((char*)(&(cmd->type)), sizeof(cmd->type));
	checkpoint_file.write((char*)(&(cmd->id)), sizeof(cmd->id));
	if(cmd->type == chfs_command::CMD_CREATE) {
	    checkpoint_file.write((char*)(&(cmd->file_type)), sizeof(uint32_t));
	    checkpoint_file.write((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
	}
	if(cmd->type == chfs_command::CMD_PUT) {
	    checkpoint_file.write((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
	    checkpoint_file.write((char*)(&(cmd->old_size)), sizeof(uint32_t));
	    char* old_str = new char[cmd->old_size + 1];
	    old_str[cmd->old_size] = 0;
	    for(uint32_t i = 0; i < cmd->old_size; ++i) old_str[i] = (cmd->old_buf)[i];
	    checkpoint_file.write(old_str, cmd->old_size);
	    checkpoint_file.write((char*)(&(cmd->buf_size)), sizeof(uint32_t));
	    char* str = new char[cmd->buf_size + 1];
	    str[cmd->buf_size] = 0;
	    for(uint32_t i = 0; i < cmd->buf_size; ++i) str[i] = (cmd->buf)[i];
	    checkpoint_file.write(str, cmd->buf_size);
	}
	if(cmd->type == chfs_command::CMD_REMOVE) {
	    checkpoint_file.write((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
	}
    }
    log_entries.clear();
    checkpoint_file.close();
    std::ofstream log_file(this->file_path_logfile, std::ios::out | std::ios::trunc);//clear the previous log file information.
    log_file.close();
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A 
    std::ifstream file(this->file_path_logfile, std::ios::in | std::ios::binary);//open the logfile in the bianry type.
    if(!file.is_open()) return ;//the file doesn't exist
    printf("the logfile opened\n");
    //file.seekg(0, std::ios::beg);
    file.peek();
    if(file.eof()) {
        file.close();
	return ;
    }//the file is empty
    file.seekg(0, std::ios::beg);
    uint32_t size = 0;
    uint32_t size1 = 0;
    size1 = file.tellg();
    file.seekg(0, std::ios::end);
    size = file.tellg() - size1;
    file.seekg(0, std::ios::beg);
    /*
    uint32_t cur_size = 0;
    printf("ready to get into the loop!\n");
    while(cur_size != size) {
	//printf("get into the loop!\n");
        chfs_command* cmd = new chfs_command();
	file.read((char*)(&(cmd->type)), sizeof(cmd->type));
	file.read((char*)(&(cmd->id)), sizeof(cmd->id));
	printf("the type got from the LOG file:%d\n", cmd->type);
	printf("the ID got from the LOG file:%d\n", cmd->id);
	//command *cmd1 = (command*)cmd;
	//log_entries.push_back(cmd1);
	cur_size += cmd->size();
        if(cmd->type == chfs_command::CMD_CREATE) {
	    file.read((char*)(&(cmd->file_type)), sizeof(uint32_t));
	    file.read((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
	    cur_size += sizeof(cmd->file_type);
	    cur_size += sizeof(cmd->extent_id);
	}
        else {
	    if(cmd->type == chfs_command::CMD_PUT) {
	        file.read((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
		file.read((char*)(&(cmd->old_size)), sizeof(uint32_t));
		if((cmd->old_size) != 0) {
		    char* old_str = new char[cmd->old_size + 1];
		    file.read(old_str, cmd->old_size);
		    old_str[cmd->old_size] = 0;
		    std::string old_s(old_str);
		    if(old_s.size() < (cmd->old_size)) {
		        uint32_t pos = 0;
		        while(old_str[pos] != '\0') pos++;
		        std::string old_s1 = old_s.substr(0, pos);
		        while(old_str[pos] == '\0') {
		            old_s1 += '\0';
			    pos++;
		        }
		        while(pos < cmd->old_size) {
		            old_s1 += old_str[pos];
			    pos++;
			} 
		        old_s = old_s1;
		    }
		    cmd->old_buf = old_s;//handle the old_buf case.
		}
		else cmd->old_buf = "";
		cur_size += sizeof(uint32_t);
		cur_size += (cmd->old_size);
		file.read((char*)(&(cmd->buf_size)), sizeof(uint32_t));
		if(cmd->buf_size == 0) {
		    cur_size += sizeof(unsigned long long);
		    cur_size += sizeof(uint32_t);
		    cmd->buf = "";
		    command* cmd2 = (command*) cmd;
		    log_entries.push_back(*cmd2);
		    continue;
		}
		char* str = new char[cmd->buf_size + 1];
		file.read(str, cmd->buf_size);
		str[cmd->buf_size] = 0;//
		std::string s(str);//
		//NOTE:the '\0' in a std::string is just a common char, but the '\0' ina char* array is a special symbol to notify the end of the string!
		//That's why we have to solve the truncate case here.
		if(s.size() < (cmd->buf_size)) {
		    uint32_t pos = 0;
		    while(str[pos] != '\0') pos++;
		    std::string s1 = s.substr(0, pos);
		    while(str[pos] == '\0') {
		        s1 += '\0';
			pos++;
		    }
		    //std::string s2 = s.substr(pos);
		    //s = s1 + s2;
		    while(pos < cmd->buf_size) {
		        s1 += str[pos];
			pos++;
		    }
		    s = s1;//
		}//handle the '\0' case
		cmd->buf = s;
		//cmd->buf = (cmd->buf) + '\0';
		printf("got-size from the logfile: %d", cmd->buf_size);
		printf("the buf restored from the logfile: %s\n", (cmd->buf).data());
		//cmd->buf = (cmd->buf).substr(0, cmd->buf_size - 1);
		cur_size += sizeof(unsigned long long);
		//cur_size += sizeof(uint32_t);
		//cur_size += (cmd->old_size);
		cur_size += sizeof(uint32_t);
		cur_size += (cmd->buf_size);
	    }
	    else {
	        if(cmd->type == chfs_command::CMD_REMOVE || cmd->type == chfs_command::CMD_GET) {
		    file.read((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
		    cur_size += sizeof(unsigned long long);
		}
	    }
	}
	command* cmd1 = (command*) cmd;
	log_entries.push_back(*cmd1);
    } 
   */
    getData(file, log_entries, size);
    file.close();
    //std::ofstream clear_file(file_path_logfile, std::ios::out | std::ios::trunc);//clear the file data.
    //clear_file.close();
    //file.close();
};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A
    std::ifstream file(this->file_path_checkpoint, std::ios::in | std::ios::binary);
    if(!file.is_open()) return ;//the file doesn't exists
    file.peek();
    if(file.eof()) {
        file.close();
	return ;
    }
    file.seekg(0, std::ios::beg);
    uint32_t size0 = file.tellg();
    file.seekg(0, std::ios::end);
    uint32_t size = file.tellg() - size0;
    file.seekg(0, std::ios::beg);
    getData(file, log_entries, size);
    file.close();
    //std::ofstream file1(file_path_checkpoint, std::iois::out | std::ios::trunc);
    //file1.close();
};

template<typename command>
std::vector<command> persister<command>::getLogList() {
    return log_entries;
}

template<typename command>
std::vector<command> persister<command>::getCheckpointList() {
    return checkpoint_entries;
}

template<typename command>
void persister<command>::getData(std::ifstream& file, std::vector<command>& vec, uint32_t size)
{
    uint32_t cur_size = 0;
    //printf("ready to get into the loop!\n");
    while(cur_size != size) {
	//printf("get into the loop!\n");
        chfs_command* cmd = new chfs_command();
	file.read((char*)(&(cmd->type)), sizeof(cmd->type));
	file.read((char*)(&(cmd->id)), sizeof(cmd->id));
	printf("the type got from the LOG file:%d\n", cmd->type);
	printf("the ID got from the LOG file:%d\n", cmd->id);
	//command *cmd1 = (command*)cmd;
	//log_entries.push_back(cmd1);
	cur_size += cmd->size();
        if(cmd->type == chfs_command::CMD_CREATE) {
	    file.read((char*)(&(cmd->file_type)), sizeof(uint32_t));
	    file.read((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
	    cur_size += sizeof(cmd->file_type);
	    cur_size += sizeof(cmd->extent_id);
	}
        else {
	    if(cmd->type == chfs_command::CMD_PUT) {
	        file.read((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
		file.read((char*)(&(cmd->old_size)), sizeof(uint32_t));
		if((cmd->old_size) != 0) {
		    char* old_str = new char[cmd->old_size + 1];
		    file.read(old_str, cmd->old_size);
		    old_str[cmd->old_size] = 0;
		    std::string old_s(old_str);
		    if(old_s.size() < (cmd->old_size)) {
		        uint32_t pos = 0;
		        while(old_str[pos] != '\0') pos++;
		        std::string old_s1 = old_s.substr(0, pos);
		        while(old_str[pos] == '\0') {
		            old_s1 += '\0';
			    pos++;
		        }
		        while(pos < cmd->old_size) {
		            old_s1 += old_str[pos];
			    pos++;
			} 
		        old_s = old_s1;
		    }
		    cmd->old_buf = old_s;//handle the old_buf case.
		}
		else cmd->old_buf = "";
		cur_size += sizeof(uint32_t);
		cur_size += (cmd->old_size);
		file.read((char*)(&(cmd->buf_size)), sizeof(uint32_t));
		if(cmd->buf_size == 0) {
		    cur_size += sizeof(unsigned long long);
		    cur_size += sizeof(uint32_t);
		    cmd->buf = "";
		    command* cmd2 = (command*) cmd;
		    log_entries.push_back(*cmd2);
		    continue;
		}
		char* str = new char[cmd->buf_size + 1];
		file.read(str, cmd->buf_size);
		str[cmd->buf_size] = 0;//
		std::string s(str);//
		//NOTE:the '\0' in a std::string is just a common char, but the '\0' ina char* array is a special symbol to notify the end of the string!
		//That's why we have to solve the truncate case here.
		if(s.size() < (cmd->buf_size)) {
		    uint32_t pos = 0;
		    while(str[pos] != '\0') pos++;
		    std::string s1 = s.substr(0, pos);
		    while(str[pos] == '\0') {
		        s1 += '\0';
			pos++;
		    }
		    //std::string s2 = s.substr(pos);
		    //s = s1 + s2;
		    while(pos < cmd->buf_size) {
		        s1 += str[pos];
			pos++;
		    }
		    s = s1;//
		}//handle the '\0' case
		cmd->buf = s;
		//cmd->buf = (cmd->buf) + '\0';
		//printf("got-size from the logfile: %d", cmd->buf_size);
		//printf("the buf restored from the logfile: %s\n", (cmd->buf).data());
		//cmd->buf = (cmd->buf).substr(0, cmd->buf_size - 1);
		cur_size += sizeof(unsigned long long);
		//cur_size += sizeof(uint32_t);
		//cur_size += (cmd->old_size);
		cur_size += sizeof(uint32_t);
		cur_size += (cmd->buf_size);
	    }
	    else {
	        if(cmd->type == chfs_command::CMD_REMOVE || cmd->type == chfs_command::CMD_GET) {
		    file.read((char*)(&(cmd->extent_id)), sizeof(unsigned long long));
		    cur_size += sizeof(unsigned long long);
		}
	    }
	}
	command* cmd1 = (command*) cmd;
	vec.push_back(*cmd1);
    }
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h
