#include "extent_server_dist.h"
using std::chrono::system_clock;
chfs_raft *extent_server_dist::leader() const {
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0) {
        return this->raft_group->nodes[0];
    } else {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id) {
    // Lab3: your code here
    
    chfs_command_raft cmd = chfs_command_raft();
    cmd.cmd_tp = chfs_command_raft::CMD_CRT;
    cmd.type = type;
    //cmd.res = std::make_shared<chfs_command_raft::result>();
    //cmd.res->done = false;
    //id is unknown at this point!
    //cmd->id = id;
    //cmd->buf = "";
    //cmd->res->type = type;
    //cmd->res->id = id;
    //cmd->res->buf = buf;
    //cmd->res->tp = chfs_command_raft::CMD_CRT;
    //cmd->res->done = false;
    //cmd->res->start = system_clock::now();
    int term;
    int index;
    chfs_raft* l = leader();//get the leader.
    //printf("begin to create a node %d", id);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    l->new_command(cmd, term, index);
    //std::unique_lock<std::mutex> lock(cmd.res->mtx);
    //while(!(cmd->res->done)) {
        //std::this_thread::sleep_for(std::chrono::milliseconds(10));
    //}
    //cmd.res->done = false;
    if(!(cmd.res->done)) {
        ASSERT((cmd.res->cv).wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2000)) == std::cv_status::no_timeout,
			"extent_server_dist:create cmd timeout");
    }
    id = cmd.res->id; 
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &) {
    // Lab3: your code here 
    
    chfs_command_raft cmd = chfs_command_raft();
    //cmd.res = std::make_shared<chfs_command_raft::result>();
    //cmd.res->done = false;
    cmd.cmd_tp = chfs_command_raft::CMD_PUT;
    //cmd->type = 0;
    cmd.id = id;
    //printf("cmd->id = %d\n", cmd.id);
    cmd.buf = buf;
    //printf("cmd->buf = %s\n", (cmd.buf).data());
    //cmd->res->id = id;
    //printf("cmd->res->id = %d\n", cmd->res->id);
    //cmd->res->buf = buf;
    //printf("cmd->res->buf = %s\n", cmd->res->buf);
    //cmd->res->tp = chfs_command_raft::CMD_PUT;
    //cmd.res->done = false;
    int term;
    int index;
    chfs_raft* l = leader();
    printf("begin to put a new file %d\n", id);
    l->new_command(cmd, term, index);
    printf("finish the put_new_command function!\n");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    //cmd.res->done = false;
    //while(!(cmd->res->done)) {
        //std::this_thread::sleep_for(std::chrono::milliseconds(10));
    //}
    if(!(cmd.res->done)) {
	//printf("be ready to the ASSERT!");
        ASSERT((cmd.res->cv).wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2000)) == std::cv_status::no_timeout,
			"extent_server_dist:put cmd timeout");
    }
    //printf("get to the pos of the end of the dist_put func!\n");
    
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf) {
    // Lab3: your code here
    
    chfs_command_raft cmd = chfs_command_raft();
    //cmd.res = std::make_shared<chfs_command_raft::result>();
    //cmd.res->done = false;

    cmd.cmd_tp = chfs_command_raft::CMD_GET;
    //cmd->type = 0;
    cmd.id = id;
    //cmd->buf = buf;
    //cmd->res->id = id;
    //cmd->res->buf = buf;
    //cmd->res->tp = chfs_command_raft::CMD_GET;
    //cmd->res->done = false;
    int term;
    int index;
    chfs_raft* l = leader();
    //printf("begin to get a file %d", id);
    l->new_command(cmd, term, index);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    //cmd.res->done = false;
    //while(!(cmd->res->done)) {
        //std::this_thread::sleep_for(std::chrono::milliseconds(10));
    //}
    if(!(cmd.res->done)) {
        ASSERT((cmd.res->cv).wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2000)) == std::cv_status::no_timeout,
			"extent_server_dist:get cmd timeout");
    }
    buf = cmd.res->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    // Lab3: your code here
    
    chfs_command_raft cmd = chfs_command_raft();
    //cmd.res = std::make_shared<chfs_command_raft::result>();
    //cmd.res->done = false;

    cmd.cmd_tp = chfs_command_raft::CMD_GETA;
    //cmd->type = 0;
    cmd.id = id;
    //cmd->res->id = id;
    //cmd->res->attr = a;
    //cmd->res->tp = chfs_command_raft::CMD_GETA;
    //cmd->res->done = false;
    int term;
    int index;
    chfs_raft* l = leader();
    //printf("begin to getattr of the file %d", id);
    l->new_command(cmd, term, index);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    //while(!(cmd->res->done)) {
        //std::this_thread::sleep_for(std::chrono::milliseconds(10));
    //}
    //cmd.res->done = false;
    if(!(cmd.res->done)) {
        ASSERT((cmd.res->cv).wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2000)) == std::cv_status::no_timeout,
			"extent_server_dist:getattr cmd timeout");
    }
    a = cmd.res->attr;
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &) {
    // Lab3: your code here
    
    chfs_command_raft cmd = chfs_command_raft();
    //cmd.res = std::make_shared<chfs_command_raft::result>();
    //cmd.res->done = false;

    cmd.cmd_tp = chfs_command_raft::CMD_RMV;
    cmd.id = id;
    //cmd->res->id = id;
    //cmd->res->tp = chfs_command_raft::CMD_RMV;
    //cmd->res->done = false;
    chfs_raft* l = leader();
    int term, index;
    //printf("begin to remove the file %d", id);
    l->new_command(cmd, term, index);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    //cmd.res->done = false;
    //while(!(cmd->res->done)) {
        //std::this_thread::sleep_for(std::chrono::milliseconds(10));
    //}
    if(!(cmd.res->done)) {
        ASSERT((cmd.res->cv).wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2000)) == std::cv_status::no_timeout,
			"extent_server_dist:remove cmd timeout");
    }
    
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist() {
    delete this->raft_group;
}
