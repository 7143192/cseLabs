#include "chfs_state_machine.h"
using std::chrono::system_clock;
chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    //printf("get into the empty constructor function of chfs_command_raft!\n");
    cmd_tp = CMD_NONE;
    //type = 0;
    //id = 0;
    res = std::make_shared<result>();
    res->done = false;
    res->start = system_clock::now();
    //buf = "";
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
    //printf("get into the parameter comstructor function of chfs_command_raft!\n");
    //cmd.res->start = system_clock::now();
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    //marshall
    
    //int size = buf.size() + sizeof(type) + sizeof(id) + 4 + 1;
    //return size;
    int size = sizeof(command_type) + buf.size() + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t);
    //return 0;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
     
    //printf("get into a serialize function!\n");
    /*
    if(size < this->size()) return ;
    int size1 = 1;
    int size2 = sizeof(type);
    int size3 = sizeof(id);
    int size4 = buf.size();
    //printf("size3 = %d\n", size3);
    //printf("size4 = %d\n", size4);
    //serialize cmd_tp
    buf_out[0] = (char)cmd_tp;
    //serialize type
    for(int i = 0; i < size2; ++i) {
        int shift = 8 * (size2 - 1 - i);
	buf_out[i + size1] = (type >> shift) & 0xff;
    }
    //serialize id
    for(int i = 0; i < size3; ++i) {
        int shift = 8 * (size3 - 1 - i);
	buf_out[i + size1 + size2] = (id >> shift) & 0xff;
    }
    //serialize buf_size
    for(int i = 0;i < 4; ++i) {
        int shift = 8 * (3 - i);
	buf_out[i + size1 + size2 + size3] = (size4 >> shift) & 0xff;
    }
    //serialize buf
    strcpy(&buf_out[5 + size2 + size3], buf.data());
    //printf("end of one serialize!\n");
    */
    if(size != this->size()) return ;
    int buf_size = buf.size();
    memcpy(buf_out, &cmd_tp, sizeof(command_type));
    memcpy(buf_out + sizeof(command_type), &type, sizeof(uint32_t));
    memcpy(buf_out + sizeof(command_type) + sizeof(uint32_t), &id, sizeof(extent_protocol::extentid_t));
    memcpy(buf_out + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t), buf.c_str(), buf_size);
    return;
    
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    /*
    //printf("get into one deserialize!\n");
    //if(size < sizeof(type) + sizeof(id) + 5) return ;
    int size1 = 1;
    int size2 = sizeof(type);
    int size3 = sizeof(id);
    //deserialize cmd_tp
    cmd_tp = (chfs_command_raft::command_type)(buf_in[0]);
    //deserialize type
    for(int i = 0; i < size2; ++i) {
        int shift = (size2 - 1 - i) * 8;
	type |= (buf_in[i + size1] & 0xff) << shift;
    }
    //deserialize id
    for(int i = 0; i < size3; ++i) {
        int shift = (size3 - 1 - i) * 8;
	id |= (buf_in[i + size1 + size2] & 0xff) << shift;
    }
    //deserialize buf_size
    int buf_size = 0;
    for(int i = 0; i < 4; ++i) {
        int shift = (3 - i) * 8;
	buf_size |= (buf_in[i + size1 + size2 + size3] & 0xff) << shift;
    }
    //deserialize buf
    //if(size < buf_size + size1 + size2 + size3) return ;
    //for(int i = 0; i < buf_size - 1; ++i) {
    //    buf += buf_in[i + size1 + size2 + size3 + 4];
    //}
    
    buf.assign(&buf_in[5 + size2 + size3], buf_size);
    //printf("end of one deserialize!\n");
    return;
    */
    memcpy(&cmd_tp, buf_in, sizeof(command_type));
    memcpy(&type, buf_in + sizeof(command_type), sizeof(uint32_t));
    memcpy(&id, buf_in + sizeof(command_type) + sizeof(uint32_t), sizeof(extent_protocol::extentid_t));
    size_t buf_size = size - sizeof(command_type) - sizeof(uint32_t) - sizeof(extent_protocol::extentid_t);
    buf.resize(buf_size);
    memcpy(&buf[0], buf_in + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t), buf_size);
    return ;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    
    //printf("get into a chfs-marshall function!\n");
    m << (int)(cmd.cmd_tp);
    m << cmd.type;
    m << cmd.id;
    m << cmd.buf;
    //printf("get to the end of a chfs-marshall function!\n");
    
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    
    //printf("get into a chfs-unmarshall function!\n");
    int tp = 0;
    u >> tp;
    cmd.cmd_tp = (chfs_command_raft::command_type)(tp);
    //printf("unmarshall-cmd_tp = %d\n", cmd.cmd_tp);
    u >> cmd.type;
    u >> cmd.id;
    u >> cmd.buf;
    //printf("cmd.buf = %s\n", cmd.buf.data());
    //printf("get to the end of a chfs-unmarshall function!\n");
    
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    
    std::unique_lock<std::mutex> lock(mtx);//hold the global lock here.
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    //chfs_cmd.res->start = system_clock::now();
    //std::unique_lock<std::mutex> lock_res(chfs_cmd.res->mtx);
    // Lab3: Your code here
    //chfs_cmd.res->done = false;
    chfs_command_raft::command_type tp = chfs_cmd.cmd_tp;
    if(tp == chfs_command_raft::CMD_CRT) {
	//printf("es create\n");
	extent_protocol::extentid_t id;
        es.create(chfs_cmd.type, id);
	chfs_cmd.res->id = id;
	chfs_cmd.id = id;
    }
    if(tp == chfs_command_raft::CMD_PUT) {
	int r;
	printf("es put \n");
        es.put(chfs_cmd.id, chfs_cmd.buf, r);
	chfs_cmd.res->buf = chfs_cmd.buf;
	//printf("cmd.buf = %s\n", chfs_cmd.buf.data());
	//printf("cmd.res.buf = %s\n", chfs_cmd.res->buf.data());
    }
    if(tp == chfs_command_raft::CMD_GET) {
        std::string buf_tmp;
	//printf("es get\n");
        es.get(chfs_cmd.id, buf_tmp);
        chfs_cmd.res->buf = buf_tmp; 
    }
    if(tp == chfs_command_raft::CMD_GETA) {
        extent_protocol::attr a;
	//printf("es getattr\n");
        es.getattr(chfs_cmd.id, a);
	chfs_cmd.res->attr = a;	
    }
    if(tp == chfs_command_raft::CMD_RMV) {
        int r;
	//printf("es remove\n");
	es.remove(chfs_cmd.id, r);
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->id = chfs_cmd.id;
    chfs_cmd.res->tp = chfs_cmd.cmd_tp;
    chfs_cmd.res->cv.notify_all();
    return;
    
}


