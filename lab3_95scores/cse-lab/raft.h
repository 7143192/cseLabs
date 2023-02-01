#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <chrono>
#include <random>
#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"
	using std::chrono::system_clock;
	template <typename state_machine, typename command>
	class raft {
	    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
	    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

	    friend class thread_pool;

	//#define RAFT_LOG(fmt, args...) \
	//    do {                       \
	//    } while (0);

#define RAFT_LOG(fmt, args...)                                                                                   \
	     do {                                                                                                         \
		 auto now =                                                                                               \
		     std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
			 std::chrono::system_clock::now().time_since_epoch())                                             \
			 .count();                                                                                        \
		 printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
	     } while (0);

	public:
	    raft(
		rpcs *rpc_server,
		std::vector<rpcc *> rpc_clients,
		int idx,
		raft_storage<command> *storage,
		state_machine *state);
	    ~raft();

	    // start the raft node.
	    // Please make sure all of the rpc request handlers have been registered before this method.
	    void start();

	    // stop the raft node.
	    // Please make sure all of the background threads are joined in this method.
	    // Notice: you should check whether is server should be stopped by calling is_stopped().
	    //         Once it returns true, you should break all of your long-running loops in the background threads.
	    void stop();

	    // send a new command to the raft nodes.
	    // This method returns true if this raft node is the leader that successfully appends the log.
	    // If this node is not the leader, returns false.
	    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    system_clock::duration election_time;//used to record the random election timeout for now.
    system_clock::time_point gotTime;//used to record the current time.
    /* ---- persist state on all servers ---- */
    //int currentTerm;//record the current term-id.
    int voteFor;//used to remember the candidateId of the cur term.-1 means nullptr.
    std::vector<char> snapshot_data;//used to record snapshot-data information.
    std::vector<log_entry<command>> log;
    /* ---- Volatile state on all server----  */
    int commitIndex;
    int lastApplied;
    int voted_num;
    std::vector<int> voted;//used to check whether the node i has voted for some other node.
    /* ---- Volatile state on leader----  */
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    //void MakeLeader();//make a node become the leader of the current term.
    void HeartBeat();//send the heart-beat to other nodes when a new leader is chosen.
    void RandomTime();//used to produce the random time for the timeout.
    void DoElection();
    void updatePersistInfo(int term, int voteFor, std::vector<log_entry<command>> log, std::vector<char> snapshot_data);//update the persisted meatdata and the log-entries.
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    /*
    voteFor = -1;//-1 means nothing.
    int size = rpc_clients.size();
    for(int i = 0; i < size; ++i) {
        voted.push_back(0);//0 means that this node doesn't vote for any node for now.
	nextIndex.push_back(1);
	matchIndex.push_back(0);
    }
    log_entry<command> le = log_entry<command>();
    log.push_back(le);//init the log, push a term=0 and id = 0's entry into it.
    voted_num = 0;
    commitIndex = log[0].index;
    lastApplied = log[0].index;
    gotTime = system_clock::now();
    RandomTime();//get the timeout of the election randomly.
    */
    //the meatdata contains the current-term before the node crashes and the vote-target info.
    
    int res = storage->Restore(current_term, voteFor, log, snapshot_data);//try to restore the metadata and the log-entries.
    if(res == -1) {
        //failed when resotre
	current_term = 0;
	voteFor = -1;
	log_entry<command> le = log_entry<command>();
	log.push_back(le);//push a dummy log-entry into the log-vec.
	snapshot_data.clear();
	//updatePersistInfo(current_term, voteFor, log, snapshot_data);
	storage->PersistMetaData(current_term, voteFor);
	storage->PersistLog(log);
	storage->PersistSnapShot(snapshot_data);
    }
    //CHANGED: add snapshot
    if(snapshot_data.size() != 0) state->apply_snapshot(snapshot_data);//if the snapshot is not empty, apply th snapshot data to the state machine.
    int size = rpc_clients.size();
    for(int i = 0; i < size; ++i) {
        voted.push_back(0);
	//nextIndex.push_back(1);
	matchIndex.push_back(0);
	nextIndex.push_back(1);
    }
    voted_num = 0;
    commitIndex = log[0].index;
    lastApplied = log[0].index;
    gotTime = system_clock::now();
    RandomTime();
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here
    /*
    int res = storage->Restore(current_term, voteFor, log, snapshot_data);//try to restore the metadata and the log-entries.
    if(res == -1) {
        //failed when resotre
	current_term = 0;
	voteFor = -1;
	log_entry<command> le = log_entry<command>();
	log.push_back(le);//push a dummy log-entry into the log-vec.
	snapshot_data.clear();
	updatePersistInfo(current_term, voteFor, log, snapshot_data);
    }
    //CHANGED: add snapshot
    if(snapshot_data.size() != 0) state->apply_snapshot(snapshot_data);//if the snapshot is not empty, apply th snapshot data to the state machine.
    int size = rpc_clients.size();
    for(int i = 0; i < size; ++i) {
        voted.push_back(0);
	nextIndex.push_back(1);
	matchIndex.push_back(0);
    }
    voted_num = 0;
    commitIndex = log[0].index;
    lastApplied = log[0].index;
    gotTime = system_clock::now();
    RandomTime();
    */
    //RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    //term = current_term;
    lock.lock();
    if(role != leader) {
	lock.unlock();
	return false;//only the leader can perform the new_command function.
    }
    //lock.lock();
    term = current_term;
    int last0 = log[log.size() - 1].index;
    index = last0 + 1;//update the new log-index.
    //RAFT_LOG("add new command %d in term %d", index, term);
    log_entry<command> entry = log_entry<command>(term, index, cmd);
    log.push_back(entry);//put the new log into the leader's log.

    storage->PersistLog(log);//a new entry is added, so update the persisted log data.

    int last = log[log.size() - 1].index;
    nextIndex[my_id] = last + 1;
    matchIndex[my_id] = last;
    lock.unlock();
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    snapshot_data = state->snapshot();//resotre the snapshot from the state machine.
    if(lastApplied > log[log.size() - 1].index) {
        //log.erase(log.begin(), log.end());
	log.clear();
    }	    
    else {//erase the ahead log-entries, and retain the left part.
        log.erase(log.begin(), log.begin() + lastApplied - log[0].index);
    }
    //update the persisted log
    storage->PersistLog(log);
    //update the persisted snapshot
    storage->PersistSnapShot(snapshot_data);
    lock.unlock();
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    // the reply should be empty at the start of the vote of this node.
    // this global lock will be released automatically after the namespace is end.
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);//use the global lock to protext the section.
    //RAFT_LOG("node %d get request_vote from %d", my_id, args.candidateId);
    lock.lock();
    reply.vote = false;//init the res of this vote to False.
    reply.term = current_term;//set the reply-term to the cur-term, so it can be used to update the term of the candidate.
    //RandomTime();
    gotTime = system_clock::now();
    if(args.candidateTerm > current_term) {
	voteFor = -1;
	role = follower;//the cur-term is less then the candidate's, so this node itself can only be a follower
	current_term = args.candidateTerm;//set the term to the bigger one.

	storage->PersistMetaData(current_term, voteFor);//metadatas are updated, update the corresponding persisted data.

	//RandomTime();
	//return 0;
    }
    if(args.candidateTerm < current_term) {
	lock.unlock();
	return 0;
    }
    if(voteFor == -1 || voteFor == args.candidateId) {
        int size = log.size();
	//log is empty/args have a bigger last-term-id/args and the cur-node have the same last-term-id and the args has a longer log.
	if(args.lastLogTerm > log[size - 1].term ||
			(args.lastLogTerm == log[size - 1].term && args.lastLogIndex >= log[size - 1].index)) {
	    reply.vote = true;//successfully voted.
	    voteFor = args.candidateId;

	    storage->PersistMetaData(current_term, voteFor);//voteFor is updated.
	}
    }	    
    lock.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    //gotTime = system_clock::now();
    lock.lock();
    if(reply.term > current_term) {
	//if the reply have a bigger term, that means the current node can't be a leader at this point
	//change its role to the follower directly.
        voteFor = -1;
	role = follower;
	current_term = reply.term;

	storage->PersistMetaData(current_term, voteFor);

	lock.unlock();
	//RandomTime();
	return ;//
    }
    if(role == candidate) {
        //only handle the case that the handler-node is a candidate
	//only when the reply votes successfully and the it doesn't vote for another node, it can vote for this candidate
        if(reply.vote == true && voted[target] == 0) {
	    voted_num++;
	    voted[target] = 1;
	    int size = rpc_clients.size();
	    if(voted_num > size / 2) {//majority
	        RAFT_LOG("the node %d will become the leader!", my_id);
		RandomTime();//produce a new timeout when has a new leader.
		role = leader;//change the role of the node.
		int new_next = (log[log.size() - 1].index) + 1;
		for(int i = 0; i < size; ++i) {
		    nextIndex[i] = new_next;//set the new-next index to the (old + 1)
		    matchIndex[i] = 0;//reinit after a new leader is chosen.
		}
		matchIndex[my_id] = new_next - 1;
		HeartBeat();//send the heartbeat to the other nodes.
	    }
	}	
    }
    lock.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    reply.success = false;
    reply.term = current_term;
    gotTime = system_clock::now();
    if(arg.term < current_term) {
	lock.unlock();
	return 0;//if the arg's term < cur-term
    }
    if(arg.term > current_term) {
        voteFor = -1;
	role = follower;
	current_term = arg.term;

	storage->PersistMetaData(current_term, voteFor);
	//return 0;
    }
    if(role == candidate) {
	//if the current node is a candidate but it receives a heartBeat from a leader,
	//that means this node can't be a cadidate to hold a vote
	//so change the role to the follower.
        voteFor = -1;
	role = follower;
	current_term = arg.term;

	storage->PersistMetaData(current_term, voteFor);
    }
    //case 2:if the prevLogIndex doesn't exist or the corresponding pos doesn't have the same term, return directly.
    //CHANGED:add snapshot, pyhsical index = logicalIndex - lastIncludedIndex(dummy entry in the log)
    if(arg.prevLogIndex > log[log.size() - 1].index || log[arg.prevLogIndex - log[0].index].term != arg.prevLogTerm) return 0; 
    //if(args.prevLogIndex <= log[log.size() - 1].index && log[arg.prevLogIndex].term == arg.prevLogTerm) {
    //RAFT_LOG("node %d require the node %d to append log", arg.leaderId, my_id);
    //reply.success = true;//in the following cases, the append can be regarded as a successful one.
    if(arg.entries.size() != 0) {
        //there are some new entries to be append to this->node.
	if(arg.prevLogIndex == log[log.size() - 1].index) {
	    //the prevLogIndex is just the end of the old log, so append it to the log directly.
	    for(int i = 0; i < arg.entries.size(); ++i) {
	        log.push_back(arg.entries[i]);
	    }

	    storage->PersistLog(log);//new log-entries are added into the log-vec, so update the persisted log data.
	}
	else {
	    int size = log.size();
	    //CHANGED:add snapshot, change the index exp.
	    for(int i = (arg.prevLogIndex + 1 - log[0].index); i < size; ++i) log.pop_back();//erase the back old-log-entries.
	    for(int i = 0; i < arg.entries.size(); ++i) {
	        log.push_back(arg.entries[i]);
	    }

	    storage->PersistLog(log);
	}
    }
    if(arg.leaderCommit > commitIndex) {//change the commitIndex according to the raft paper.
        commitIndex = std::min(arg.leaderCommit, log[log.size() - 1].index);
    }
    reply.success = true;
    //}
    lock.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    //std::unique_lock<std::mutex> lock(mtx, std::defer_lock);//remember to use the lock.
    //lock.lock();
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    if(role != leader) {
	lock.unlock();
        return ;//only the leader can give out the request of appending a log.
    }
    if(reply.term > current_term) {//the current-term has a lower term-id, so change its role to follower.
        voteFor = -1;
	role = follower;
	current_term = reply.term;

	storage->PersistMetaData(current_term, voteFor);
	lock.unlock();
	return ;
    }
    //lock.lock();
    if(reply.success == false) {
	//failed means that the log can't pair.
	nextIndex[node] = (arg.prevLogIndex < nextIndex[node]) ? arg.prevLogIndex : nextIndex[node];
	lock.unlock();
        return ;//if append failed, return directly.
    }
    //after the appending success, the leader should try to update the match and the next index of the target node.
    //matchIndex[node] = std::max((int)(arg.prevLogIndex + arg.entries.size()), matchIndex[node]);
    int tmp1 = (int)(arg.prevLogIndex + arg.entries.size());
    matchIndex[node] = (tmp1 >= matchIndex[node]) ? tmp1 : matchIndex[node];
    nextIndex[node] = matchIndex[node] + 1;
    //update the commitIndex when new entries has applied to majority of the nodes.
    
    std::vector<int> tmp;
    for(int i = 0; i < matchIndex.size(); ++i) {
        tmp.push_back(matchIndex[i]);
    } 
    std::sort(tmp.begin(), tmp.end());//sort the matchIndex 
    commitIndex = tmp[tmp.size() / 2];//beacause the log can be commited when majority nodes has this log, so the sorted-half val is the new commitId.
    lock.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    gotTime = std::chrono::system_clock::now();
    if(args.term > current_term) {//current term is not the newest, become a follower directly.
        role = follower;
	current_term = args.term;
	voteFor = -1;
	storage->PersistMetaData(current_term, voteFor);
    }
    if(args.term < current_term) {
	lock.unlock();
	return 0;//current term is bigger, so the arg should not be a leader, so return directly.
    }
    if(role == candidate) {
        role = follower;
	current_term = args.term;
	voteFor = -1;
	storage->PersistMetaData(current_term, voteFor);
    }
    //CHANGED: add snapshot
    if(args.lastIncludedIndex > log[log.size() - 1].index || log[args.lastIncludedIndex - log[0].index].term != args.lastIncludedTerm) {
        //case: the old-log does not contain the lastIncluded data.
	//so clear the old-log and append a new dummy entry into the log(work as the previous log-entry<0, 0>)
	log.clear();
	log_entry<command> entry = log_entry<command>();
	entry.term = args.lastIncludedTerm;
	entry.index = args.lastIncludedIndex;
	log.push_back(entry);
    }
    else {
        //the old log contains the lastIncluded information.
	//cleat the entries followed by this index.
	//remeber to save the lastIncluded index to act like a dummy entry!
	log.erase(log.begin(), log.begin() - log[0].index + args.lastIncludedIndex);
    }
    snapshot_data = args.data;//store the snapshot info at local node.
    state->apply_snapshot(args.data);
    storage->PersistLog(log);//update the log
    storage->PersistSnapShot(snapshot_data);//update the snapshot
    //if lastIncludedIndex is bigger, update the commitIndex
    commitIndex = (commitIndex > args.lastIncludedIndex) ? commitIndex : args.lastIncludedIndex;
    lastApplied = args.lastIncludedIndex;

    lock.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    lock.lock();
    if(role == candidate || role == follower) {
	lock.unlock();
        return ;
    }
    if(reply.term > current_term) {
        role = follower;
	voteFor = -1;
	current_term = reply.term;
	storage->PersistMetaData(current_term, voteFor);

	lock.unlock();
	return ;//
    }
    //update the matchIndex[node]
    //if the matchIndex[node] is bigger, means that the snapshot's end is ahead of the current-node's end.
    //otherwise, the log will be replaced by a dummy entry <lastIncludedIndex, lastIncludedTerm>
    matchIndex[node] = (matchIndex[node] > arg.lastIncludedIndex) ? matchIndex[node] : arg.lastIncludedIndex;
    nextIndex[node] = matchIndex[node] + 1;
    lock.unlock();
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    //RAFT_LOG("node %d run background election", my_id);
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
	//if(role == leader) return ;//if this node is the leader, it can't hold an election.
	lock.lock();
	if(role != leader && (system_clock::now() - gotTime) > election_time) {
	    DoElection();//go to do the election.
	}
	lock.unlock();
	std::this_thread::sleep_for(std::chrono::milliseconds(10));//sleep for 10ms.
    }    
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
	lock.lock();
//	if(role != leader) {
//	    lock.unlock();
//	    return ;
//	}
	//RAFT_LOG("leader %d commit a log in term %d", my_id, current_term);
	int size = rpc_clients.size();
	int pos = log[log.size() - 1].index;
	for(int i = 0; role == leader && i < size; ++i) {
	    if(i != my_id) {//don't append the log to the leader itself.
	        if(nextIndex[i] > log[log.size() - 1].index) continue;
		//CHANGED: add snapshot
                if(nextIndex[i] <= log[0].index) {
		    //the follower i has a too-old log, use the snapshot to update it.
		    //and in other words, the current leader has at least done one snapshot work.
		    install_snapshot_args arg;
		    arg.term = current_term;
		    arg.leaderId = my_id;
		    arg.offset = 0;
		    arg.done = false;
		    arg.data = snapshot_data;
		    arg.lastIncludedIndex = log[0].index;
		    arg.lastIncludedTerm = log[0].term;
		    thread_pool->addObjJob(this, &raft::send_install_snapshot, i, arg);
		}
		else {
		    append_entries_args<command> arg;
		    arg.term  = current_term;
		    arg.leaderId = my_id;
		    arg.prevLogIndex = nextIndex[i] - 1;
		    //CHANGED: add snapshot
		    arg.prevLogTerm = log[arg.prevLogIndex - log[0].index].term;
		    arg.leaderCommit = commitIndex;
		    std::vector<log_entry<command>> tmp;
		    //CHANGED: add snapshot
		    for(int j = nextIndex[i] - log[0].index; j <= pos - log[0].index; ++j) tmp.push_back(log[j]);//append the log that current node doesn't have according to the log of the leader.
		    arg.entries = tmp;
		    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);//send out the rpc.
		}
	    }
	}
	lock.unlock();
	std::this_thread::sleep_for(std::chrono::milliseconds(10));//sleep for a period time.
    }    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine
    // Work for all the nodes.
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
	lock.lock();
	//RAFT_LOG("apply a log at index %d", commitIndex);
	if(commitIndex > lastApplied) {
	    //CHANGED: add snapshot
	    int start = lastApplied + 1 - log[0].index;
	    //CHANGED: add snapshot
	    int end = commitIndex - log[0].index;
	    for(int i = start; i <= end; ++i) {
		//RAFT_LOG("apply a log at index %d\n", i);
	        state->apply_log(log[i].cmd);//apply the commited log to the state_machine.
	    }
	    lastApplied = commitIndex;
	}
	lock.unlock();
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    //if(role != leader) return ;//only the leader can send out the heartbeat to ping other machines.
    while (true) {
        if (is_stopped()) return;
	lock.lock();
        // Lab3: Your code here:
	if(role == leader) HeartBeat();//send the heartbeat.
	lock.unlock();
	//beacause the election timeout is between the 150ms and the 300ms, so make the sleep time is 150ms to make the system sound. 
	//std::this_thread::sleep_for(std::chrono::milliseconds(150));
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }    
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/
template <typename state_machine, typename command> 
void raft<state_machine, command>::HeartBeat() {
    //should send an EMPTY log-entry here.
    append_entries_args<command> arg;
    arg.term = current_term;
    arg.leaderId = my_id;
    //arg.prevLogIndex = log[log.size() - 1].index;
    //arg.prevLogTerm = log[log.size() - 1].term;
    arg.leaderCommit = commitIndex;
    std::vector<log_entry<command>> tmp;
    arg.entries = tmp;
    int size = rpc_clients.size();
    for(int i = 0; i < size; ++i) {
        if(i != my_id) {
	    arg.prevLogIndex = nextIndex[i] - 1;
	    arg.prevLogTerm = log[arg.prevLogIndex - log[0].index].term;
	    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
	}
    }
    return ;
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::RandomTime() {
    std::random_device rd;
    std::minstd_rand gen(rd());
    std::uniform_int_distribution<int> distrib(150, 300);
    //std::uniform_int_distribution<int> distrib(300, 500);
    election_time = std::chrono::duration_cast<system_clock::duration>(std::chrono::milliseconds(distrib(gen)));
}

template <typename state_machine, typename command>
void raft<state_machine, command>::DoElection() {
    RAFT_LOG("node %d go into the DoElection func", my_id);
    role = candidate;//change the role of the cur-node.
    int size = rpc_clients.size();
    for(int i = 0; i < size; ++i) voted[i] = 0;//reset the voted array.
    voted[my_id] = 1;
    voteFor = my_id;//set the vote-target to the current node.

    storage->PersistMetaData(current_term, voteFor);

    current_term++;
    voted_num = 1;//only itsel for now.
    request_vote_args arg = request_vote_args();
    arg.candidateTerm = current_term;
    arg.candidateId = my_id;
    arg.lastLogTerm = log[log.size() - 1].term;
    arg.lastLogIndex = log[log.size() - 1].index;
    RandomTime();//reset the election-timeout time.
    for(int i = 0; i < size; ++i) {
	if(i == my_id) continue;
        thread_pool->addObjJob(this, &raft::send_request_vote, i, arg);
    }  
    gotTime = std::chrono::system_clock::now();
}

template <typename state_machine, typename command>
void raft<state_machine, command>::updatePersistInfo(int term, int voteFor, std::vector<log_entry<command>> log, std::vector<char> snapshot_data) {
     storage->PersistMetaData(term, voteFor);
     storage->PersistLog(log);
     storage->PersistSnapShot(snapshot_data);
     return ;
}
#endif // raft_h
