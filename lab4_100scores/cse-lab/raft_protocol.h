#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    // Lab3: Your code here
    int candidateTerm;//the termId that the candidate that voting for
    int candidateId;//the candidate-node-id
    //log-entry-attrs
    int lastLogIndex;
    int lastLogTerm; 
    request_vote_args(int ct, int ci, int li, int lt) {
        candidateTerm = ct;
	candidateId = ci;
	lastLogIndex = li;
	lastLogTerm = lt;
    }
    request_vote_args() {
        candidateTerm = 0;
	candidateId = 0;
	lastLogIndex = 0;
	lastLogTerm = 0;
    }
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    // Lab3: Your code here
    int term;//the receiver's current term-id
    bool vote;//the bool-val used to check whether the candidate accept the vote.
    //int voteId;//the voter-node-id
    request_vote_reply() {
        term = 0;
	vote = false;
    }
    request_vote_reply(int t, bool v) {
        term = t;
	vote = false;
    }
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command>
class log_entry {
public:
    // Lab3: Your code here
    int term;//used to record a log-entry's term-id
    int index;//used to record a log-entry's index information of the entire log.
    command cmd;
    log_entry() {
        term = 0;
	index = 0;
    }
    log_entry(int t, int i, command c) {
        term = t;
	index = i;
	cmd = c;
    }
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Lab3: Your code here
    m << entry.term;
    m << entry.index;
    m << entry.cmd;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Lab3: Your code here
    u >> entry.term;
    u >> entry.index;
    u >> entry.cmd;
    return u;
}

template <typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;
    std::vector<log_entry<command>> entries;
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Lab3: Your code here
    m << args.term;
    m << args.leaderId;
    m << args.prevLogIndex;
    m << args.prevLogTerm;
    m << args.leaderCommit;
    m << args.entries;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Lab3: Your code here
    u >> args.term;
    u >> args.leaderId;
    u >> args.prevLogIndex;
    u >> args.prevLogTerm;
    u >> args.leaderCommit;
    u >> args.entries;
    return u;
}

class append_entries_reply {
public:
    // Lab3: Your code here
    int term;
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Lab3: Your code here
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    int offset;
    std::vector<char> data;
    bool done;
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Lab3: Your code here
    int term;
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h
