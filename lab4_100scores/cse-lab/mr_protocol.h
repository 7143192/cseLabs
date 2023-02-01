#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers { //used to register the rpc member functions.
		asktask = 0xa001,
		submittask,
	};

	//the ASK structs should be used by the mr_worker.
	struct AskTaskResponse {
		// Lab4: Your definition here.
		int index;
		int taskType;
		//int num;//used to store the num of the files.
		string filename;
		vector<string> filenames;
	};

	struct AskTaskRequest {
		// Lab4: Your definition here.
		int index;
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

marshall &operator<<(marshall& m, mr_protocol::AskTaskResponse a) {
    m << a.index;
    m << a.taskType;
    m << a.filename;
    int num = a.filenames.size();
    m << num;
    for(int i = 0; i < num; ++i) {
        m << a.filenames[i];
    }
    return m;
}

unmarshall &operator>>(unmarshall& u, mr_protocol::AskTaskResponse& a) {
    u >> a.index;
    u >> a.taskType;
    u >> a.filename;
    int num;
    u >> num;
    for(int i = 0; i < num; ++i) {
        string s;
	u >> s;
	a.filenames.push_back(s);
    }
    return u;
}

marshall &operator<<(marshall& m, mr_protocol::AskTaskRequest a) {
    m << a.index;
    return m;
}

unmarshall &operator>>(unmarshall& u, mr_protocol::AskTaskRequest &a) {
    u >> a.index;
    return u;
}

int GetNum(string s) {
    int ans = 0;
    for(size_t i = 0; i < s.size(); ++i) {
        int t = s[i] - '0';
	ans = ans * 10 + t;
    }
    return ans;
}


#endif

