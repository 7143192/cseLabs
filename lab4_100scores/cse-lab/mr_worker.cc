#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

int GetHash(string s) {
    
    unsigned int ans = 0;
    for(int i = 0; i < s.size(); ++i) {
        char c = s[i];
	ans = ans * 131 + (int)(c);
    }
    int res = ans % REDUCER_COUNT;
    return res;
    
    //return 0;
}

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.
    vector<KeyVal> ans;
    int cur = 0;
    string tmp = "";
    while(cur < content.size()) {
        for(int i = cur; i < content.size(); ++i) {
	    char c = content[i];
	    if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
	        string tt = "a";
		tt[0] = c;
		tmp += tt;
	    }
	    else break;
	}
	if(tmp.size() != 0) {
	    KeyVal kv;
	    kv.key = tmp;
	    kv.val = "1";//the original val is all 1.
	    ans.push_back(kv);
	    cur += (int)(tmp.size());
	    //tmp = "";
	}
	else cur++;
	tmp = "";
    }
    return ans;

}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
    int ans = 0;
    for(size_t i = 0; i < values.size(); ++i) {
        string tmp = values[i];
	//ans += GetNum(tmp);
	ans += atoi(tmp.c_str());
    }
    string res = std::to_string(ans);
    return res;
	
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	//void doMap(int index, const vector<string> &filenames);
	void doMap(int index, string& filename);
	void doReduce(int index);
	
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;//should act as a lock.
	int id;

	rpcc *cl;
	std::string basedir;//the basedir should be the root dir of the file-path where we put the immediate files.
	MAPF mapf;
	REDUCEF reducef;

	int file_num;//used to record the um of the files.
	bool busy;//used to notify whether the current node is working some job.
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}

	file_num = 0;//init to 0.
	busy = false;//the node is free initially.
}

void Worker::doMap(int index, string& filename)
{
	// Lab4: Your code goes here.
	//vector<KeyVal> intermediate;
	//int num = filenames.size();
	//cout << "begin the Map job " << index << "!" << endl; 
	busy = true;

	//copied from the mr_sequential.cc
	//for (int i = 0; i < num; ++i) {
        //string filename = filenames[i];
        string content;
        // Read the whole file into the buffer.
        getline(ifstream(filename), content, '\0');
        vector <KeyVal> KVA = Map(filename, content);
        //intermediate.insert(intermediate.end(), KVA.begin(), KVA.end());
	/*
	for(size_t i = 0; i < KVA.size(); ++i) {
	    intermediate.push_back(KVA[i]);
	}
        //}//get all the words in the file.
 	sort(intermediate.begin(), intermediate.end(),
    	[](KeyVal const & a, KeyVal const & b) {
		return a.key < b.key;
	});//sort the pairs by the key(at this point all the value are all "1".)
	*/
	string path = basedir + "mr-" + std::to_string(index) + "-";
	vector<string> fileInfo(REDUCER_COUNT, "");
	/*
	for(int i = 0; i < REDUCER_COUNT; ++i) {
	    fileInfo.push_back("");
	}
	*/
	for(size_t i = 0; i < KVA.size(); ++i) {
	    string key = KVA[i].key;
	    string val = KVA[i].val;
	    int file_id = GetHash(key);
	    //string inPath = path + std::to_string(id);
	    fileInfo[file_id] = fileInfo[file_id] + key + '\n' + val + '\n';//use one line for the key, and another line for the val.
	    //fileInfo[0] = fileInfo[0] + key + ' ' + val + '\n';
	}
	for(int i = 0; i < REDUCER_COUNT; ++i) {
	    string inPath = path + std::to_string(i) + ".txt";
	    ofstream f;
	    f.open(inPath, std::ios::out);
	    //cout << "create a file" << inPath << "!" << endl;
	    f.write(fileInfo[i].c_str(), fileInfo[i].size());//put the corresponding info into the corresponding file.
	    //f << fileInfo[i];
	    f.close();
	}
	//cout << "end the Map job " << index << "!" << endl;
}

void Worker::doReduce(int index)
{
	// Lab4: Your code goes here.
	int num = file_num;//get the file_num first.
	//cout << "total num of the files:" << num << endl;
	busy = true;
	//cout << "begin the reduce job " << index << "!" << endl;	
	std::map<string, std::vector<string>> pairs;
	for(int i = 0; i < num; ++i) {
	    string path = basedir + "mr-" + std::to_string(i) + "-" + std::to_string(index) + ".txt";
	    ifstream file(path, std::ios::in);
	    string key = "";
	    string val = "";
	    while(file >> key >> val) {
		//cout << key << ' ' << val << endl;
	        pairs[key].push_back(val);
	    }//read all files, and put the corresponding pairs into the file.
	    //cout << "read a file " << path << " finished!" << endl;
	    file.close();
	}
	std::map<string, std::vector<string>>::iterator it;
	string ans = "";
	//cout << "ready to do the reduce job" << index << "!" << endl;
	for(it = pairs.begin(); it != pairs.end(); ++it) {
	    string got = Reduce(it->first, it->second);//reduce one key.
	    //cout << it->first << ' ' << got << endl;
	    ans = ans + it->first + ' ' + got + '\n';
	    //cout << "current ans = " << ans << endl;
	}
	//cout << "ans = "<<endl;
	//cout << ans << endl;
	string finalPath = basedir + "mr-out";
	ofstream out(finalPath, std::ios::out | std::ios::app);//append at the end of the file(beacause the reduce is a concurrent job.)
	out.write(ans.c_str(), ans.size());
	out.close();//remember to close the file.

	//cout << "end the reduce job " << index << "!" << endl;
	
	//cout << "begin a reduce job " << index << "!" << endl;
	/*
	std::map<string, int> pairs;
	for(int i = 0; i < num; ++i) {
	    string path = basedir + "mr-" + std::to_string(i) + "-" + std::to_string(index) + ".txt";
	    ifstream file(path, std::ios::in);
	    string key = "";
	    string val = "";
	    while(file >> key >> val) {
		//cout << key << ' ' << val << endl;
	        pairs[key] += atoi(val.c_str());
	    }//read all files, and put the corresponding pairs into the file.
	    //cout << "read a file " << path << " finished!" << endl;
	    file.close();
	}
	//cout << "end read all files for the reduce job " << index << endl;
	std::map<string, int>::iterator it;
	string ans = "";
	//cout << "ready to do the reduce job" << index << "!" << endl;
	for(it = pairs.begin(); it != pairs.end(); ++it) {
	    //string got = Reduce(it->first, it->second);//reduce one key.
	    //cout << it->first << ' ' << got << endl;
	    ans = ans + it->first + ' ' + std::to_string(it->second) + '\n';
	    //cout << "current ans = " << ans << endl;
	}
	//cout << "ans = "<<endl;
	//cout << ans << endl;
	string finalPath = basedir + "mr-out";
	ofstream out(finalPath, std::ios::out | std::ios::app);//append at the end of the file(beacause the reduce is a concurrent job.)
	out.write(ans.c_str(), ans.size());
	//out << ans << endl;
	out.close();//remember to close the file.
	//cout << "end the Reduce job " << index << "!" << endl;
	//busy = true;
	*/
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
	busy = false;
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab4: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
		mr_protocol::AskTaskResponse a;
		if(busy == false) {
		    cl->call(mr_protocol::asktask, id, a);
		    file_num = (a.filenames).size();
		}
		if(a.taskType == MAP) {
		    //cout << "get a map job" << a.index << "!" << endl;		    
		    doMap(a.index, a.filename);
		    doSubmit(MAP, a.index);
		}
		else {
		    //cout << "get a reduce job" << a.index << "!" << endl;
		    if(a.taskType == REDUCE) {
			//cout << "get a reduce job" << a.index << "!" << endl;
		        doReduce(a.index);
			doSubmit(REDUCE, a.index);
		    }
		    else {
			//cout << "get no job!" << endl;
		        //NONE
			//sleep(1);
			usleep(100000);
		    }
		}
	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;

	string init = argv[2];
	string basedir = "../ans_tmp/";
	mkdir("../ans_tmp", 0777);
	/*
	for(int i = 0; i < 6; ++i) {
	    for(int j = 0; j < REDUCER_COUNT; ++j) {
	        string path = "../ans_tmp/mr-" + std::to_string(i) + "-" + std::to_string(j) + ".txt";
		remove(path.c_str());
	    }
	}	
	string tmp_path = "../ans_tmp/mr-out";
	remove(tmp_path.c_str());
	*/
	Worker w(argv[1], basedir, mf, rf);
	w.doWork();

	return 0;
}
