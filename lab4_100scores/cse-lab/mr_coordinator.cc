#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <iostream>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

struct Task {
	int taskType;     // should be either Mapper or Reducer
	bool isAssigned;  // has been assigned to a worker
	bool isCompleted; // has been finised by a worker
	int index;        // index to the file
};

class Coordinator {
public:
	Coordinator(const vector<string> &files, int nReduce);
	mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
	mr_protocol::status submitTask(int taskType, int index, bool &success);
	int CheckRemain(Task& task);//used to check whether there is a alive job at the point the coordinator accept the asktask rpc.
	bool isFinishedMap();
	bool isFinishedReduce();
	bool Done();

private:
	vector<string> files;
	vector<Task> mapTasks;
	vector<Task> reduceTasks;

	mutex mtx;
	mutex mtx1;

	long completedMapCount;
	long completedReduceCount;
	bool isFinished;
	
	string getFile(int index);
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
	// Lab4 : Your code goes here.
	//this->mtx1.lock();
	Task task;
	int got = CheckRemain(task);
	//this->mtx.lock();
	if(got == 1) {
	    //find a available job!
	    //this->mtx.lock();
	    reply.index = task.index;
	    reply.taskType = task.taskType;
	    reply.filename = this->files[reply.index];
	    reply.filenames = files;
	    //this->mtx.unlock();
	}
	else {
	    //no available job left!
	    reply.index = -1;
	    reply.taskType = NONE;
	}
	//this->mtx.unlock();
	//this->mtx1.unlock();
	return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
	// Lab4 : Your code goes here.
	this->mtx.lock();
	//success = false;
	if(taskType == MAP) {
	    mapTasks[index].isCompleted = true;
	    mapTasks[index].isAssigned = false;//release the finished job
	    completedMapCount++;
	}
	if(taskType == REDUCE) {
	    reduceTasks[index].isCompleted = true;
	    reduceTasks[index].isAssigned = false;//realease the finished job.
	    completedReduceCount++;
	}
	//bool map_end = isFinishedMap();
	//bool reduce_end = isFinishedReduce();
	//if(map_end == true && reduce_end == true) {
	if(completedMapCount >= long(this->mapTasks.size()) && completedReduceCount >= long(this->reduceTasks.size())) {
	    isFinished = true;//all jobs finished at this point.
	    //hack
	    /*
	    string ans_path = "./mr-out";
	    string tmp_path = "../ans_tmp/mr-out";
	    ifstream file1(tmp_path, std::ios::in);
	    string ans = "";
	    string key;
	    string val;
	    while(file1 >> key >> val) {
	        ans = ans + key + ' ' + val + '\n';
	    }
	    file1.close();
	    ofstream file2(ans_path, std::ios::out);
	    file2.write(ans.c_str(), ans.size());
	    file2.close();
	    */
	}
	//success = true;//successfully get to the end of the submitTask function.
	this->mtx.unlock();
	success = true;
	return mr_protocol::OK;
}

int Coordinator::CheckRemain(Task& task) {
	this->mtx.lock();
	if(completedMapCount < long(mapTasks.size())) {
	    for(size_t i = 0; i < mapTasks.size(); ++i) {
	        if(mapTasks[i].isAssigned == true || mapTasks[i].isCompleted == true) continue;
		task = mapTasks[i];
		mapTasks[i].isAssigned = true;//find a map job left.
		task.taskType = MAP;
		task.index = mapTasks[i].index;
		this->mtx.unlock();
		return 1;
	    }
	}
	else {
	    if(completedReduceCount < long(reduceTasks.size())) {
	    	//there may be a left reduce job.
		for(size_t i = 0; i < reduceTasks.size(); ++i) {
		    if(reduceTasks[i].isAssigned == true || reduceTasks[i].isCompleted == true) continue;
		    task = reduceTasks[i];//find a left reduce job.
		    reduceTasks[i].isAssigned = true;
		    task.taskType = REDUCE;
		    task.index = reduceTasks[i].index;
		    this->mtx.unlock();
		    return 1;
		}
	    }
	    else {
	        //all jobs are finished
		task.taskType = NONE;
		task.index = -1;
	    }
	}
	this->mtx.unlock();
	return 0;
}

string Coordinator::getFile(int index) {
	this->mtx.lock();
	string file = this->files[index];
	this->mtx.unlock();
	return file;
}

bool Coordinator::isFinishedMap() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedMapCount >= long(this->mapTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

bool Coordinator::isFinishedReduce() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedReduceCount >= long(this->reduceTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
	bool r = false;
	this->mtx.lock();
	r = this->isFinished;
	this->mtx.unlock();
	return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce)
{
	this->files = files;
	this->isFinished = false;
	this->completedMapCount = 0;
	this->completedReduceCount = 0;

	int filesize = files.size();
	for (int i = 0; i < filesize; i++) {
		this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
	}
	for (int i = 0; i < nReduce; i++) {
		this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
	}
}

int main(int argc, char *argv[])
{
	int count = 0;

	if(argc < 3){
		fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
		exit(1);
	}
	char *port_listen = argv[1];
	
	setvbuf(stdout, NULL, _IONBF, 0);

	char *count_env = getenv("RPC_COUNT");
	if(count_env != NULL){
		count = atoi(count_env);
	}

	vector<string> files;
	char **p = &argv[2];
	while (*p) {
		files.push_back(string(*p));
		++p;
	}

	rpcs server(atoi(port_listen), count);

	Coordinator c(files, REDUCER_COUNT);
	
	//
	// Lab4: Your code here.
	// Hints: Register "askTask" and "submitTask" as RPC handlers here
	// 
	
	server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
	server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

	while(!c.Done()) {
		sleep(1);
	}
        string ans_path = "./mr-out";
	string tmp_path = "../ans_tmp/mr-out";
	ifstream file1(tmp_path, std::ios::in);
	string ans = "";
	string key;
	string val;
	while(file1 >> key >> val) {
	    ans = ans + key + ' ' + val + '\n';
	}
	file1.close();
	ofstream file2(ans_path, std::ios::out);
	file2.write(ans.c_str(), ans.size());
	file2.close();
	
        for(int i = 0; i < 6; ++i) {
	    for(int j = 0; j < REDUCER_COUNT; ++j) {
	        string path = "../ans_tmp/mr-" + std::to_string(i) + "-" + std::to_string(j) + ".txt";
		remove(path.c_str());
	    }
	}
	string tmp_path_1 = "../ans_tmp/mr-out";
	remove(tmp_path_1.c_str());


	return 0;
}


