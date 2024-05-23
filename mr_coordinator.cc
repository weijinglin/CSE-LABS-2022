#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"


#include<chrono>

class mytimer
{
private:
    std::chrono::steady_clock::time_point _begin;
    std::chrono::steady_clock::time_point _end;
public:
    mytimer()
	{
		_begin = std::chrono::steady_clock::time_point();
		_end = std::chrono::steady_clock::time_point();
	}
    
	virtual ~mytimer(){};  
    
    // update the begin timestamp
    void UpDate()
    {
        _begin = std::chrono::steady_clock::now();
    }

	// get the time period from begin
    double GetSecond()
    {
        _end = std::chrono::steady_clock::now();
        std::chrono::duration<double> temp = std::chrono::duration_cast<std::chrono::duration<double>>(_end - _begin);
       	return temp.count();
    }
};

// 500 seconds is overtime
#define OVERTIME 500

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
	bool isFinishedMap();
	bool isFinishedReduce();
	bool Done();

	// return true if there left map job and return the index of the Task
	bool getMapTask(int &index);
	// return true if there left reduce job and return the index of the Task
	bool getReduceTask(int &index);

private:
	vector<string> files;
	vector<Task> mapTasks;
	vector<Task> reduceTasks;

	vector<int> map_work_num;
	vector<mytimer*> map_timer;
	vector<int> reduce_work_num;
	vector<mytimer*> reduce_timer;

	mutex mtx;

	long completedMapCount;
	long completedReduceCount;
	bool isFinished;
	
	string getFile(int index);
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
	// Lab4 : Your code goes here.
	if(!this->isFinishedMap()){
		// assign map job
		this->mtx.lock();
		int index;
		bool have_job = this->getMapTask(index);
		if(have_job){
			// assign map task
			this->mapTasks.at(index).isAssigned = true;
			reply.type = mr_tasktype::MAP;
			reply.file_index = this->mapTasks.at(index).index;
			reply.mem_idx = index;
			reply.file_names = this->files;
			// represent there has a worker to do the work
			this->map_work_num[index] = 1;
			this->map_timer.at(index)->UpDate();
		} else {
			// return null;
			// TODO(wjl) : check for over time
			int counter = 0;
			bool is_hit = false;
			for(auto task : this->mapTasks){
				double time_per = this->map_timer.at(counter)->GetSecond();
				if(time_per > OVERTIME){
					this->map_work_num[counter]++;
					// assign the task 
					this->map_timer.at(counter)->UpDate();
					reply.type = mr_tasktype::MAP;
					reply.file_index = task.index;
					reply.mem_idx = counter;
					reply.file_names = this->files;
					is_hit = true;
					break;	
				}
				counter++;
			}


			// return none
			if(!is_hit){
				// not overtime
				reply.type = mr_tasktype::NONE;
				reply.file_index = -1;
				reply.mem_idx = -1;
				reply.file_names = this->files;
			}
		}

		this->mtx.unlock();
	} else if(!this->isFinishedReduce()) {
		// assign reduce job
		this->mtx.lock();
		int index;
		bool have_job = this->getReduceTask(index);
		if(have_job){
			// assign map task
			this->reduceTasks.at(index).isAssigned = true;
			reply.type = mr_tasktype::REDUCE;
			reply.file_index = this->reduceTasks.at(index).index;
			reply.mem_idx = index;
			reply.file_names = this->files;
			// represent there has a worker to do the work
			this->reduce_work_num[index] = 1;
			this->reduce_timer.at(index)->UpDate();
		} else {
			// return null;
			// TODO(wjl) : check for over time
			int counter = 0;
			bool is_hit = false;
			for(auto task : this->reduceTasks){
				double time_per = this->reduce_timer.at(counter)->GetSecond();
				if(time_per > OVERTIME){
					this->reduce_work_num[counter]++;
					// assign the task 
					this->reduce_timer.at(counter)->UpDate();
					is_hit = true;
					reply.type = mr_tasktype::REDUCE;
					reply.file_index = task.index;
					reply.mem_idx = counter;
					reply.file_names = this->files;
					break;	
				}
				counter++;
			}

			// return none
			if(!is_hit){
				// not overtime
				reply.type = mr_tasktype::NONE;
				reply.file_index = -1;
				reply.mem_idx = -1;
				reply.file_names = this->files;
			}
			
		}

		this->mtx.unlock();
	} else {
		// with no task to handle
		this->isFinished = true;
		reply.type = mr_tasktype::NONE;
		reply.file_index = -1;
		reply.mem_idx = -1;
		reply.file_names = this->files;
	}
	return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
	// Lab4 : Your code goes here.
	this->mtx.lock();
	if(taskType == mr_tasktype::MAP){
		// TODO(wjl) : we assume here the index is the index of the array
		this->mapTasks[index].isCompleted = true;
		this->completedMapCount++;
		success = true;
	} else if(taskType == mr_tasktype::REDUCE){
		this->reduceTasks[index].isCompleted = true;
		this->completedReduceCount++;
		success = true;
	} else {
		// TODO(wjl) : nothing to do
		// this->isFinished = true;
		success = false;
	}
	this->mtx.unlock();
	return mr_protocol::OK;
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

// this function will be called in the protect of mutex so it don't need to acquire lock
bool Coordinator::getMapTask(int &index)
{
	bool have_job = false;
	int counter = 0;
	for(auto task : this->mapTasks){
		if(task.isAssigned == false){
			have_job = true;
			index = counter;
			break;
		}
		counter++;
	}
	return have_job;
}

// this function will be called in the protect of mutex so it don't need to acquire lock
bool Coordinator::getReduceTask(int &index)
{
	bool have_job = false;
	int counter = 0;
	for(auto task : this->reduceTasks){
		if(task.isAssigned == false){
			have_job = true;
			index = counter;
			break;
		}
		counter++;
	}
	return have_job;
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

	// TODO(wjl) : init the redo time
	for (int i = 0; i < filesize; i++) {
		this->map_work_num.push_back(0);
		this->map_timer.push_back(new mytimer());
	}
	for (int i = 0; i < nReduce; i++) {
		this->reduce_work_num.push_back(0);
		this->reduce_timer.push_back(new mytimer());
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

	server.reg(mr_protocol::asktask,&c,&Coordinator::askTask);
	server.reg(mr_protocol::submittask,&c,&Coordinator::submitTask);

	printf("debug in coordinator\n");

	while(!c.Done()) {
		sleep(1);
	}

	return 0;
}


