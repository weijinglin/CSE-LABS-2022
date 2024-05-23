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
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab4: Your definition here.

		// the file type need to handle
		mr_tasktype type;
		// the index of file need to deal with
		int file_index;
		// the index of task
		int mem_idx;
		// all the file_name
		vector<string> file_names;
	};

	struct AskTaskRequest {
		// Lab4: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

marshall &operator<<(marshall &m, const mr_protocol::AskTaskResponse &args)
{
	m << args.file_index;
	m << args.mem_idx;
	m << args.type;
	int size = args.file_names.size();
	m << size;
	for(auto file : args.file_names){
		m << file;
	}
}

unmarshall &operator>>(unmarshall &u, mr_protocol::AskTaskResponse &args)
{
	u >> args.file_index;
	u >> args.mem_idx;
	int type;
	u >> type;
	switch (type)
	{
	case 0:
	{
		/* code */
		args.type = mr_tasktype::NONE;
		break;
	}

	case 1:
	{
		/* code */
		args.type = mr_tasktype::MAP;
		break;
	}

	case 2:
	{
		/* code */
		args.type = mr_tasktype::REDUCE;
		break;
	}
	
	default:
		break;
	}
	int size;
	u >> size;
	std::string buf;
	for(int i = 0;i < size;++i){
		u >> buf;
		args.file_names.push_back(buf);
	}
}

#endif

