#ifndef _DBP_GLOBAL_H_
#define _DBP_GLOBAL_H_
#include <string>
#include <map>
#include <fstream>
#include <sstream>
#include <vector>
#include <iostream>

#include "log.h"

#include "cm_load.h"
#include "protocol.h"
#include "waitqueue.h"
#include "task_request.h"

#define DEF_PID_FILE "async-conn.pid"

using namespace std;

typedef CThreadingWaitQueue<CTaskRequest*> TransThreadQueue;

class TableInfo{
public:
	string table_path;
	string socketaddr;

	int keytype;
	int keysize;

	std::string access_key;
};

class ConfigHelper ;
class TableInfo;


class TransactionInfo
{
public:
	std::string tablename_prefix;
	std::string sql;

	std::string szkey;
	int64_t ikey;
	uint64_t ukey;
	double dkey;
	
	int key_type;
	std::string adapt_sql;
};

extern std::map<string, TableInfo> g_table_set;
extern ConfigHelper  g_config;

#endif
