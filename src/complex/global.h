#ifndef _DBP_GLOBAL_H_
#define _DBP_GLOBAL_H_
#include <string>
#include <map>
#include <fstream>
#include <sstream>
#include <vector>
#include <iostream>

#include "log.h"

#include "dbconfig.h"
#include "protocol.h"
#include "waitqueue.h"
#include "task_request.h"

using namespace std;

/* 默认key-hash so文件名及路径 */
#define DEFAULT_KEY_HASH_SO_NAME	"../lib/key-hash.so"
/* key-hash接口函数 */
typedef uint64_t (*key_hash_interface)(const char *key, int len, int left, int right);

typedef CThreadingWaitQueue<CTaskRequest*> TransThreadQueue;

struct CKeyHash{
	int keyHashEnable;
	int keyHashLeftBegin; 	/* buff 的左起始位置 */
	int keyHashRightBegin;  /* buff 的右起始位置 */
	key_hash_interface keyHashFunction;
};

class TableInfo{
public:
	string table_path;
	string socketaddr;

	int keytype;
	int keysize;

	CKeyHash keyHashConfig;
	char depoly;
	int tblDiv;
	int tblMod;

	int dbDiv;
	int dbMod;

	std::string access_key;
};

class dbconfig;
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
extern int init_map_table_conf();
extern dbconfig g_dbconfig;

#endif
