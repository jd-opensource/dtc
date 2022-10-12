/*
* Copyright [2021] JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef _H_DTC_DB_CONFIG_H_
#define _H_DTC_DB_CONFIG_H_

#include "../table/table_def.h"
#include "config.h"
#include <stdint.h>
#include <vector>
#include <map>

#define MAXDB_ON_MACHINE 1000
#define GROUPS_PER_MACHINE 4
#define GROUPS_PER_ROLE 3
#define ROLES_PER_MACHINE 2
#define MACHINEROLESTRING "ms?????"

#define DB_FIELD_FLAGS_READONLY 1
#define DB_FIELD_FLAGS_UNIQ 2
#define DB_FIELD_FLAGS_DESC_ORDER 4
#define DB_FIELD_FLAGS_VOLATILE 8
#define DB_FIELD_FLAGS_DISCARD 0x10
#define DB_FIELD_FLAGS_HAS_DEFAULT 0x20
#define DB_FIELD_FLAGS_NULLABLE 0x40

/* 默认key-hash so文件名及路径 */
#define DEFAULT_KEY_HASH_SO_NAME "../lib/key-hash.so"
#define DEFAULT_KEY_HASH_FUNCTION "StringHash(1,128)"
/* key-hash接口函数 */
typedef uint64_t (*key_hash_interface)(const char *key, int len, int left,
				       int right);

enum { INSERT_ORDER_LAST = 0, INSERT_ORDER_FIRST = 1, INSERT_ORDER_PURGE = 2 };

enum { BY_MASTER, BY_SLAVE, BY_DB, BY_TABLE, BY_KEY, BY_FIFO };

typedef enum {
	DUMMY_HELPER = 0,
	DTC_HELPER,
	MYSQL_HELPER,
	TDB_HELPER,
	CUSTOM_HELPER,
} HELPERTYPE;

struct MachineConfig {
	struct {
		const char *addr;
		const char *user;
		const char *pass;
		const char *optfile;
		//DataMerge Addr
		const char *dm;
		char path[32];
	} role[GROUPS_PER_MACHINE]; // GROUPS_PER_ROLE should be ROLES_PER_MACHINE

	HELPERTYPE helperType;
	int mode;
	uint16_t dbCnt;
	uint16_t procs;
	uint16_t dbIdx[MAXDB_ON_MACHINE];
	uint16_t gprocs[GROUPS_PER_MACHINE];
	uint32_t gqueues[GROUPS_PER_MACHINE];
	bool is_same(MachineConfig *mach);
};

struct FieldConfig {
	const char *name;
	char type;
	int size;
	DTCValue dval;
	int flags;
	bool is_same(FieldConfig *field);
};

struct KeyHash {
	int keyHashEnable;
	int keyHashLeftBegin; /* buff 的左起始位置 */
	int keyHashRightBegin; /* buff  的右起始位置 */
	key_hash_interface keyHashFunction;
};

enum Layered {
	HOT = 0,
	FULL
};

enum Depoly{
	SINGLE = 0,
	SHARDING_DB_ONE_TAB = 1,
	SINGLE_DB_SHARDING_TAB = 2,
	SHARDING_DB_SHARDING_TAB = 3
};

class DbConfig {
public:
	DTCConfig *cfgObj;
	char *dbName;
	char *dbFormat;
	char *tblName;
	char *tblFormat;

	int dstype; /* data-source type: default is mysql   0: mysql  1: gaussdb  2: rocksdb */
	int checkTable;
	unsigned int dbDiv;
	unsigned int dbMod;
	unsigned int tblMod;
	unsigned int tblDiv;
	int fieldCnt;
	int keyFieldCnt;
	int idxFieldCnt;
	int machineCnt;
	int procs; //all machine procs total
	int database_max_count; //max db index
	enum Depoly depoly;

	struct KeyHash keyHashConfig;

	int slaveGuard;
	int autoinc;
	int lastacc;
	int lastmod;
	int lastcmod;
	int expireTime;
	int compressflag;
	int ordIns;
	char *ordSql;
	struct FieldConfig *field;
	struct MachineConfig *mach;

	static struct DbConfig *load_buffered(char *buf);
	static struct DbConfig *Load(const char *file);
	static struct DbConfig *Load(DTCConfig * , int i_server_type = 0);
	static bool build_path(char *path, int n, int pid, int group, int role,
			       int type);
	void destory(void);
	void dump_db_config(const struct DbConfig *cf);
	void set_helper_path(int);
	DTCTableDefinition *build_table_definition(void);
	int load_key_hash(DTCConfig *);
	bool compare_mach(DbConfig *config);
	bool Compare(DbConfig *config, bool compareMach = false);
	int find_new_mach(DbConfig *config, std::vector<int> &newMach,
			  std::map<int, int> &machMap);
	static int get_dtc_mode(YAML::Node dtc_config);
	static std::string get_shm_size(YAML::Node dtc_config);
	static int get_shm_id(YAML::Node dtc_config);
	static std::string get_bind_addr(YAML::Node dtc_config);

private:
	int get_dtc_config(YAML::Node dtc_config, DTCConfig* raw,int i_server_type);
	int convert_case_sensitivity(std::string& s_val);
};

#endif
