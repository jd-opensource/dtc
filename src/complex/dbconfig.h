#ifndef _DBP_DBCONFIG_H_
#define _DBP_DBCONFIG_H_
#include "global.h"

class TableInfo;

void trim_space(char* str);
int init_map_table_conf();
bool init_unixsocket_addr();
int GetTableIdx(void* Key, int FieldType, TableInfo* dbConfig);
int GetDbIdx(void* Key, int FieldType, TableInfo* dbConfig);

class DTCConfig{
public:
	DTCConfig();
	~DTCConfig();

	int SaveTableConf();
	int InitBizIdConf();
};

class dbconfig
{
private:
	std::string config_path;
	std::string m_data;
public:
	dbconfig(/* args */);
	~dbconfig();
	bool InitSystemConfig();
    int GetIntValue(const char* key, int default_value = 0);
    std::string GetStringValue(const char* key, std::string default_value = "");
	std::vector<int> GetIntArray(const char* key);
private:
	void LoadConfigFile();
};

#endif