#include <iostream>
#include <vector>
#include <string.h>
#include <time.h>
#include <sstream>
#include <unistd.h>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <dirent.h>
#include <sys/mman.h>
#include <dlfcn.h>
#include <map>

#include "dbconfig.h"
#include "config.h"
#include "protocol.h"
#include "DBConn.h"

using namespace std;

#define STRCPY(d,s) do{ strncpy(d, s, sizeof(d)-1); d[sizeof(d)-1]=0; }while(0)

#define GET_STR_CFG(s, k, d) do{ p = stCfgFile.GetStrVal(s, k); \
    if(p==NULL || p[0]==0){ \
	fprintf(stderr, "%s is null!\n", k); \
	return(-1); \
    } \
    STRCPY(d, p); \
}while(0)

std::map<string, TableInfo> g_table_set;


/* 前置空格已经过滤了 */
static char* skip_blank(char *p)
{
	char *iter = p;
	while(!isspace(*iter) && *iter != '\0')
		++iter;

	*iter = '\0';
	return p;
}

int init_servers(TableInfo* ti, const char* tableName)
{
	return 0;
}

int init_map_table_conf()
{
	DIR* dir;
    struct dirent* ptr;
    string rootdirPath = "../conf/";
    string x, table_path;
    dir = opendir((char *)rootdirPath.c_str());
	if(dir == NULL)
		return 0;
    while((ptr = readdir(dir)) != NULL) 
    {
		std::string filename = ptr->d_name;
		if(strlen(ptr->d_name) >= strlen(".conf") && filename.find(".conf") != string::npos && strcmp(ptr->d_name, "cache.conf") != 0)
		{
			x = ptr->d_name;
			table_path = rootdirPath + x;
			
			CConfig stCfgFile;
			if(stCfgFile.ParseConfig(table_path.c_str()) < 0)
			{
				log_error("get config error:%s", table_path.c_str());
				return -1;
			}

			const char* tname = stCfgFile.GetStrVal("TABLE_DEFINE", "TableName");
			TableInfo ti;

			ti.table_path = table_path;
			ti.keytype = stCfgFile.GetIntVal("FIELD1", "FieldType", 0);
			ti.keysize = stCfgFile.GetIntVal("FIELD1", "FieldSize", 0);
			char szaddr[1024] = {0};
			snprintf(szaddr, 1024, "%s.%s", g_dbconfig.GetStringValue("BindAddr").c_str(), tname);
			ti.socketaddr = szaddr;

			const char* str = stCfgFile.GetStrVal("TABLE_DEFINE", "AccessKey");
			if(str == NULL)
			{
				log_error("invalid AccessKey");
				return -1;
			}
			ti.access_key = str;

			int tblDiv = 0, tblMod = 0;
			const char *cp = stCfgFile.GetStrVal("TABLE_DEFINE", "TableNum");
			if(sscanf(cp?:"", "(%u,%u)", &tblDiv, &tblMod) != 2 || tblDiv==0 || tblMod==0)
			{
				log_error("invalid [TABLE_DEFINE].TableNum = %s", cp);
				return -1;
			}
			ti.tblDiv = tblDiv;
			ti.tblMod = tblMod;

			int dbDiv = 0, dbMod = 0;
			cp = NULL;
			cp = stCfgFile.GetStrVal("DB_DEFINE", "DbNum");
			if(sscanf(cp?:"", "(%u,%u)", &dbDiv, &dbMod) != 2 || dbDiv==0 || dbMod==0)
			{
				log_error("invalid [DB_DEFINE].DbNum = %s", cp);
				return -1;
			}
			ti.dbDiv = dbDiv;
			ti.dbMod = dbMod;

			ti.depoly = stCfgFile.GetIntVal("DB_DEFINE", "Deploy", 0);

			init_servers(&ti, tname);

			g_table_set[tname] = ti;

		}
    }
	if(dir)
    	closedir(dir);
	return 0;
}

void uninit_map_table_conf()
{
	return;
}


int GetDbIdx(void* Key, int FieldType, TableInfo* dbConfig) {
	int dbid = 0;
	uint64_t n;
	double f;

	if(Key != NULL && dbConfig->depoly != 0)
	{
		switch(FieldType)
		{
			case DField::Signed:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key,
							sizeof(int64_t),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);
				}
				else{
					if(*(int64_t*)Key >= 0)
						n = *(int64_t*)Key;
					else if(*(int64_t*)Key == LONG_LONG_MIN)
						n = 0;
					else
						n = 0 - *(int64_t*)Key;
				}
				
				dbid = (n/dbConfig->dbDiv)%dbConfig->dbMod;
				break;

			case DField::Unsigned:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key,
							sizeof(uint64_t),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);
				}
				else{
					n = *(uint64_t*)Key;
				}
			
				dbid = (n/dbConfig->dbDiv)%dbConfig->dbMod;
				break;

			case DField::Float:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key, 
							sizeof(float),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);

					dbid = (n/dbConfig->dbDiv)%dbConfig->dbMod;
				}
				else{
					if(*(float*)Key >= 0)
						f = *(float*)Key;
					else
						f = 0 - *(float*)Key;

					dbid = ((int)(f/dbConfig->dbDiv))%dbConfig->dbMod;
				}
				break;
			
			case DField::String:
			case DField::Binary:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key,
							strlen((const char*)Key),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);

					dbid = (n/dbConfig->dbDiv)%dbConfig->dbMod;
				}
				break;
		}
	}

	return dbid;
}


int GetTableIdx(void* Key, int FieldType, TableInfo* dbConfig) {
	int tableid = -1;
	uint64_t n;
	double f;

	if(Key != NULL && (dbConfig->depoly == 2 || dbConfig->depoly == 3))
	{
		switch(FieldType)
		{
			case DField::Signed:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key,
							sizeof(int64_t),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);
				}
				else{
					if(*(int64_t*)Key >= 0)
						n = *(int64_t*)Key;
					else if(*(int64_t*)Key == LONG_LONG_MIN)
						n = 0;
					else
						n = 0 - *(int64_t*)Key;
				}
				
				tableid = (n/dbConfig->tblDiv)%dbConfig->tblMod;
				break;

			case DField::Unsigned:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key,
							sizeof(uint64_t),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);
				}
				else{
					n = *(uint64_t*)Key;
				}
			
				tableid = (n/dbConfig->tblDiv)%dbConfig->tblMod;
				break;

			case DField::Float:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key, 
							sizeof(float),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);

					tableid = (n/dbConfig->tblDiv)%dbConfig->tblMod;
				}
				else{
					if(*(float*)Key >= 0)
						f = *(float*)Key;
					else
						f = 0 - *(float*)Key;

					tableid = ((int)(f/dbConfig->tblDiv))%dbConfig->tblMod;
				}
				break;
			
			case DField::String:
			case DField::Binary:
				if(dbConfig->keyHashConfig.keyHashEnable){
					n = dbConfig->keyHashConfig.keyHashFunction((const char*)Key,
							strlen((const char*)Key),
							dbConfig->keyHashConfig.keyHashLeftBegin,
							dbConfig->keyHashConfig.keyHashRightBegin);

					tableid = (n/dbConfig->tblDiv)%dbConfig->tblMod;
				}
				break;
		}
	}

	return tableid;
}


dbconfig::dbconfig(/* args */)
{
	config_path = "../conf/config.json";
}

dbconfig::~dbconfig()
{
}

void dbconfig::LoadConfigFile()
{
	ifstream ifile(config_path.c_str());
	ostringstream buf;
	char ch;
	while(buf && ifile.get(ch))
		buf.put(ch);
	m_data = buf.str();
	ifile.close();
}

bool dbconfig::InitSystemConfig()
{
	LoadConfigFile();

	return true;
}

int dbconfig::GetIntValue(const char* key, int default_value)
{
	return 0;
}

std::vector<int> dbconfig::GetIntArray(const char* key)
{
	std::vector<int> v;

	return v;
}

std::string dbconfig::GetStringValue(const char* key, std::string default_value)
{
	return default_value;
}


DTCConfig::DTCConfig()
{

}

DTCConfig::~DTCConfig()
{
}

int DTCConfig::SaveTableConf()
{
	return 0;
}

int DTCConfig::InitBizIdConf()
{
	DBHost host;
	strcpy(host.Host, g_dbconfig.GetStringValue("AdminHost").c_str());
	strcpy(host.User, g_dbconfig.GetStringValue("AdminUser").c_str());
	strcpy(host.Password, g_dbconfig.GetStringValue("AdminPwd").c_str());
	host.Port = g_dbconfig.GetIntValue("AdminPort");
	
	CDBConn DBConn(&host);

	std::vector<int> v = g_dbconfig.GetIntArray("DTCInst");
	int size = v.size();
	for(int i = 0; i < size; i++)
	{
		int biz_id = v[i];

		ostringstream oss;
		oss << "select access_token from dtc_business where business_id = " << biz_id << ";";
		int ret = DBConn.Query(g_dbconfig.GetStringValue("AdminDb").c_str(), oss.str().c_str());
		if(0 != ret)
		{
			const char *errmsg = DBConn.GetErrMsg();
			int m_ErrorNo = DBConn.GetErrNo();
			log_error("get dtc_businewss error. errno[%d]  errmsg[%s]", m_ErrorNo, errmsg);
			return m_ErrorNo;
		}
		ret = DBConn.UseResult();
		if (0 != ret) {
			log_error("can not use result");
			return -4;
		}

		int nRows = DBConn.ResNum; 
		for (int i = 0; i < nRows; i++) {
			ret = DBConn.FetchRow();
			if (0 != ret) {
				DBConn.FreeResult();
				return -4;
			}
			unsigned long *lengths = 0;
			lengths = DBConn.getLengths();
			if (0 == lengths) {
				DBConn.FreeResult();
				return -4;
			}

			//DBConn.Row[0];

		}
		DBConn.FreeResult();
	}

	return 0;
}