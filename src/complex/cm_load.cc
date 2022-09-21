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

#include "cm_load.h"
#include "config.h"
#include "protocol.h"
#include "cm_conn.h"
#include "log.h"

using namespace std;

#define STRCPY(d,s) do{ strncpy(d, s, sizeof(d)-1); d[sizeof(d)-1]=0; }while(0)
#define MIN(x,y) ((x)<=(y)?(x):(y))

#define GET_STR_CFG(s, k, d) do{ p = stCfgFile.GetStrVal(s, k); \
    if(p==NULL || p[0]==0){ \
	fprintf(stderr, "%s is null!\n", k); \
	return(-1); \
    } \
    STRCPY(d, p); \
}while(0)

std::map<string, TableInfo> g_table_set;


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

ConfigHelper::ConfigHelper ()
{
}

ConfigHelper::~ConfigHelper ()
{
}

void ConfigHelper::load_yaml_fields(YAML::Node node)
{
	if(node["primary"])
	{
		std::string type_string;
		std::string db = node["primary"]["db"].as<string>();
		std::string table = node["primary"]["table"].as<string>();
		std::string keystr = db;
		keystr += ".";
		keystr += table;

		if(node["primary"]["full"])
			type_string = "LAYERED TALBE";
		else if(node["primary"]["hot"]["sharding"])
			type_string = "SHARDING TALBE";
		else
			type_string = "";
		table_type_info[keystr] = type_string;
	}

	if(node["extension"])
	{
		std::string db = node["extension"]["logic"]["db"].as<string>();
		std::string table = node["extension"]["logic"]["table"].as<string>();
		std::string keystr = db;
		keystr += ".";
		keystr += table;
		table_type_info[keystr] = "SINGLE TABLE";
	}
}

void ConfigHelper::load_layered_info()
{
	unsigned int count = 0;
	DIR *dir;
	struct dirent *ptr;
	const char* rootpath = "../conf/";
	if ((dir = opendir(rootpath)) == NULL)
		return;

	while ((ptr = readdir(dir)) != NULL) {
		if (strcasecmp(ptr->d_name, "..") == 0 ||
		    strcasecmp(ptr->d_name, ".") == 0)
			continue;
		std::string path = rootpath;
		path += ptr->d_name;
		if(path.find("dtc-conf-") == string::npos)
			continue;
		log4cplus_debug("load path: %s", path.c_str());
		YAML::Node node;
		try 
		{
			node = YAML::LoadFile(path);
			if(node.IsNull())
				continue;
		}
		catch (const YAML::Exception &e) 
		{
			log4cplus_error("load yaml file error:%s", e.what());
			continue;
		}

		load_yaml_fields(node);
	}

	closedir(dir);
	return;
}

bool ConfigHelper::load_dtc_config(std::string conf_file)
{
	try 
	{
        dtc = YAML::LoadFile(conf_file);
		if(dtc.IsNull())
		{
			log4cplus_error("dtc null");
			return false;
		}
	}
	catch (const YAML::Exception &e) 
	{
		log4cplus_error("load dtc file error:%s", e.what());
		return false;
	}
	
	if(!load_full_inst_info())
	{
		log4cplus_error("load full database config error");
		return false;
	}

	if(!load_hot_inst_info())
	{
		log4cplus_error("load hot db config error");
		return false;
	}

	return true;
}

bool ConfigHelper::load_hot_inst_info()
{
	memset(&hot_instance, 0, sizeof(DBHost));

	if( !dtc["primary"]["hot"]["logic"]["connection"]["addr"] || 
		!dtc["primary"]["hot"]["logic"]["connection"]["user"] ||
		!dtc["primary"]["hot"]["logic"]["connection"]["pwd"] ||
		!dtc["primary"]["hot"]["logic"]["db"])
		return false;

	const char* addr = dtc["primary"]["hot"]["logic"]["connection"]["addr"].as<string>().c_str();
	const char* p = strrchr(addr, ':');
	if(p == NULL){
		strncpy(hot_instance.Host, addr, sizeof(hot_instance.Host)-1 );
		hot_instance.Port = 0;
	}
	else{
		strncpy(hot_instance.Host, addr, MIN(p - addr, (int)sizeof(hot_instance.Host)-1) );
		hot_instance.Port = atoi(p+1);
	}
	
	strncpy(hot_instance.User, dtc["primary"]["hot"]["logic"]["connection"]["user"].as<string>().c_str(), sizeof(hot_instance.User)-1 );
	strncpy(hot_instance.Password, dtc["primary"]["hot"]["logic"]["connection"]["pwd"].as<string>().c_str(), sizeof(hot_instance.Password)-1 );
	strncpy(hot_instance.DbName, dtc["primary"]["hot"]["logic"]["db"].as<string>().c_str(), sizeof(hot_instance.DbName)-1 );
	
	hot_instance.ConnTimeout = 10;
	hot_instance.ReadTimeout = 10;

	log4cplus_debug("hot Host:%s:%d, user:%s, pwd:%s, db:%s",
		hot_instance.Host, hot_instance.Port, hot_instance.User, hot_instance.Password, hot_instance.DbName);
	return true;
}

bool ConfigHelper::load_full_inst_info()
{
	memset(&full_instance, 0, sizeof(DBHost));

	if( !dtc["primary"]["full"]["logic"]["connection"]["addr"] || 
		!dtc["primary"]["full"]["logic"]["connection"]["user"] ||
		!dtc["primary"]["full"]["logic"]["connection"]["pwd"] ||
		!dtc["primary"]["full"]["logic"]["db"])
		return false;

	const char* addr = dtc["primary"]["full"]["logic"]["connection"]["addr"].as<string>().c_str();
	const char* p = strrchr(addr, ':');
	if(p == NULL){
		strncpy(full_instance.Host, addr, sizeof(full_instance.Host)-1 );
		full_instance.Port = 3358;
	}
	else{
		strncpy(full_instance.Host, addr, MIN(p - addr, (int)sizeof(full_instance.Host)-1) );
		full_instance.Port = atoi(p+1);
	}

	strncpy(full_instance.User, dtc["primary"]["full"]["logic"]["connection"]["user"].as<string>().c_str(), sizeof(full_instance.User)-1 );
	strncpy(full_instance.Password, dtc["primary"]["full"]["logic"]["connection"]["pwd"].as<string>().c_str(), sizeof(full_instance.Password)-1 );
	strncpy(full_instance.DbName, dtc["primary"]["full"]["logic"]["db"].as<string>().c_str(), sizeof(full_instance.DbName)-1 );

	full_instance.ConnTimeout = 10;
	full_instance.ReadTimeout = 10;
	log4cplus_debug("full_instance:%s:%d, user:%s, pwd:%s, db:%s", full_instance.Host, full_instance.Port, full_instance.User,
	full_instance.Password, full_instance.DbName);

	return true;
}

int ConfigHelper::GetIntValue(const char* key, int default_value)
{
	return default_value;
}

std::vector<int> ConfigHelper::GetIntArray(const char* key)
{
	std::vector<int> v;

	return v;
}

std::string ConfigHelper::GetStringValue(const char* key, std::string default_value)
{
	return default_value;
}
