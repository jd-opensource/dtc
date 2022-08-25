#include "rule.h"
#include <stdio.h>
#include <iostream>
#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include "re_comm.h"
#include "re_load.h"
#include "re_match.h"
#include "re_cache.h"
#include "log.h"
#include "yaml-cpp/yaml.h"

#define SPECIFIC_L1_SCHEMA "L1"
#define SPECIFIC_L2_SCHEMA "L2"
#define SPECIFIC_L3_SCHEMA "L3"

using namespace std;

extern vector<vector<hsql::Expr*> > expr_rules;
extern std::string conf_file;

std::string get_key_info(std::string conf)
{
    YAML::Node config;
    try {
        config = YAML::LoadFile(conf);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s\n", e.what());
		return "";
	}

    YAML::Node node = config["primary"]["cache"]["field"][0]["name"];
    if(node)
    {
        std::string keystr = node.as<string>();
        transform(keystr.begin(),keystr.end(),keystr.begin(),::toupper);
        return keystr;
    }
    
    return "";
}

extern "C" const char* rule_get_key(const char* conf)
{
    std::string strkey = get_key_info(conf);
    printf("222222222222, conf file: %s\n", conf);
    printf("key len: %d, key: %s\n", strkey.length(), strkey.c_str());
    if(strkey.length() > 0)
        return strkey.c_str();
    else
        return NULL;
}

extern "C" int rule_get_key_type(const char* conf)
{
    YAML::Node config;
    if(conf == NULL)
        return -1;

    try {
        config = YAML::LoadFile(conf);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s\n", e.what());
		return -1;
	}

    YAML::Node node = config["primary"]["cache"]["field"][0]["type"];
    if(node)
    {
        std::string str = node.as<string>();
        if(str == "signed")
            return 1;
        else if(str == "unsigned")
            return 2;
        else if(str == "float")
            return 3;
        else if(str == "string")
            return 4;
        else if(str == "binary")
            return 5;
        else   
            return -1;
    }
    return -1;
}

extern "C" int rule_sql_match(const char* szsql, const char* dbname, const char* conf)
{
    if(!szsql)
        return -1;
        
    std::string key = "";
    std::string sql = szsql;
    bool flag = false;

    init_log4cplus();

    if(conf)
    {
        conf_file = std::string(conf);
        flag = true;

        key = get_key_info(conf_file);
        if(key.length() == 0)
            return -1;
    }

    log4cplus_debug("key len: %d, key: %s, sql len: %d, sql: %s, dbname len: %d, dbname: %s", key.length(), key.c_str(), sql.length(), sql.c_str(), strlen(dbname), std::string(dbname).c_str());

    if(sql == "show databases" || sql == "SHOW DATABASES" || sql == "select database()" || sql == "SELECT DATABASE()")
    {
        return 3;
    }

    if(sql == "show tables" || sql == "SHOW TABLES")
    {
        if(dbname == NULL || strlen(dbname) == 0)
            return -6;
        else
            return 3;
    }

    log4cplus_debug("#############dbname:%s", dbname);
    if(dbname != NULL && strlen(dbname) > 0 && flag == false)
    {
        log4cplus_debug("#############111111111111");
        return 3;
    }
    log4cplus_debug("#############22222222222");
    int ret = re_load_rule();
    if(ret != 0)
    {
        log4cplus_error("load rule error:%d", ret);
        return -5;
    }

    if(sql.find("INSERT INTO") != sql.npos || sql.find("insert into") != sql.npos)
    {
        log4cplus_debug("INSERT request, force direct to L1.");
        //L1: DTC cache.
        return 1;
    }

    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(sql, &sql_ast) != 0)
        return -1;

    ret = re_match_sql(&sql_ast, expr_rules);
    if(ret == 0)
    {
        if(re_is_cache_sql(&sql_ast, key))
        {
            //L1: DTC cache.
            return 1;
        }
        else
        {
            //L2: sharding hot database.
            return 2;
        }
    }
    else {
        //L3: full database.
        return 3;
    }

    return 3;
}

extern "C" int sql_parse_table(const char* szsql, char* out)
{
    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(szsql, &sql_ast) != 0)
        return -1;

    std::string tablename = get_table_name(&sql_ast);
    if(tablename.length() > 0)
        strcpy(out, tablename.c_str());

    return tablename.length();
}