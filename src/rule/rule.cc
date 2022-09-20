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
#include "mxml.h"
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
		log4cplus_error("config file(%s) error:%s\n", conf, e.what());
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

bool ParseAgentConf(std::string path, std::vector<std::string>* vec){
    FILE *fp = fopen(path.c_str(), "r");
    if (fp == NULL) {
        log4cplus_error("conf: failed to open configuration '%s': %s", path.c_str(), strerror(errno));
        return false;
    }
    mxml_node_t* tree = mxmlLoadFile(NULL, fp, MXML_TEXT_CALLBACK);
    if (tree == NULL) {
        log4cplus_error("mxmlLoadFile error, file: %s", path.c_str());
        return false;
    }
    fclose(fp);

	mxml_node_t *poolnode, *servernode, *instancenode, *lognode;

	for (poolnode = mxmlFindElement(tree, tree, "MODULE",
	NULL, NULL, MXML_DESCEND); poolnode != NULL;
			poolnode = mxmlFindElement(poolnode, tree, "MODULE",
			NULL, NULL, MXML_DESCEND)) 
	{    
        std::string get_str;

        char* name = (char *)mxmlElementGetAttr(poolnode, "Name");
        if(name != NULL)
        {
            get_str = name;
            std::transform(get_str.begin(), get_str.end(), get_str.begin(), ::toupper);
            vec->push_back(get_str);
        }        
	}

    mxmlDelete(tree);
    
    return true;
}

int is_ext_table(hsql::SQLParserResult* ast,const char* dbname)
{
    //get db.tb array
    std::vector<std::string> agent_info;
    
    //load agent.xml
    if(ParseAgentConf("../conf/agent.xml", &agent_info) == false)
    {
        log4cplus_error("ParseAgentConf");
        return -1;
    }

    //get table name
    std::string table_name = get_table_name(ast);
    if(table_name.length() == 0)
    {
        log4cplus_debug("table name can not be found");
        return -1;
    }

    //combine db.tb
    std::string cmp_str = dbname;
    cmp_str += ".";
    cmp_str += table_name;
    std::transform(cmp_str.begin(), cmp_str.end(), cmp_str.begin(), ::toupper);

    //string compare between array and above string
    std::vector<std::string>::iterator iter;
    for(iter = agent_info.begin(); iter < agent_info.end(); iter++)
    {
        if(*iter == cmp_str)
            return 0;
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

    if(sql == "SHOW DATABASES" || sql == "SELECT DATABASE()")
    {
        return 3;
    }

    if(sql == "SHOW TABLES")
    {
        if(dbname == NULL || strlen(dbname) == 0)
            return -6;
        else
            return 2;
    }

    if(sql.find("WITHOUT@@") != sql.npos)
    {
        log4cplus_debug("data-lifecycle request, force direct to L1.");
        //L1: DTC cache.
        return 1;
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

    /*if(sql.find("INSERT INTO") != sql.npos || sql.find("UPDATE") != sql.npos || sql.find("DELETE FROM") != sql.npos)
    {
        log4cplus_debug("INSERT/UPDATE/DELETE request, force direct to L1.");
        //L1: DTC cache.
        return 1;
    }*/

    hsql::SQLParserResult* ast = NULL;
    hsql::SQLParserResult ast2;
    std::string tempsql = "SELECT * FROM TMPTB ";
    if(sql.find("WHERE") != -1)
    {
        tempsql += sql.substr(sql.find("WHERE"));
        if(re_parse_sql(tempsql, &ast2) != 0)
            return -1;
        log4cplus_debug("temsql: %s", tempsql.c_str());
        ast = &ast2;
    }


    if(sql.find("INSERT INTO") != -1 && sql.find("WHERE") != -1)
    {
        sql = sql.substr(0, sql.find("WHERE"));
        log4cplus_debug("new sql: %s", sql.c_str());
    }

    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(sql, &sql_ast) != 0)
        return -1;

    if(is_ext_table(&sql_ast, dbname) != 0)
        return 2;
    ret = re_match_sql(&sql_ast, expr_rules, ast);  //rule match
    if(ret == 0)
    {
        if(re_is_cache_sql(&sql_ast, key))  //if exist dtc key.
        {
            //L1: DTC cache.
            std::string tab_name = get_table_name(&sql_ast);
            std::string conf_tab_name = re_load_table_name();
            if(tab_name == conf_tab_name)
                return 1;
            else
            {
                log4cplus_error("table name dismatch: %s, %s", tab_name.c_str(), conf_tab_name.c_str());
                return 3;
            }
        }
        else
        {
            //L2: sharding hot database.
            return 2;
        }
    }
    else if(ret == -100)
    {
        //writing without match rule, Refuse.
        return -6;
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
    {
        strcpy(out, tablename.c_str());
    }
    return tablename.length();
}