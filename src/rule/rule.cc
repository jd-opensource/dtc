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
using namespace hsql;

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

extern "C" int rule_get_key(const char* conf, char* out)
{
    std::string strkey = get_key_info(conf);
    printf("conf file: %s\n", conf);
    printf("key len: %d, key: %s\n", strkey.length(), strkey.c_str());
    if(strkey.length() > 0)
    {
        strcpy(out, strkey.c_str());
        return strkey.length();
    }

    return 0;
}

extern "C" int get_statement_value(char* str, int len, const char* strkey, int* start_offset, int* end_offset)
{
    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(str, &sql_ast) != 0)
        return -1;

    StatementType t = sql_ast.getStatement(0)->type();
    if(t == kStmtInsert)
    {
        const InsertStatement* stmt = (const InsertStatement*)(sql_ast.getStatement(0));
        if(stmt->columns == NULL)  // for all
        {
            int i = 0;
            int pos = 0;
            char sztmp[100] = {0};
            std::string strsql;
			switch (stmt->values->at(i)->type) 
            {
                case hsql::ExprType::kExprLiteralInt:
                    sprintf(sztmp, "%d", stmt->values->at(i)->ival);
                    strsql = str;
                    pos = strsql.find(sztmp);
                    if(pos != string::npos)
                    {
                        *start_offset = pos;
                        *end_offset = pos + strlen(sztmp);
                        return 0;
                    }
                    else
                        return -1;
                case hsql::ExprType::kExprLiteralFloat:
                    sprintf(sztmp, "%f", stmt->values->at(i)->fval);
                    strsql = str;
                    pos = strsql.find(sztmp);
                    if(pos != string::npos)
                    {
                        *start_offset = pos;
                        *end_offset = pos + strlen(sztmp);
                        return 0;
                    }
                    else
                        return -1;
                case hsql::ExprType::kExprLiteralString:
                    strsql = str;
                    pos = strsql.find(stmt->values->at(i)->name);
                    if(pos != string::npos)
                    {
                        *start_offset = pos;
                        *end_offset = pos + strlen(stmt->values->at(i)->name);
                        return 0;
                    }
                    else
                        return -1;
                default:
                    return -1;
            }
        }
        else if(stmt->columns->size() >= 0) // specified
        {
            for(int i = 0; i < stmt->columns->size(); i++)
            {
                if(std::string(stmt->columns->at(i)) == std::string(strkey))
                {
                    std::string strsql;
                    int pos = 0;
                    char sztmp[100] = {0};
                    switch (stmt->values->at(i)->type) {
                    case hsql::ExprType::kExprLiteralInt:
                        sprintf(sztmp, "%d", stmt->values->at(i)->ival);
                        strsql = str;
                        pos = strsql.find(sztmp);
                        if(pos != string::npos)
                        {
                            *start_offset = pos;
                            *end_offset = pos + strlen(sztmp);
                            return 0;
                        }
                        else
                            return -1;
                    case hsql::ExprType::kExprLiteralFloat:
                        sprintf(sztmp, "%f", stmt->values->at(i)->fval);
                        strsql = str;
                        pos = strsql.find(sztmp);
                        if(pos != string::npos)
                        {
                            *start_offset = pos;
                            *end_offset = pos + strlen(sztmp);
                            return 0;
                        }
                        else
                            return -1;
                    case hsql::ExprType::kExprLiteralString:
                        strsql = str;
                        pos = strsql.find(stmt->values->at(i)->name);
                        if(pos != string::npos)
                        {
                            *start_offset = pos;
                            *end_offset = pos + strlen(stmt->values->at(i)->name);
                            return 0;
                        }
                        else
                            return -1;
                    default:
                        return -1;
                    }
                }
            }
        }
    }
    else
        return -1;
}

extern "C" int get_table_with_db(const char* sessiondb, const char* sql, char* result)
{
	if(result == NULL)
		return -1;
    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(sql, &sql_ast) != 0)
        return -2;
	memset(result, 0, 300);

    // Get db name
    std::string sqldb = get_schema(&sql_ast);
    if(sqldb.length() > 0)
    {
        strcat(result, sqldb.c_str());
    }
    else if(exist_session_db(sessiondb))
    {
        strcat(result, sessiondb);
    }
    else
    {
        log4cplus_error("no database selected.");
        return -3;
    }

    //Append symbol.
    strcat(result, ".");

    // Get table name
    std::string sqltb = get_table_name(&sql_ast);
    if(sqltb.length() > 0)
    {
        strcat(result, sqltb.c_str());
    }
    else
    {
        return -4;
    }

    return 0;
}

extern "C" int rule_get_key_type(const char* conf)
{
    YAML::Node config;
    if(conf == NULL)
        return -1;
    
    if(strlen(conf) <= 0)
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
        return -2;
    }

    std::string schema = get_schema(ast);
    //combine db.tb
    std::string cmp_str;
    if(schema.length() > 0)
    {
        cmp_str += schema;
    }
    else if(dbname != NULL && strlen(dbname) > 0) 
    {
        cmp_str += dbname;
    }
    else
    {
        log4cplus_debug("db can not be found");
        return -2;
    }
    cmp_str += ".";
    cmp_str += table_name;
    std::transform(cmp_str.begin(), cmp_str.end(), cmp_str.begin(), ::toupper);

    //string compare between array and above string
    std::vector<std::string>::iterator iter;
    for(iter = agent_info.begin(); iter < agent_info.end(); iter++)
    {
        log4cplus_debug("cmp: %s, %s", iter->c_str(), cmp_str.c_str());
        if(*iter == cmp_str)
            return 0;
    }

    return -1;
}

bool is_set_with_ast(hsql::SQLParserResult* ast)
{
    int t = ast->getStatement(0)->type();
    if(t == kStmtSet)
    {
        return true;
    }

    return false;
}

bool is_show_create_table_with_ast(hsql::SQLParserResult* ast)
{
    int t = ast->getStatement(0)->type();
    if(t == kStmtShow)
    {
        ShowStatement* stmt = (ShowStatement*)ast->getStatement(0);
        if(stmt->type == ShowType::kShowCreateTables)
            return true;
    }

    return false;
}

bool is_show_table_with_ast(hsql::SQLParserResult* ast)
{
    int t = ast->getStatement(0)->type();
    if(t == kStmtShow)
    {
        ShowStatement* stmt = (ShowStatement*)ast->getStatement(0);
        if(stmt->type == ShowType::kShowTables)
            return true;
    }

    return false;
}

bool is_show_db_with_ast(hsql::SQLParserResult* ast)
{
    int t = ast->getStatement(0)->type();
    if(t == kStmtShow)
    {
        //show databases;
        ShowStatement* stmt = (ShowStatement*)ast->getStatement(0);
        if(stmt->type == ShowType::kShowDatabases)
            return true;
    }
    else if(t == kStmtSelect)
    {
        //select database();
        SelectStatement* stmt = (SelectStatement*)ast->getStatement(0);
        if(stmt->select_object_type == SelectObjectType::kDataBase)
            return true;
    }

    return false;
}

bool exist_session_db(const char* dbname)
{
    if(dbname != NULL && strlen(dbname) > 0)
        return true;
    
    return false;
}

bool exist_sql_db(hsql::SQLParserResult* ast)
{
    if(get_schema(ast).length() > 0)
        return true;
    
    return false;
}

extern "C" int rule_sql_match(const char* szsql, const char* osql, const char* dbname, const char* conf)
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
        log4cplus_debug("flag is true, conf: %s", conf);
        key = get_key_info(conf_file);
        if(key.length() == 0)
            return -1;
    }

    log4cplus_debug("key len: %d, key: %s, sql len: %d, sql: %s, dbname len: %d, dbname: %s", key.length(), key.c_str(), key.length(), osql, strlen(dbname), std::string(dbname).c_str());

    if(sql.find("WITHOUT@@") != sql.npos)
    {
        log4cplus_debug("data-lifecycle request, force direct to L1.");
        //L1: DTC cache.
        return 1;
    }

    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(sql, &sql_ast) != 0)
        return -1;

    if(is_show_db_with_ast(&sql_ast))
    {
        return 3;
    }

    if(is_set_with_ast(&sql_ast))
        return 2;

    if(is_show_table_with_ast(&sql_ast))
    {
        if(exist_session_db(dbname))
            return 2;
        else
            return -6;
    }

    if(is_show_create_table_with_ast(&sql_ast))
        return 2;

    log4cplus_debug("flag: %d %d %d", flag, exist_session_db(dbname), exist_sql_db(&sql_ast));
    if((exist_session_db(dbname) || (exist_sql_db(&sql_ast))) && flag == false)
    {
        log4cplus_debug("db session & single table");
        return 2;
    }

    int ret = re_load_rule();
    if(ret != 0)
    {
        log4cplus_error("load rule error:%d", ret);
        return -5;
    }

    //Building condition sql tree, in order to do layered rule matching.
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
    else if(sql_ast.getStatement(0)->type() == StatementType::kStmtInsert)
    {
        tempsql += "WHERE ";
        const InsertStatement* stmt = (const InsertStatement*)(sql_ast.getStatement(0));
        if(stmt->columns == NULL)  // for all, not supported right now.
            return -1;
        for(int i = 0; i < stmt->columns->size(); i ++)
        {
            char sztmp[100] = {0};
            tempsql += stmt->columns->at(i);
            tempsql += "=";
            log4cplus_debug("name: %s, type: %d", stmt->columns->at(i), stmt->values->at(i)->type);
            if(stmt->values->at(i)->type == hsql::ExprType::kExprLiteralInt)
            {
                sprintf(sztmp, "%d", stmt->values->at(i)->ival);
                tempsql += sztmp;
            }
            else if(stmt->values->at(i)->type == hsql::ExprType::kExprLiteralFloat)
            {
                sprintf(sztmp, "%f", stmt->values->at(i)->fval);
                tempsql += sztmp;
            }
            else if(stmt->values->at(i)->type == hsql::ExprType::kExprLiteralString)
            {
                tempsql += "'";
                tempsql += stmt->values->at(i)->name;
                tempsql += "'";
            }
            tempsql += " ";
            if(i + 1 < stmt->columns->size())
                tempsql += "AND ";
        }

        if(re_parse_sql(tempsql, &ast2) != 0)
            return -1;
        log4cplus_debug("temsql: %s", tempsql.c_str());
        ast = &ast2;
    }

    int ext = is_ext_table(&sql_ast, dbname);
    if(ext == -1)
    {
        return 2;
    }
    else if(ext == -2)
    {
        return -2;
    }
        
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