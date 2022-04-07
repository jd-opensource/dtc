#include <stdio.h>
#include <iostream>
#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include "re_comm.h"
#include "re_load.h"
#include "re_match.h"
#include "re_cache.h"
#include "log.h"

using namespace std;

extern vector<expr_properity> expr_rules;

int main(int argc, char* argv[])
{
    printf("hello dtc, ./bin KEY SQL\n");
    std::string key = argv[1];
    std::string sql = argv[2];
    cout<<"key: "<<key<<endl;
    cout<<"sql: "<<sql<<endl;

    init_log4cplus();
    
    int ret = re_load_rule();
    if(ret != 0)
    {
        log4cplus_error("load rule error:%d", ret);
        return 0;
    }

    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(sql, &sql_ast) != 0)
        return -1;

    ret = re_match_sql(&sql_ast, expr_rules);
    if(ret == 0)
    {
        if(re_is_cache_sql(&sql_ast, key))
        {
            printf("RULE MATCH : L1 - cache data\n");
        }
        else
        {
            printf("RULE MATCH : L2 - hot data\n");
        }
    }
    else {
        printf("RULE MATCH : L3 - full data\n");
    }

    return 0;
}