#include <stdio.h>
#include <iostream>
#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include "re_comm.h"
#include "re_load.h"
#include "re_match.h"
#include "re_cache.h"

using namespace std;

extern vector<expr_properity> expr_rules;

int main(int argc, char* argv[])
{
    printf("hello dtc, ./bin KEY SQL\n");
    std::string key = argv[1];
    std::string sql = argv[2];
    cout<<key<<endl;
    cout<<sql<<endl;
    
    re_load_rule();

    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(sql, &sql_ast) != 0)
        return -1;

    int ret = 0;
    ret = re_match_sql(sql, expr_rules);
    if(ret == 0)
    {
        if(re_is_cache_sql(&sql_ast, key))
        {
            printf("L1 - cache data\n");
        }
        else
        {
            printf("L2 - hot data\n");
        }
    }
    else{
        if(ret == -1)
        {
            printf("parse sql failed.\n");
        }
        else if(ret == -2)
        {
            printf("L3 - full data\n");
        }
    }
    


    return 0;
}