#include <stdio.h>
#include <iostream>
#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include "re_comm.h"
#include "re_load.h"
#include "re_match.h"
#include "re_cache.h"
#include "log.h"
#include "rule.h"

using namespace std;

int main(int argc, char* argv[])
{
    printf("hello dtc, ./bin KEY SQL\n");
    std::string key = "";
    std::string sql = argv[2];
    char szkey[50] = {0};

    cout<<"sql: "<<sql<<endl;

    hsql::SQLParserResult sql_ast;
    if(re_parse_sql(sql, &sql_ast) != 0)
    {
        cout<<"parsing failed."<<endl;
        return -1;
    }
    
    cout<<"parsing success."<<sql_ast.isValid()<<endl;

    return 0;
}
