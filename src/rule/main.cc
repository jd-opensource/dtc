#include <stdio.h>
#include <iostream>
#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"

using namespace std;

int main(int argc, char* argv[])
{
    printf("hello dtc\n");
    cout<<argv[1]<<endl;
    
    re_load_rule();
    int ret = 0;
    ret = re_match_sql();
    if(ret == 0)
    {
        if(re_is_cache_sql())
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