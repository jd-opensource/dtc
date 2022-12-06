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
    std::string sql = "DELETE FROM `wx_msg_push_log_4` WHERE `pin`='jd_zpTHwVohmqAk' AND `wxid`='6112817552' AND `send_time`=1670221175 AND `content`='{\"keyword3\":{\"color\":\"#333333\",\"value\":\"陕西西安市雁塔区曲江街道金花南路旺座曲江E座1504\"},\"keyword4\":{\"color\":\"#333333\",\"value\":\"256549187771\"},\"keyword1\":{\"color\":\"#333333\",\"value\":\"￥176.80元\"},\"keyword2\":{\"color\":\"#333333\",\"value\":\"得力(deli)120*80mm金属方形中号秒干印台印泥 办公用品 红色9892\"},\"remark\":{\"color\":\"#e4393c\",\"value\":\"※恭喜！限时抽1000元E卡！回复“1”参与\"},\"first\":{\"color\":\"#263684\",\"value\":\"尊敬的京东用户，您的京东订单已经成功付款啦~正在秒速安排发货，尽心尽力，不愿让你等快递。\n\"}}' AND `ptag`='17005.9.185' AND `biz`='10002' AND `code`=43004";
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
