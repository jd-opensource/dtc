#include "data_conf.h"
#include "mxml.h"
#include "log/log.h"
#include "dtc_global.h"
#include "config.h"
#include "global.h"
#include "daemon.h"
#include "dbconfig.h"

extern DTCConfig *g_dtc_config;
extern DbConfig *dbConfig;
extern char cache_file[256];
char agent_file[256] = "/etc/dtc/agent.xml";

DataConf::DataConf(){
}

DataConf::~DataConf(){
}

bool DataConf::ParseAgentConf(std::string path){
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
    mxml_node_t *poolnode = mxmlFindElement(tree, tree, "MODULE", NULL, NULL, MXML_DESCEND);
    char* c_listen_on = (char *) mxmlElementGetAttr(poolnode, "ListenOn");
    if (c_listen_on == NULL) {
        log4cplus_error("get ListenOn from conf '%s' error", path.c_str());
        mxmlDelete(tree);
        return false;
    }
    std::string listen_on = c_listen_on;
    mxmlDelete(tree);
    std::string::size_type pos = listen_on.find(":");
    if(pos == std::string::npos){
        log4cplus_error("string find error, file: %s", path.c_str());
        return false;
    }
    std::string port = listen_on.substr(pos+1);
    port_ = std::stoul(port);
    return true;
}

uint32_t DataConf::Port(){
    return port_;
}

int DataConf::LoadConfig(int argc, char *argv[]){
    int c;
    strcpy(table_file, "/etc/dtc/table.yaml");
    strcpy(cache_file, "/etc/dtc/dtc.yaml");

    while ((c = getopt(argc, argv, "df:t:hvV")) != -1) {
        switch (c) {
        case 'd':
            background = 0;
            break;
        case 'f':
            strncpy(cache_file, optarg, sizeof(cache_file) - 1);
            break;
        case 't':
            strncpy(table_file, optarg, sizeof(table_file) - 1);
            break;
        case 'a':
            strncpy(agent_file, optarg, sizeof(agent_file) - 1);
            break;
        case 'h':
            show_usage();
            return 0;
        case '?':
            show_usage();
            return -1;
        }
    }

    g_dtc_config = new DTCConfig;
    if (0 != g_dtc_config->parse_config(cache_file, "data_lifecycle", false)){
        log4cplus_error("parse_config error.");
        return DTC_CODE_LOAD_CONFIG_ERR;
    }
    if (0 != g_dtc_config->parse_config(table_file, "DATABASE_CONF", false)){
        log4cplus_error("parse_config error.");
        return DTC_CODE_LOAD_CONFIG_ERR;
    }
    if(false == ParseAgentConf(agent_file)){
        log4cplus_error("DataConf ParseConf error.");
        return DTC_CODE_LOAD_CONFIG_ERR;
    }
    dbConfig = new DbConfig();
    return 0;
}

int DataConf::ParseConfig(ConfigParam& config_param){
    config_param.single_query_cnt_ = g_dtc_config->get_int_val("data_lifecycle", "SingleQueryCount", 10);
    const char* data_rule = g_dtc_config->get_str_val("data_lifecycle", "DataSQLRule");
    if(NULL == data_rule){
        log4cplus_error("data_rule not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.data_rule_ = data_rule;

    const char* operate_time_rule = g_dtc_config->get_str_val("data_lifecycle", "OperateTimeRule");
    if(NULL == operate_time_rule){
        operate_time_rule = "00 01 * * * ?";
    }
    config_param.operate_time_rule_ = operate_time_rule;

    // 规则对应的操作operate_type  delete或update
    const char* operate_type = g_dtc_config->get_str_val("data_lifecycle", "OperateType");
    if(NULL == operate_type){
        operate_type = "delete";
    }
    config_param.operate_type_ = operate_type;

    const char* life_cycle_table_name = g_dtc_config->get_str_val("data_lifecycle", "LifeCycleTableName");
    if(NULL == life_cycle_table_name){
        life_cycle_table_name = "data_lifecycle_table";
    }
    config_param.life_cycle_table_name_ = life_cycle_table_name;

     const char* hot_db_name = g_dtc_config->get_str_val("data_lifecycle", "HotDBName");
    if(NULL == hot_db_name){
        hot_db_name = "L2";
    }
    config_param.hot_db_name_ = hot_db_name;

     const char* cold_db_name = g_dtc_config->get_str_val("data_lifecycle", "ColdDBName");
    if(NULL == cold_db_name){
        cold_db_name = "L3";
    }
    config_param.cold_db_name_ = cold_db_name;

    const char* key_field_name = g_dtc_config->get_str_val("FIELD1", "field_name");
    if(NULL == key_field_name){
        log4cplus_error("key_field_name not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.key_field_name_ = key_field_name;

    const char* table_name = g_dtc_config->get_str_val("HOT_TABLE_CONF", "table_name");
    if(NULL == table_name){
        log4cplus_error("table_name not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.table_name_ = table_name;

    log4cplus_debug("single_query_cnt_: %d, data_rule: %s, operate_time_rule: %s, operate_type: %s, "
        "life_cycle_table_name: %s, key_field_name: %s, table_name: %s, hot_database_name: %s",
        config_param.single_query_cnt_, config_param.data_rule_.c_str(), config_param.operate_time_rule_.c_str(),
        config_param.operate_type_.c_str(), config_param.life_cycle_table_name_.c_str(), config_param.key_field_name_.c_str(),
        config_param.table_name_.c_str(), config_param.hot_db_name_.c_str());
    return 0;
}
