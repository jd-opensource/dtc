#include "data_conf.h"
#include "mxml.h"
#include "log/log.h"
#include "dtc_global.h"
#include "config.h"
#include "global.h"
#include "daemon.h"
#include "dbconfig.h"

extern DbConfig *dbConfig;
extern char cache_file[256];
char agent_file[256] = "../conf/agent.xml";

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

int DataConf::LoadConfig(const std::string& config_path){
    int c;
    strcpy(table_file, config_path.c_str());
    strcpy(cache_file, config_path.c_str());

    YAML::Node config;
    try {
        config = YAML::LoadFile(config_path);
    } catch (const YAML::Exception &e) {
        log4cplus_error("config file error:%s, %s\n", e.what(), config_path.c_str());
        return DTC_CODE_LOAD_CONFIG_ERR;
    }

    if(!config["data_lifecycle"]) {
        log4cplus_error("parse_config error.");
        return DTC_CODE_LOAD_CONFIG_ERR;
    }
    if(!config["primary"]) {
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

int DataConf::ParseConfig(const std::string& config_path, ConfigParam& config_param){
    YAML::Node config;
    try {
        config = YAML::LoadFile(config_path);
    } catch (const YAML::Exception &e) {
        log4cplus_error("config file error:%s, %s\n", e.what(), config_path.c_str());
        return DTC_CODE_LOAD_CONFIG_ERR;
    }

    YAML::Node node = config["data_lifecycle"]["single.query.count"];
    config_param.single_query_cnt_ = node? node.as<int>(): 10;    
    
    //node = config["data_lifecycle"]["rule.sql"];
    node = config["primary"]["layered.rule"];
    if(!node){
        log4cplus_error("rule.sql not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.data_rule_ = node.as<string>();

    node = config["data_lifecycle"]["rule.cron"];
    config_param.operate_time_rule_ = node? node.as<string>(): "00 01 * * * ?";

    // Operation type corresponding to the rule: delete or update
    node = config["data_lifecycle"]["type.operate"];
    config_param.operate_type_ = node? node.as<string>(): "delete";

    node = config["data_lifecycle"]["lifecycle.tablename"];
    config_param.life_cycle_table_name_ = node? node.as<string>(): "data_lifecycle_table";

    node = config["primary"]["hot"]["logic"]["db"];
    config_param.hot_db_name_ = node? node.as<string>(): "L2";

    if(config["primary"]["full"]["real"].size() == 0){
        log4cplus_error("full real db not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }

    node = config["primary"]["full"]["real"][0]["db"];
    config_param.cold_db_name_ = node? node.as<string>(): "L3";

    node = config["primary"]["full"]["real"][0]["addr"];
    if(!node){
        log4cplus_error("full db addr not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.full_db_addr_ = node.as<string>();

    node = config["primary"]["full"]["real"][0]["user"];
    if(!node){
        log4cplus_error("full db user not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.full_db_user_ = node.as<string>();

    node = config["primary"]["full"]["real"][0]["pwd"];
    if(!node){
        log4cplus_error("full db pwd not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.full_db_pwd_ = node.as<string>();

    node = config["primary"]["cache"]["field"][0]["name"];
    if(!node){
        log4cplus_error("key_field_name not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.key_field_name_ = node.as<string>();

    int field_size = config["primary"]["cache"]["field"].size();
    if(field_size <= 0){
        log4cplus_error("parse field name error.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }

    for(int i = 0; i < field_size; i++){
        node = config["primary"]["cache"]["field"][i]["name"];
        if(!node){
            log4cplus_error("field_name not defined.");
            return DTC_CODE_PARSE_CONFIG_ERR;
        }
        config_param.field_vec_.push_back(node.as<string>());
        node = config["primary"]["cache"]["field"][i]["type"];
        if(!node){
            log4cplus_error("field_type not defined.");
            return DTC_CODE_PARSE_CONFIG_ERR;
        }
        int flag = (node.as<string>() == "string" || node.as<string>() == "binary") ? 1 : 0;
        config_param.field_flag_vec_.push_back(flag);
    }

    node = config["primary"]["hot"]["logic"]["table"];
    if(!node){
        log4cplus_error("table_name not defined.");
        return DTC_CODE_PARSE_CONFIG_ERR;
    }
    config_param.table_name_ = node.as<string>();
    config_param.port_ = port_;

    node = config["primary"]["option.file.path"];
    config_param.option_file = node? node.as<string>(): "";

    log4cplus_debug("single_query_cnt_: %d, data_rule: %s, operate_time_rule: %s, operate_type: %s, "
        "life_cycle_table_name: %s, key_field_name: %s, table_name: %s, hot_database_name: %s, cold_database_name: %s",
        config_param.single_query_cnt_, config_param.data_rule_.c_str(), config_param.operate_time_rule_.c_str(),
        config_param.operate_type_.c_str(), config_param.life_cycle_table_name_.c_str(), config_param.key_field_name_.c_str(),
        config_param.table_name_.c_str(), config_param.hot_db_name_.c_str(), config_param.cold_db_name_.c_str());
    return 0;
}
