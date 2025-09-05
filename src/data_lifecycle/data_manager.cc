#include "data_manager.h"
#include "global.h"
#include "data_conf.h"
#include "croncpp.h"
#include <unistd.h>
#include <set>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>

#define default_option_file "../conf/my.conf"

DataManager::DataManager(){
    next_process_time_ = 0;
    DBHost* db_host = new DBHost();
    memset(db_host, 0, sizeof(db_host));
    strcpy(db_host->Host, "127.0.0.1");
    db_host->Port = 3306;
    strcpy(db_host->User, "");
    strcpy(db_host->Password, "");
    db_host->ReadTimeout = 1;
    db_host->WriteTimeout = 1;
    db_conn_ = new CDBConn(db_host);
    if(NULL != db_host){
        delete db_host;
    }
}

DataManager::DataManager(const ConfigParam& config_param):
data_rule_(config_param.data_rule_),
operate_time_rule_(config_param.operate_time_rule_),
single_query_cnt_(config_param.single_query_cnt_),
table_name_(config_param.table_name_),
key_field_name_(config_param.key_field_name_),
life_cycle_table_name_(config_param.life_cycle_table_name_),
hot_db_name_(config_param.hot_db_name_),
cold_db_name_(config_param.cold_db_name_),
field_vec_(config_param.field_vec_),
field_flag_vec_(config_param.field_flag_vec_){
    next_process_time_ = 0;
    DBHost* db_host = new DBHost();
    memset(db_host, 0, sizeof(db_host));
    strcpy(db_host->Host, "127.0.0.1");
    db_host->Port = config_param.port_;
    strcpy(db_host->User, "root");
    strcpy(db_host->Password, "root");
    db_host->ReadTimeout = 1;
    db_host->WriteTimeout = 1;
    db_conn_ = new CDBConn(db_host);
    if(NULL != db_host){
        delete db_host;
    }

    std::vector<std::string> full_db_vec = splitVecStr(config_param.full_db_addr_, ":");
    if(full_db_vec.size() == 2){
        DBHost* full_db_host = new DBHost();
        memset(full_db_host, 0, sizeof(full_db_host));
        strcpy(full_db_host->Host, full_db_vec[0].c_str());
        full_db_host->Port = stoi(full_db_vec[1]);
        strcpy(full_db_host->User, config_param.full_db_user_.c_str());
        strcpy(full_db_host->Password, config_param.full_db_pwd_.c_str());
        strcpy(full_db_host->OptionFile, "");
        if(config_param.option_file.size() != 0){
            strcpy(full_db_host->OptionFile, config_param.option_file.c_str());
        } else {
            strcpy(full_db_host->OptionFile, default_option_file);
        }
        printf("full_db_host->OptionFile: %s\n", full_db_host->OptionFile);
        full_db_conn_ = new CDBConn(full_db_host);
        if(NULL != full_db_host){
            delete full_db_host;
        }
    }
}

DataManager::~DataManager(){
    if(NULL != db_conn_){
        delete db_conn_;
    }
    if(NULL != full_db_conn_){
        delete full_db_conn_;
    }
}

static std::string GetIp(){
    struct ifaddrs * ifAddrStruct = NULL;
    struct ifaddrs * ifAddrStruct1 = NULL;
    void * tmpAddrPtr = NULL;

    getifaddrs(&ifAddrStruct);
    ifAddrStruct1 = ifAddrStruct;
    std::string my_ip;

    while (ifAddrStruct != NULL)
    {
        if (ifAddrStruct->ifa_addr->sa_family == AF_INET) {
           tmpAddrPtr = &((struct sockaddr_in*)ifAddrStruct->ifa_addr)->sin_addr;
           char addressBuffer[INET_ADDRSTRLEN];
           inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
           if(strcmp(ifAddrStruct->ifa_name, "eth0") == 0){
               my_ip = addressBuffer;
           }
        }
        ifAddrStruct=ifAddrStruct->ifa_next;
    }
    freeifaddrs(ifAddrStruct1);
    return my_ip;
}

int DataManager::ConnectAgent(){
    return db_conn_->Open();
}

int DataManager::ConnectFullDB(){
    return full_db_conn_->Open();
}

int DataManager::DoProcess(){
    auto cron = cron::make_cron(operate_time_rule_);
    try{
        std::time_t now = std::time(0);
        next_process_time_ = cron::cron_next(cron, now);
        log4cplus_debug("now: %d, next_process_time_: v%d", now, next_process_time_);
    }
    catch (cron::bad_cronexpr const & ex){
        log4cplus_error("bad_cronexpr: %s", ex.what());
        return -1;
    }
    while(!stop){
        sleep(1);
        if (stop){
            break;
        }
        std::time_t now = std::time(0);
        if(now >= next_process_time_){
            DoTaskOnce();
            try{
                std::time_t now = std::time(0);
                next_process_time_ = cron::cron_next(cron, now);
                log4cplus_debug("now: %d, next_process_time_: v%d", now, next_process_time_);
            }
            catch (cron::bad_cronexpr const & ex){
                log4cplus_error("bad_cronexpr: %s", ex.what());
            }
        }
    }
    return 0;
}

int DataManager::DoTaskOnce(){
    while(true){
        uint64_t last_delete_id = 0;
        std::string last_invisible_time;
        int ret = GetLastId(last_delete_id, last_invisible_time);
        if(0 != ret){
            printf("GetLastId error, ret: %d\n", ret);
            return DTC_CODE_MYSQL_QRY_ERR;
        }
        if("" == last_invisible_time){
            last_invisible_time = "1970-01-01 08:00:00";
        }
        std::string query_sql = ConstructQuerySql(last_delete_id, last_invisible_time);
        std::vector<QueryInfo> query_info_vec;
        //full_db_conn_->do_query(cold_db_name_.c_str(), "set names utf8");
        ret = DoQuery(query_sql, query_info_vec);
        if(0 != ret){
            printf("DoQuery error, ret: %d\n", ret);
            return DTC_CODE_MYSQL_QRY_ERR;
        }
        printf("query_info_vec.size: %d\n", (int)query_info_vec.size());
        if(query_info_vec.size() == 0){
            printf("query result empty, end the procedure.\n");
            break;
        }
        for(auto iter = query_info_vec.begin(); iter != query_info_vec.end(); iter++){
            // If execution fails, update last_id and exit the loop
            std::string sql_set = ConstructDeleteSql(iter->field_info);
            ret = DoDelete(sql_set);
            log4cplus_debug("DoDelete ret: %d\n", ret);
            last_delete_id_ = iter->id;
            last_invisible_time_ = iter->invisible_time;
            if(0 != ret){
                //UpdateLastDeleteId();
                log4cplus_debug("DoDelete error, ret: %d\n", ret);
                return DTC_CODE_MYSQL_DEL_ERR;
            }
        }
        UpdateLastDeleteId();
        sleep(1);
    }
    return 0;
}

void DataManager::SetTimeRule(const std::string& time_rule){
    operate_time_rule_ = time_rule;
}

int DataManager::GetLastId(uint64_t& last_delete_id, std::string& last_invisible_time){
    std::stringstream ss_sql;
    ss_sql << "select id,ip,last_id,last_update_time from " << life_cycle_table_name_
            << " where uniq_table_name ='" << table_name_
            << "' order by id desc limit 1";
    log4cplus_debug("query sql: %s", ss_sql.str().c_str());
    int ret = full_db_conn_->do_query(cold_db_name_.c_str(), ss_sql.str().c_str());
    if(0 != ret){
        log4cplus_debug("query error, ret: %d, err msg: %s", ret, full_db_conn_->get_err_msg());
        return ret;
    }
    if(0 == full_db_conn_->use_result()){
        if (0 == full_db_conn_->fetch_row()){
            string ip = full_db_conn_->Row[1];
            last_delete_id = std::stoull(full_db_conn_->Row[2]);
            last_invisible_time = full_db_conn_->Row[3];
        } else {
            full_db_conn_->free_result();
            log4cplus_error("db fetch row error: %s", full_db_conn_->get_err_msg());
            return ret;
        }
        full_db_conn_->free_result();
    }
    return 0;
}

std::string DataManager::ConstructQuerySql(uint64_t last_delete_id, std::string last_invisible_time){
    // example: select id from table_A where status=0 and (invisible_time>6 or (invisible_time=6 and id>6)) order by invisible_time limit 2
    std::stringstream ss_sql;
    ss_sql << "select id,invisible_time,";
    for(int i = 0; i < field_vec_.size(); i++){
        ss_sql << field_vec_[i];
        if(i != field_vec_.size()-1){
            ss_sql << ",";
        }
    }
    ss_sql << " from " << table_name_
        << " where not(" << data_rule_
        << ") and (invisible_time>'" << last_invisible_time
        << "' or (invisible_time='" << last_invisible_time
        << "' and id>" << last_delete_id
        << ")) order by invisible_time,id limit " << single_query_cnt_;
    log4cplus_debug("query sql: %s", ss_sql.str().c_str());
    return ss_sql.str();
}

int DataManager::DoQuery(const std::string& query_sql, std::vector<QueryInfo>& query_info_vec){
    printf("begin DoQuery\n");

    int ret = full_db_conn_->do_query(cold_db_name_.c_str(), query_sql.c_str());
    if(0 != ret){
        printf("query error, ret: %d, err msg: %s\n", ret, full_db_conn_->get_err_msg());
        return ret;
    }
    if(0 == full_db_conn_->use_result()){
        for (int i = 0; i < full_db_conn_->res_num; i++) {
            ret = full_db_conn_->fetch_row();
            if (ret != 0) {
                full_db_conn_->free_result();
                printf("db fetch row error: %s\n", full_db_conn_->get_err_msg());
                return ret;
            }
            QueryInfo query_info;
            query_info.id = std::stoull(full_db_conn_->Row[0]);
            query_info.invisible_time = full_db_conn_->Row[1];
            query_info.key_info = full_db_conn_->Row[2];
            for(int row_idx = 2; row_idx < field_vec_.size() + 2; row_idx++){
                if(full_db_conn_->Row[row_idx] == NULL){
                    if(field_flag_vec_[row_idx-2] == 1){
                        query_info.field_info.push_back("");
                    } else {
                        query_info.field_info.push_back("0");
                    }
                } else {
                    query_info.field_info.push_back(full_db_conn_->Row[row_idx]);
                }
            }
            query_info_vec.push_back(query_info);
        }
        full_db_conn_->free_result();
    }
    return 0;
}

void hextostring(char* str, int len){
    for(int i = 0; i < len; i++){
        printf("%02x", str[i]);
    }
    printf("\n");
}

std::set<std::string> DataManager::ConstructDeleteSql(const std::string& key){
    // Delete based on key with rules applied
    std::set<std::string> sql_set;
    std::string or_flag = " or ";
    std::set<std::string> res = splitStr(data_rule_, or_flag);
    for(auto iter = res.begin(); iter != res.end(); iter++){
        std::stringstream ss_sql;
        ss_sql << "delete from " << table_name_
            << " where " << key_field_name_
            << " = " << key
            << " and " << *iter;
        log4cplus_debug("delete sql: %s", ss_sql.str().c_str());
        sql_set.insert(ss_sql.str());
    }

    return sql_set;
}

std::string DataManager::ConstructDeleteSql(const std::vector<std::string>& key_vec){
    if(field_vec_.size() != key_vec.size() || field_flag_vec_.size() != key_vec.size()){
        log4cplus_debug("field_vec_.size(): %d, key_vec.size(): %d, field_flag_vec_.size(): %d", field_vec_.size(), key_vec.size(), field_flag_vec_.size());
        return "";
    }
    std::stringstream ss_sql;
    ss_sql << "delete from " << table_name_ << " where ";
    for(int i = 0; i < field_vec_.size(); i++){
        if(field_flag_vec_[i] == 1){
            char* esc = new char[key_vec[i].length()*2];
            db_conn_->escape_string(esc, key_vec[i].c_str());
            ss_sql << field_vec_[i] << " = '" << esc << "'";
            delete []esc;
        } else {
            ss_sql << field_vec_[i] << " = " << key_vec[i];
        }
        ss_sql << " and ";
    }
    ss_sql << "WITHOUT@@ = 1";
    log4cplus_debug("delete sql: %s", ss_sql.str().c_str());
    return ss_sql.str();
}

int DataManager::DoDelete(const std::string& delete_sql){
    int ret = db_conn_->do_query(hot_db_name_.c_str(), delete_sql.c_str());
    if(0 != ret){
        log4cplus_debug("DoDelete error, ret: %d, err msg: %s, delete_sql: %s", ret, db_conn_->get_err_msg(), delete_sql.c_str());
        return ret;
    }
    int affected = db_conn_->affected_rows();
    log4cplus_debug("affected row: %d", affected);
    return 0;
}

int DataManager::UpdateLastDeleteId(){
    std::string local_ip = GetIp();
    std::stringstream ss_sql;
    ss_sql << "replace into " << life_cycle_table_name_
        << " values(NULL,'" << local_ip
        << "', '" << table_name_
        << "', " << last_delete_id_
        << ", '" << last_invisible_time_
        << "')";
    int ret = full_db_conn_->do_query(cold_db_name_.c_str(), ss_sql.str().c_str());
    if(0 != ret){
        log4cplus_debug("insert error, ret: %d, err msg: %s", ret, full_db_conn_->get_err_msg());
        return ret;
    }
    return 0;
}

int DataManager::ShowVariables(){
    int ret = full_db_conn_->do_query(cold_db_name_.c_str(), "show variables like '%%char%%'");
    if(0 != ret){
        printf("query error, ret: %d, err msg: %s\n", ret, full_db_conn_->get_err_msg());
        return ret;
    }
    if(0 == full_db_conn_->use_result()){
        for (int i = 0; i < full_db_conn_->res_num; i++) {
            ret = full_db_conn_->fetch_row();
            if (ret != 0) {
                full_db_conn_->free_result();
                printf("db fetch row error: %s\n", full_db_conn_->get_err_msg());
                return ret;
            }
            printf("%s: %s\n", full_db_conn_->Row[0], full_db_conn_->Row[1]);
        }
    }
    return 0;
}

int DataManager::CreateTable(){
    std::stringstream ss_sql;
    ss_sql << "CREATE TABLE if not exists " << life_cycle_table_name_ << "("
        << "`id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        << "`ip` varchar(20) NOT NULL DEFAULT '0' COMMENT 'IP address of the machine performing cleanup operations',"
        << "`uniq_table_name` varchar(40) DEFAULT NULL UNIQUE ,"
        << "`last_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'ID corresponding to the last deleted record',"
        << "`last_update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time corresponding to the last deleted record',"
        << "PRIMARY KEY (`id`)"
        << ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
    int ret = full_db_conn_->do_query(cold_db_name_.c_str(), ss_sql.str().c_str());
    if(0 != ret){
        log4cplus_debug("create table error, ret: %d, err msg: %s", ret, full_db_conn_->get_err_msg());
        return ret;
    }
    return 0;
}

std::set<std::string> DataManager::splitStr(const std::string& src, const std::string& separate_character)
{
    std::set<std::string> strs;

    int separate_characterLen = separate_character.size();
    int last_position = 0, index = -1;
    while (-1 != (index = src.find(separate_character, last_position)))
    {
        if (src.substr(last_position, index - last_position) != " ") {
            strs.insert(src.substr(last_position, index - last_position));
        }
        last_position = index + separate_characterLen;
    }
    string last_string = src.substr(last_position);//Extract content after the last separator
    if (!last_string.empty() && last_string != " ")
        strs.insert(last_string);//If there's content after the last separator, add to queue
    return strs;
}

std::vector<std::string> DataManager::splitVecStr(const std::string& src, const std::string& separate_character)
{
    std::vector<std::string> strs;

    int separate_characterLen = separate_character.size();
    int last_position = 0, index = -1;
    while (-1 != (index = src.find(separate_character, last_position)))
    {
        if (src.substr(last_position, index - last_position) != " ") {
            strs.push_back(src.substr(last_position, index - last_position));
        }
        last_position = index + separate_characterLen;
    }
    string last_string = src.substr(last_position);//Extract content after the last separator
    if (!last_string.empty() && last_string != " ")
        strs.push_back(last_string);//If there's content after the last separator, add to queue
    return strs;
}
