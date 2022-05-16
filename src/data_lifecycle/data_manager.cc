#include "data_manager.h"
#include "global.h"
#include "data_conf.h"
#include "croncpp.h"
#include <unistd.h>
#include <set>

DataManager::DataManager(){
    next_process_time_ = 0;
    DBHost* db_host = new DBHost();
    memset(db_host, 0, sizeof(db_host));
    strcpy(db_host->Host, "127.0.0.1");
    db_host->Port = DataConf::Instance()->Port();
    strcpy(db_host->User, "");
    strcpy(db_host->Password, "");
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
cold_db_name_(config_param.cold_db_name_){
    next_process_time_ = 0;
    DBHost* db_host = new DBHost();
    memset(db_host, 0, sizeof(db_host));
    strcpy(db_host->Host, "127.0.0.1");
    db_host->Port = DataConf::Instance()->Port();
    strcpy(db_host->User, "root");
    strcpy(db_host->Password, "root");
    db_conn_ = new CDBConn(db_host);
    if(NULL != db_host){
        delete db_host;
    }
}

DataManager::~DataManager(){
    if(NULL != db_conn_){
        delete db_conn_;
    }
}

int DataManager::ConnectAgent(){
    return db_conn_->Open();
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
        std::string query_sql = ConstructQuerySql(last_delete_id, last_invisible_time);
        std::vector<QueryInfo> query_info_vec;
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
            // 如果执行失败，更新last_id，并退出循环
            // 如果清除规则有or，delete语句需要拆分成多个语句
            std::set<std::string> sql_set = ConstructDeleteSql(iter->key_info);
            bool success_flag = true;
            for(auto del_iter = sql_set.begin(); del_iter != sql_set.end(); del_iter++){
                ret = DoDelete(*del_iter);
                printf("DoDelete ret: %d\n", ret);
                if(0 != ret){
                    success_flag = false;
                }
            }
            last_delete_id_ = iter->id;
            last_invisible_time_ = iter->invisible_time;
            if(false == success_flag){
                UpdateLastDeleteId();
                printf("DoDelete error, ret: %d\n", ret);
                return DTC_CODE_MYSQL_DEL_ERR;
            }
        }
        UpdateLastDeleteId();
    }
    return 0;
}

void DataManager::SetTimeRule(const std::string& time_rule){
    operate_time_rule_ = time_rule;
}

int DataManager::GetLastId(uint64_t& last_delete_id, std::string& last_invisible_time){
    std::stringstream ss_sql;
    ss_sql << "select id,ip,last_id,last_update_time from " << life_cycle_table_name_
            << " order by id desc limit 1";
    int ret = db_conn_->do_query(cold_db_name_.c_str(), ss_sql.str().c_str());
    if(0 != ret){
        log4cplus_debug("query error, ret: %d, err msg: %s", ret, db_conn_->get_err_msg());
        return ret;
    }
    if(0 == db_conn_->use_result()){
        if (0 == db_conn_->fetch_row()){
            string ip = db_conn_->Row[1];
            last_delete_id = std::stoull(db_conn_->Row[2]);
            last_invisible_time = db_conn_->Row[3];
        } else {
            db_conn_->free_result();
            log4cplus_error("db fetch row error: %s", db_conn_->get_err_msg());
            return ret;
        }
        db_conn_->free_result();
    }
    return 0;
}

std::string DataManager::ConstructQuerySql(uint64_t last_delete_id, std::string last_invisible_time){
    // example: select id from table_A where status=0 and (invisible_time>6 or (invisible_time=6 and id>6)) order by invisible_time limit 2
    std::stringstream ss_sql;
    ss_sql << "select id,invisible_time," << key_field_name_ 
        << " from " << table_name_
        << " where (" << data_rule_
        << ") and (invisible_time>'" << last_invisible_time
        << "' or (invisible_time='" << last_invisible_time
        << "' and id>" << last_delete_id
        << ")) order by invisible_time,id limit " << single_query_cnt_;
    log4cplus_debug("query sql: %s", ss_sql.str().c_str());
    return ss_sql.str();
}

int DataManager::DoQuery(const std::string& query_sql, std::vector<QueryInfo>& query_info_vec){
    int ret = db_conn_->do_query(cold_db_name_.c_str(), query_sql.c_str());
    if(0 != ret){
        printf("query error, ret: %d, err msg: %s\n", ret, db_conn_->get_err_msg());
        return ret;
    }
    if(0 == db_conn_->use_result()){
        for (int i = 0; i < db_conn_->res_num; i++) {
            ret = db_conn_->fetch_row();
            if (ret != 0) {
                db_conn_->free_result();
                printf("db fetch row error: %s\n", db_conn_->get_err_msg());
                return ret;
            }
            QueryInfo query_info;
            query_info.id = std::stoull(db_conn_->Row[0]);
            query_info.invisible_time = db_conn_->Row[1];
            query_info.key_info = db_conn_->Row[2];
            query_info_vec.push_back(query_info);
        }
        db_conn_->free_result();
    }
    return 0;
}

std::set<std::string> DataManager::ConstructDeleteSql(const std::string& key){
    // delete根据key删除，并带上规则
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

int DataManager::DoDelete(const std::string& delete_sql){
    int ret = db_conn_->do_query(hot_db_name_.c_str(), delete_sql.c_str());
    if(0 != ret){
        log4cplus_debug("DoDelete error, ret: %d, err msg: %s", ret, db_conn_->get_err_msg());
        return ret;
    }
    return 0;
}

int DataManager::UpdateLastDeleteId(){
    std::string local_ip;
    std::stringstream ss_sql;
    ss_sql << "insert into " << life_cycle_table_name_
        << " values(NULL,'" << local_ip
        << "', " << last_delete_id_
        << ", '" << last_invisible_time_
        << "')";
    int ret = db_conn_->do_query(cold_db_name_.c_str(), ss_sql.str().c_str());
    if(0 != ret){
        log4cplus_debug("insert error, ret: %d, err msg: %s", ret, db_conn_->get_err_msg());
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
    string last_string = src.substr(last_position);//截取最后一个分隔符后的内容
    if (!last_string.empty() && last_string != " ")
        strs.insert(last_string);//如果最后一个分隔符后还有内容就入队
    return strs;
}
