#ifndef __DATA_MANAGER_H__
#define __DATA_MANAGER_H__

#include <string>
#include <stdint.h>
#include <chrono>
#include <vector>
#include <set>
#include "database_connection.h"
#include "data_conf.h"

class QueryInfo
{
public:
    uint64_t id;
    std::string key_info;
    std::string invisible_time;
    std::vector<std::string> field_info;
};

class DataManager
{
public:
    DataManager();
    DataManager(const ConfigParam& config_param);
    virtual ~DataManager();
    int ConnectAgent();
    int ConnectFullDB();
    int DoProcess();
    int DoTaskOnce();
    void SetTimeRule(const std::string& time_rule);
    void SetDataRule(const std::string& data_rule){
    data_rule_ = data_rule;
    }
    virtual int GetLastId(uint64_t& last_delete_id, std::string& last_invisible_time);
    std::string ConstructQuerySql(uint64_t last_delete_id, std::string last_invisible_time);
    virtual int DoQuery(const std::string& query_sql, std::vector<QueryInfo>& query_info_vec);
    std::set<std::string> ConstructDeleteSql(const std::string& key);
    std::string ConstructDeleteSql(const std::vector<std::string>& key_vec);
    virtual int DoDelete(const std::string& delete_sql);
    virtual int UpdateLastDeleteId();
    std::set<std::string> splitStr(const std::string& src, const std::string& separate_character);
    std::vector<std::string> splitVecStr(const std::string& src, const std::string& separate_character);
private:
    std::string data_rule_; // example: status=0
    std::string operate_time_rule_; // example: 0 */5 * * * ?
    uint32_t single_query_cnt_;
    std::string table_name_;
    std::string key_field_name_;
    std::string life_cycle_table_name_;
    std::string hot_db_name_;
    std::string cold_db_name_;
    std::time_t next_process_time_;
    CDBConn* db_conn_;
    CDBConn* full_db_conn_;
    uint64_t last_delete_id_;
    std::string last_invisible_time_;
    std::vector<std::string> field_vec_;
};


#endif
