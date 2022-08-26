#ifndef __DATA_CONF_H__
#define __DATA_CONF_H__

#include "algorithm/singleton.h"
#include <string>
#include <vector>
#include <stdint.h>

struct ConfigParam{
public:
    uint32_t single_query_cnt_;
    std::string data_rule_;
    std::string operate_time_rule_;
    std::string operate_type_;
    std::string key_field_name_;
    std::string table_name_;
    std::string life_cycle_table_name_;
    std::string hot_db_name_;
    std::string cold_db_name_;
    std::vector<std::string> field_vec_;
};

class DataConf{
public:
    DataConf();
    ~DataConf();
    static DataConf *Instance(){
        return Singleton<DataConf>::instance();
    }
    static void Destroy(){
        Singleton<DataConf>::destory();
    }
    int LoadConfig(int argc, char *argv[]);
    int ParseConfig(ConfigParam& config_param);
    bool ParseAgentConf(std::string path);
    uint32_t Port();
private:
    uint32_t port_;
};



#endif
