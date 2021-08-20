/*
* Copyright [2021] JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
* 
*/
#ifndef DTC_STAT_ALARM_REPORTER_H
#define DTC_STAT_ALARM_REPORTER_H
#include <vector>
#include <string>
#include "stat_client.h"
#include "algorithm/singleton.h"

typedef struct AlarmCfg {
	// 统计项对应的统计项id
	uint64_t ddw_stat_item_id;
	// 统计项对应的告警阈值
	uint64_t ddw_threshold_value;
	// 统计项告警时依据的数据，分别为cur，10s、10m、all，默认为10s
	unsigned char cat;
	// 告警项在统计文件中的位置
	StatClient::Iterator_ info;
	// 告警内容字符串
	std::string str_alarm_content;
} AlarmConf;
typedef std::vector<AlarmConf> AlarmCfgInfo;
typedef long FileLastModifyTime;

class StatAlarmReporter {
    public:
	StatAlarmReporter()
	{
		modify_time_ = 0;
		post_time_out_ = 1;
		init_module_id();
		init_local_id();
	}
	~StatAlarmReporter()
	{
	}
	// 统计进程上报告警接口，通过配置文件的阈值和告警内容上报
	void report_alarm();
	// 其他进程上报告警的接口，直接上报告警内容
	void report_alarm(const std::string &str_alarm_content);
	bool set_stat_client(StatClient *stat_client);
	void set_time_out(int time_out);
	/*
	 *  初始化配置的时候需要考虑两种场景：
	 *	1、dtcserver，只需要url和cellphonenum，对于dtcserver来说是直接触发告警的
	 *	2、stattool，即需要url和cellphonenum，又需要各个统计项的阈值 告警内容等	
	 *	
	*/
	bool init_alarm_cfg(const std::string &str_alarm_conf_file_path,
			    bool is_dtc_server = false);

    private:
	void post_alarm(const std::string &str_alarm_content);
	void init_module_id();
	void init_local_id();
	bool parse_alarm_cfg(uint64_t ddw_stat_id,
			     const std::string &str_cfg_item, AlarmCfg &cfg);
	uint64_t parse_module_id(const std::string &str_current_work_path);
	void do_singe_stat_item_alm_report(const AlarmConf &alarm_conf);
	bool
	is_alarm_cfg_file_modify(const std::string &str_alarm_conf_file_path);

    private:
	std::string str_report_url_;
	AlarmCfgInfo alarm_Cfg_;
	uint64_t ddw_module_id_; /*本业务对应的ModuleId*/
	StatClient *stat_client_;
	std::string cell_phone_list_;
	std::string dtc_ip_;
	int post_time_out_;
	FileLastModifyTime modify_time_;
};
#define ALARM_REPORTER Singleton<StatAlarmReporter>::instance()
#endif