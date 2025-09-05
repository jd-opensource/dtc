/*
* Copyright JD.com, Inc.
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
	// Statistical item corresponding to statistical item id
	uint64_t ddw_stat_item_id;
	// Alarm threshold corresponding to statistical item
	uint64_t ddw_threshold_value;
	// Data basis for statistical item alarm, respectively cur, 10s, 10m, all, default is 10s
	unsigned char cat;
	// Position of alarm item in statistical file
	StatClient::Iterator_ info;
	// Alarm content string
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
	// Statistical process alarm reporting interface, report through threshold and alarm content in configuration file
	void report_alarm();
	// Interface for other processes to report alarms, directly report alarm content
	void report_alarm(const std::string &str_alarm_content);
	bool set_stat_client(StatClient *stat_client);
	void set_time_out(int time_out);
	/*
	 *  When initializing configuration, two scenarios need to be considered:
	 *	1. dtcserver, only needs url and cellphonenum, for dtcserver it directly triggers alarms
	 *	2. stattool, needs both url and cellphonenum, and also needs thresholds and alarm content of various statistical items	
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
	uint64_t ddw_module_id_; /*ModuleId corresponding to this service*/
	StatClient *stat_client_;
	std::string cell_phone_list_;
	std::string dtc_ip_;
	int post_time_out_;
	FileLastModifyTime modify_time_;
};
#define ALARM_REPORTER Singleton<StatAlarmReporter>::instance()
#endif