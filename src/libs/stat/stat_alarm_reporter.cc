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
#include "stat_alarm_reporter.h"
#include <unistd.h>
#include <iostream>
#include <stdlib.h>
#include <fstream>
#include "stat_info.h"
#include <string.h>
#include <sstream>
#include "dtcutils.h"
#include "log/log.h"
#include <sys/stat.h>

void StatAlarmReporter::set_time_out(int time_out)
{
	post_time_out_ = time_out;
}

/* 当前运行路径的第四个位置是accesskey，access的前八个字节代表moduleid */
uint64_t
StatAlarmReporter::parse_module_id(const std::string &str_current_work_path)
{
	log4cplus_debug("CurWorkingPath is %s", str_current_work_path.c_str());
	std::vector<std::string> pathVec;
	dtc::utils::split_str(str_current_work_path, '/', pathVec);
	if (pathVec.size() < 5) {
		log4cplus_error("error working path : %s",
				str_current_work_path.c_str());
		return 0;
	}
	std::string strAccessKey = pathVec[4];
	std::string strModuleId = strAccessKey.substr(0, 8);
	return atoi(strModuleId.c_str());
}

bool StatAlarmReporter::set_stat_client(StatClient *stat_client)
{
	if (NULL == stat_client) {
		return false;
	}
	this->stat_client_ = stat_client;
	return true;
}

void StatAlarmReporter::init_module_id()
{
	char buf[1024];
	getcwd(buf, sizeof(buf));
	ddw_module_id_ = parse_module_id(std::string(buf));
}

/* 优先尝试从/usr/local/dtc/ip读取本机的ip，如果该文件没有ip，则在使用GetLocalIp函数获取本机Ip */
void StatAlarmReporter::init_local_id()
{
	std::string strIpFilePath("/usr/local/dtc/ip");
	std::ifstream file;
	file.open(strIpFilePath.c_str(), std::ios::in);
	if (file.fail()) {
		log4cplus_error("open file %s fail", strIpFilePath.c_str());
		dtc_ip_ = dtc::utils::get_local_ip();
		return;
	}
	std::string strLine;
	while (std::getline(file, strLine)) {
		if (!strLine.empty()) {
			dtc_ip_ = strLine;
			log4cplus_debug("dtc ip is %s", dtc_ip_.c_str());
			return;
		}
	}

	dtc_ip_ = dtc::utils::get_local_ip();
}

bool StatAlarmReporter::parse_alarm_cfg(uint64_t ddw_stat_id,
					const std::string &str_cfg_item,
					AlarmCfg &cfg)
{
	cfg.ddw_stat_item_id = ddw_stat_id;
	std::vector<std::string> itemVec;
	dtc::utils::split_str(str_cfg_item, ';', itemVec);
	if (itemVec.size() < 3) {
		log4cplus_error("warning: bad cfg item  : %s",
				str_cfg_item.c_str());
		return false;
	}
	cfg.ddw_threshold_value = atol(itemVec[0].c_str());
	if ("job_operation" == itemVec[1]) {
		cfg.cat = SC_CUR;
	} else if ("10s" == itemVec[1]) {
		cfg.cat = SCC_10S;
	} else if ("10m" == itemVec[1]) {
		cfg.cat = SCC_10M;
	} else if ("all" == itemVec[1]) {
		cfg.cat = SCC_ALL;
	} else {
		log4cplus_error("warning: bad cat cfg : %s",
				itemVec[1].c_str());
		return false;
	}
	cfg.str_alarm_content = itemVec[2];
	log4cplus_debug("str_alarm_content is %s",
			cfg.str_alarm_content.c_str());
	if (NULL == stat_client_) {
		log4cplus_error("warning: stat_client_ is null:");
		return false;
	}
	StatClient &stat_client = *stat_client_;
	StatClient::Iterator_ stat_client_ite =
		stat_client[(unsigned int)ddw_stat_id];
	if (stat_client_ite == NULL) {
		log4cplus_error("warning: stat_client_ite is null");
		return false;
	}
	if (stat_client_ite->is_counter() || stat_client_ite->is_value()) {
		cfg.info = stat_client_ite;
	} else {
		log4cplus_error(
			"warning: cfg[%lu] type is not value or counter",
			ddw_stat_id);
		return false;
	}

	return true;
}

/*
 *
 * 看配置文件是否被修改了，如果被修改需要重新加载配置
 * 如果stat文件失败也认为需要重新加载配置
 *
 */
bool StatAlarmReporter::is_alarm_cfg_file_modify(
	const std::string &str_alarm_conf_file_path)
{
	struct stat st;

	if (stat(str_alarm_conf_file_path.c_str(), &st) != 0) {
		log4cplus_error("warning: stat cfg file faile");
		return true;
	}
	if (st.st_mtime == modify_time_) {
		return false;
	}

	/* 配置文件被修改了，更新文件修改时间，同时打error日志（此类操作较少）记录该文件修改事件 */
	log4cplus_error(
		"alarm cfg file has been modified ,the last modify time is %lu, this modify time is %lu",
		modify_time_, st.st_mtime);
	modify_time_ = st.st_mtime;
	return true;
}

/*
 * 
 * 告警的配置项分为如下三种：
 * 1、上报的url(结束处不加?号)
 *    url=http://192.168.214.62/api/dtc_sms_alarm_sender.php
 * 2、短信通知的手机号码,中间以英文分号分开(结束处加分号)
 *    cellphone=1283930303;1020123123;1212312312;
 * 3、配置的告警项(结束处不加分号)
 *    statItemId=thresholdValue;cat;alarmContent
 *    其中cat分为10s 、job_operation、10m、all，如下以inc0的cpu占用率为例,
 *    20000=8000;10s;inc0 thread cpu overload(80%)
 *    这个配置项的含义就是接入线程cpu使用率的统计值（从10s文件中取值）大小超过了80%，发短信通知cpu超载
 *    所有配置的设置都采用小写英文  
 *
 */
bool StatAlarmReporter::init_alarm_cfg(
	const std::string &str_alarm_conf_file_path, bool is_dtc_server)
{
	/* 如果配置文件没有被修改过，就不再加载配置文件 */
	if (!is_alarm_cfg_file_modify(str_alarm_conf_file_path)) {
		return true;
	}
	std::ifstream file;
	file.open(str_alarm_conf_file_path.c_str(), std::ios::in);
	if (file.fail()) {
		return false;
	}

	alarm_Cfg_.clear();
	std::string strLine;
	while (std::getline(file, strLine)) {
		log4cplus_debug("Cfg Line is %s", strLine.c_str());
		std::vector<std::string> cfgVec;
		dtc::utils::split_str(strLine, '=', cfgVec);
		if (cfgVec.size() < 2) {
			continue;
		}

		std::string strCfgKey = cfgVec[0];
		if ("url" == strCfgKey) {
			str_report_url_ = cfgVec[1];
		} else if ("cellphone" == strCfgKey) {
			cell_phone_list_ = cfgVec[1];
		} else {
			if (is_dtc_server) {
				continue;
			}
			uint64_t ddw_stat_id = atoi(strCfgKey.c_str());
			if (ddw_stat_id > 0) {
				std::string str_cfg_item = cfgVec[1];
				AlarmConf cfg;
				if (parse_alarm_cfg(ddw_stat_id, str_cfg_item,
						    cfg)) {
					log4cplus_debug(
						"push back to alarmcfg, statid %lu ",
						cfg.ddw_stat_item_id);
					alarm_Cfg_.push_back(cfg);
				}
			} else {
				log4cplus_error("warning: bad cfg key :%s",
						strCfgKey.c_str());
			}
		}
	}
	file.close();
	return true;
}

void StatAlarmReporter::do_singe_stat_item_alm_report(
	const AlarmConf &alarm_conf)
{
	if (NULL == stat_client_) {
		log4cplus_error("stat_client_ is null");
		return;
	}

	uint64_t ddwStatValue = stat_client_->read_counter_value(
		alarm_conf.info, alarm_conf.cat);
	log4cplus_debug("RealStatValue is %lu, ThreSholdValue is %lu",
			ddwStatValue, alarm_conf.ddw_threshold_value);

	if (ddwStatValue < alarm_conf.ddw_threshold_value) {
		log4cplus_debug("RealStatValue le than ThreSholdValue ");
		return;
	}
	log4cplus_debug("post alarm ,realstatvalue %lu", ddwStatValue);
	post_alarm(alarm_conf.str_alarm_content);
}

void StatAlarmReporter::post_alarm(const std::string &str_alarm_content)
{
	return;
}

void StatAlarmReporter::report_alarm(const std::string &str_alarm_content)
{
	post_alarm(str_alarm_content);
}

void StatAlarmReporter::report_alarm()
{
	static int chkpt = 0;
	int curcptpoint = stat_client_->check_point();
	if (curcptpoint == chkpt) {
		log4cplus_debug("curcptpoint[%d] is equal chkptr [%d]",
				curcptpoint, chkpt);
		return;
	}
	chkpt = curcptpoint;

	log4cplus_debug("alarm_Cfg_ size is %ld ", alarm_Cfg_.size());
	for (AlarmCfgInfo::const_iterator iter = alarm_Cfg_.begin();
	     iter != alarm_Cfg_.end(); iter++) {
		do_singe_stat_item_alm_report(*iter);
	}
}
