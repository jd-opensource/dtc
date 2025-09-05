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
*/

#include <system_command_ask_chain.h>
#include <log/log.h>
#include "protocol.h"

SystemCommandAskChain *SystemCommandAskChain::system_command_instance = NULL;

SystemCommandAskChain::SystemCommandAskChain(PollerBase *o)
	: JobAskInterface<DTCJobOperation>(o), main_chain(o)
{
	atomic8_set(&read_only_, 0);
	stat_read_only = g_stat_mgr.get_stat_int_counter(SERVER_READONLY);
	stat_read_only.set((0 == atomic8_read(&read_only_)) ? 0 : 1);
}

SystemCommandAskChain::~SystemCommandAskChain(void)
{
}

SystemCommandAskChain *SystemCommandAskChain::get_instance(PollerBase *o)
{
	if (NULL == system_command_instance) {
		NEW(SystemCommandAskChain(o), system_command_instance);
	}
	return system_command_instance;
}

SystemCommandAskChain *SystemCommandAskChain::get_instance()
{
	return system_command_instance;
}

bool SystemCommandAskChain::is_read_only()
{
	return 0 != atomic8_read(&read_only_);
}
void SystemCommandAskChain::query_mem_info(DTCJobOperation *job_operation)
{
	struct DTCServerInfo s_info;
	memset(&s_info, 0x00, sizeof(s_info));

	s_info.version = 0x1;
	s_info.datasize = g_stat_mgr.get_interval_10s_stat_value(DTC_DATA_SIZE);
	s_info.memsize = g_stat_mgr.get_interval_10s_stat_value(DTC_CACHE_SIZE);
	log4cplus_debug("Memory info is: memsize is %lu , datasize is %lu",
			s_info.memsize, s_info.datasize);
	job_operation->resultInfo.set_server_info(&s_info);
}
void SystemCommandAskChain::deal_server_admin(DTCJobOperation *job_operation)
{
	switch (job_operation->requestInfo.admin_code()) {
	case DRequest::SystemCommand::SET_READONLY: {
		atomic8_set(&read_only_, 1);
		stat_read_only.set(1);
		log4cplus_info("set server status to readonly.");
		break;
	}
	case DRequest::SystemCommand::SET_READWRITE: {
		atomic8_set(&read_only_, 0);
		stat_read_only.set(0);
		log4cplus_info("set server status to read/write.");
		break;
	}
	case DRequest::SystemCommand::QUERY_MEM_INFO: {
		log4cplus_debug("query meminfo.");
		query_mem_info(job_operation);
		break;
	}

	default: {
		log4cplus_debug("unknow cmd: %d",
				job_operation->requestInfo.admin_code());
		job_operation->set_error(-EC_REQUEST_ABORTED, "RequestControl",
					 "Unknown svrAdmin command.");
		break;
	}
	}

	job_operation->turn_around_job_answer();
}

void SystemCommandAskChain::job_ask_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("enter job_ask_procedure");
	log4cplus_debug("Cmd is %d, AdminCmd is %u",
			job_operation->request_code(),
			job_operation->requestInfo.admin_code());
	//Handle ServerAdmin commands
	if (DRequest::TYPE_SYSTEM_COMMAND == job_operation->request_code()) {
		switch (job_operation->requestInfo.admin_code()) {
		case DRequest::SystemCommand::SET_READONLY:
		case DRequest::SystemCommand::SET_READWRITE:
		case DRequest::SystemCommand::QUERY_MEM_INFO:
			deal_server_admin(job_operation);
			return;

			//allow all admin_code pass
		default: {
			log4cplus_debug(
				"job_ask_procedure admincmd,  next process ");

			main_chain.job_ask_procedure(job_operation);
			return;
		}
		}
	}

	//When server is readonly, directly return error for non-query requests
	if (0 != atomic8_read(&read_only_)) {
		if (DRequest::Get != job_operation->request_code()) {
			log4cplus_info(
				"server is readonly, reject write operation");
			job_operation->set_error(-EC_SERVER_READONLY,
						 "RequestControl",
						 "Server is readonly.");
			job_operation->turn_around_job_answer();
			return;
		}
	}

	main_chain.job_ask_procedure(job_operation);
	log4cplus_debug("leave job_ask_procedure");
}
