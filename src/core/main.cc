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
* Author:   Linjinming, linjinming@jd.com
			Qiulu, choulu@jd.com
			Yangshuang, yangshuang68@jd.com
			Zhulin, shzhulin3@jd.com
			Chenyujie, chenyujie28@jd.com
		   	Wuxinzhen, wuxinzhen1@jd.com
			Caopei, caopei11@jd.com
*/

#include "main_supply.h"
#include "../misc/dtc_code.h"

using namespace ClusterConfig;

const char project_name[] = "core";
const char usage_argv[] = "";

BufferProcessAskChain *g_buffer_process_ask_instance = NULL;
BarrierAskAnswerChain *g_buffer_barrier_instance = NULL;
BufferBypassAskChain *g_buffer_bypass_ask_instance = NULL;
BarrierAskAnswerChain *g_connector_barrier_instance = NULL;
SystemCommandAskChain *g_system_command_ask_instance = NULL;
DataConnectorAskChain *g_data_connector_ask_instance = NULL;
HotBackupAskChain *g_hot_backup_ask_instance = NULL;
KeyRouteAskChain *g_key_route_ask_instance = NULL;
AgentHubAskChain *g_agent_hub_ask_instance = NULL;
JobHubAskChain *g_job_hub_ask_instance = NULL;
RemoteDtcAskAnswerChain *g_remote_dtc_instance = NULL;
BlackHoleAskChain *g_black_hole_ask_instance = NULL;

AgentListenPool *agent_listener = NULL;
ListenerPool *main_listener = NULL;

//single thread mode only use below two thread instance.
PollerBase *g_main_thread = NULL;
PollerBase *g_hot_backup_thread = NULL;
//below thread instance used in multi thread mode.
//remote dispatcher,in order to migrate data to remote dtc.
PollerBase *g_remote_thread = NULL;
PollerBase *g_buffer_multi_thread = NULL;
PollerBase *g_datasource_thread = NULL;

PluginManager *main_plugin_mgr;
int g_max_conn_cnt;
int enable_plugin;
int init_plugin;
int cache_key;
int g_datasource_mode;
int async_update;
int g_target_new_hash;
int g_hash_changing;
char cache_file[256] = CACHE_CONF_NAME;
char table_file[256] = TABLE_CONF_NAME;

extern DTCConfig *g_dtc_config;
extern void init_task_executor(const char *, AgentListenPool *,
			       JobAskInterface<DTCJobOperation> *);

int collect_load_config(DbConfig *dbconfig);

// second part of entry
static int init_thread(void *dummy);
// thread startp
static int single_thread_mode_initiazation();
static int multiple_thread_mode_initiazation();

#ifdef _CORE_
int main(int argc, char *argv[])
{
	init_proc_title(argc, argv);

	init_log4cplus();

	if (load_entry_parameter(argc, argv) < 0)
		return DTC_CODE_FAILED;

	if (g_dtc_config->get_int_val("cache", "EnableCoreDump", 1))
		init_core_dump();

	if (init_config_info())
		return DTC_CODE_FAILED;
	if (init_statistics())
		return DTC_CODE_FAILED;
	if (init_cache_mode() < 0)
		return DTC_CODE_FAILED;
	if (init_daemon() < 0)
		return DTC_CODE_FAILED;
	Thread::set_auto_config_instance(
		g_dtc_config->get_auto_config_instance("cache"));
	log4cplus_debug("entry start dtc");
	if (start_dtc(init_thread, NULL) < 0)
	{
		log4cplus_debug("entry will exit");
		abort();
		return DTC_CODE_FAILED;
	}
	log4cplus_debug("entry start dtc finished.");

	if (init_thread(NULL))
		return DTC_CODE_FAILED;

	Logger::shutdown();
	return DTC_CODE_SUCCESS;
}
#endif
//main thread initialization.
static int init_thread(void *dummy)
{
	int ret = DTC_CODE_SUCCESS;

	Thread *root_thread =
		new Thread("dtc-thread-root", Thread::ThreadTypeProcess);
	if (root_thread != NULL)
		root_thread->initialize_thread();
	if (daemon_set_fd_limit(g_dtc_config->get_int_val("cache", "MaxFdCount",
							  10240)) < 0)
		return DTC_CODE_FAILED;

	//start statistic thread.
	g_stat_mgr.start_background_thread();

	int thread_mode =
		g_dtc_config->get_int_val("cache", "UseSingleThread", 1);
	//choose mode for single/multiple thread.
	switch (thread_mode) {
	case SINGLE_THREAD_MODE:
		ret = single_thread_mode_initiazation();
		break;
	case MULTIPLE_THREAD_MODE:
		ret = multiple_thread_mode_initiazation();
		break;

	default:
		log4cplus_error("thread mode error:%d", thread_mode);
	}

	if (ret == DTC_CODE_SUCCESS) {
		init_task_executor(TableDefinitionManager::instance()
					   ->get_cur_table_def()
					   ->table_name(),
				   agent_listener,
				   g_system_command_ask_instance);

		log4cplus_info("--------%s-v%s BEGIN!--------", project_name,
			       version);
		daemon_wait();
	} else {
		log4cplus_error(
			"thread initilization failed, now prepare to free resource and exit.");
	}

	log4cplus_info("--------%s-v%s free resource now --------",
		       project_name, version);

	free_all_resource();

	return ret;
}

//chain of responsibility pattern.
static int single_thread_mode_initiazation()
{
	// 1. thread initilization.
	if (init_main_chain_thread())
		return DTC_CODE_FAILED;

	if (init_hotbackup_chain_thread())
		return DTC_CODE_FAILED;

	// 2. chain initilization.
	if (init_remote_dtc_chain(g_main_thread) < 0)
		return DTC_CODE_FAILED;

	if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION) {
		if (init_data_connector_ask_chain(g_main_thread) < 0)
			return DTC_CODE_FAILED;
	}

	if (g_datasource_mode != DTC_MODE_DATABASE_ONLY) {
		if (init_buffer_process_ask_chain(g_main_thread) < 0)
			return DTC_CODE_FAILED;
	}

	int max_barrier_count =
		g_dtc_config->get_int_val("cache", "MaxBarrierCount", 100000);
	int max_key_count =
		g_dtc_config->get_int_val("cache", "MaxKeyCount", 10000);

	g_buffer_barrier_instance =
		new BarrierAskAnswerChain(g_main_thread, max_barrier_count,
					  max_key_count,
					  BarrierAskAnswerChain::IN_FRONT);
	if (g_datasource_mode == DTC_MODE_DATABASE_ONLY) {
		g_buffer_bypass_ask_instance =
			new BufferBypassAskChain(g_main_thread);
		//Step 4 : barrier_cache bind bypass_unit
		g_buffer_barrier_instance->get_main_chain()->register_next_chain(
			g_buffer_bypass_ask_instance);
		//Step 5 : bypass_unit bind helper_unit
		g_buffer_bypass_ask_instance->get_main_chain()
			->register_next_chain(g_data_connector_ask_instance);
	} else {
		g_key_route_ask_instance = new KeyRouteAskChain(
			g_main_thread, TableDefinitionManager::instance()
					       ->get_cur_table_def()
					       ->key_format());
		//ClusterConfig
		if (!check_and_create()) {
			log4cplus_error(
				"check_and_create cluster config error.");
			return DTC_CODE_FAILED;
		} else {
			log4cplus_debug("check_and_create cluster config ok.");
		}

		std::vector<ClusterNode> clusterConf;
		if (!parse_cluster_config(&clusterConf)) {
			log4cplus_error("parse_cluster_config error");
			return DTC_CODE_FAILED;
		} else {
			log4cplus_debug("parse_cluster_config ok");
		}

		g_key_route_ask_instance->init(clusterConf);
		if (g_key_route_ask_instance->load_node_state_if_any() != 0) {
			log4cplus_error("key route init error!");
			return DTC_CODE_FAILED;
		} else {
			log4cplus_debug("keyRoute->do_init ok");
		}

		//Setp 4 : barrier_cache bind key_route
		g_buffer_barrier_instance->get_main_chain()->register_next_chain(
			g_key_route_ask_instance);

		//Step 5 : key_route bind cache_process
		g_key_route_ask_instance->get_main_chain()->register_next_chain(
			g_buffer_process_ask_instance);
		//Step 5 : key_route bind remote_client
		g_key_route_ask_instance->get_remote_chain()
			->register_next_chain(g_remote_dtc_instance);

		//Step 6 : cache_process bind remoteClinet
		g_buffer_process_ask_instance->get_remote_chain()
			->register_next_chain(g_remote_dtc_instance);
		//Step 6 : cache_process bind hotback_process
		g_buffer_process_ask_instance->get_hotbackup_chain()
			->register_next_chain(g_hot_backup_ask_instance);

		if (g_datasource_mode == DTC_MODE_CACHE_ONLY) {
			g_black_hole_ask_instance =
				new BlackHoleAskChain(g_main_thread);
			//Step 6 : cache_process bind hole
			g_buffer_process_ask_instance->get_main_chain()
				->register_next_chain(
					g_black_hole_ask_instance);
		} else if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION) {
			if (g_buffer_process_ask_instance->update_mode() ||
			    g_buffer_process_ask_instance->is_mem_dirty()) {
				g_connector_barrier_instance =
					new BarrierAskAnswerChain(
						g_main_thread,
						max_barrier_count,
						max_key_count,
						BarrierAskAnswerChain::IN_BACK);
				//Step 6 : cache_process bind barrier_helper
				g_buffer_process_ask_instance->get_main_chain()
					->register_next_chain(
						g_connector_barrier_instance);
				//Step 7 : barrier_helper bind helper_unit
				g_connector_barrier_instance->get_main_chain()
					->register_next_chain(
						g_data_connector_ask_instance);
			} else {
				//Step 6 : cache_process bind helper_unit
				g_buffer_process_ask_instance->get_main_chain()
					->register_next_chain(
						g_data_connector_ask_instance);
			}
		} else {
			log4cplus_error("g_datasource_mode error:%d",
					g_datasource_mode);
			return DTC_CODE_FAILED;
		}
	}

	g_system_command_ask_instance =
		SystemCommandAskChain::get_instance(g_main_thread);
	if (NULL == g_system_command_ask_instance) {
		log4cplus_error(
			"create system command failed, errno[%d], msg[%s]",
			errno, strerror(errno));
		return DTC_CODE_FAILED;
	}
	//Step 3 : system_command_instance bind bar_cache
	g_system_command_ask_instance->get_main_chain()->register_next_chain(
		g_buffer_barrier_instance);

	g_job_hub_ask_instance = new JobHubAskChain(g_main_thread);
	//Step 2 : multi_plexer bind system_command_instance
	g_job_hub_ask_instance->get_main_chain()->register_next_chain(
		g_system_command_ask_instance);

	g_agent_hub_ask_instance = new AgentHubAskChain(g_main_thread);
	//Step 1 : agent_process bind multi_plexer
	g_agent_hub_ask_instance->get_main_chain()->register_next_chain(
		g_job_hub_ask_instance);

	//Step 0 : chain of responsibility entrance.
	agent_listener = new AgentListenPool();
	if (agent_listener->register_entrance_chain(
		    g_dtc_config, g_agent_hub_ask_instance, g_main_thread) < 0)
		return DTC_CODE_FAILED;

	int open_cnt = stat_open_fd();
	g_max_conn_cnt =
		g_dtc_config->get_int_val("cache", "MaxFdCount", 10240) -
		open_cnt - 10; // reserve 10 fds
	if (g_max_conn_cnt < 0) {
		log4cplus_error("MaxFdCount should large than %d",
				open_cnt + 10);
		return DTC_CODE_FAILED;
	}

	// start thread....
	if (g_main_thread)
		g_main_thread->running_thread();

	if (g_hot_backup_thread)
		g_hot_backup_thread->running_thread();

	return DTC_CODE_SUCCESS;
}

static int multiple_thread_mode_initiazation()
{
	if (init_hotbackup_chain_thread())
		return DTC_CODE_FAILED;
	if (init_data_connector_chain_thread() < 0)
		return DTC_CODE_FAILED;
	if (init_remote_dtc_chain_thread() < 0)
		return DTC_CODE_FAILED;
	

	if (g_datasource_mode == DTC_MODE_DATABASE_ONLY) {
		g_data_connector_ask_instance->disable_commit_group();
	} else if (init_buffer_process_ask_chain_thread() < 0) {
		return DTC_CODE_FAILED;
	}

	if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION) {
		g_data_connector_ask_instance->do_attach(g_datasource_thread);
	}

	int iMaxBarrierCount =
		g_dtc_config->get_int_val("cache", "MaxBarrierCount", 100000);
	int iMaxKeyCount =
		g_dtc_config->get_int_val("cache", "MaxKeyCount", 10000);

	g_buffer_barrier_instance = new BarrierAskAnswerChain(
		g_buffer_multi_thread ?: g_datasource_thread, iMaxBarrierCount,
		iMaxKeyCount, BarrierAskAnswerChain::IN_FRONT);
	if (g_datasource_mode == DTC_MODE_DATABASE_ONLY) {
		g_buffer_bypass_ask_instance =
			new BufferBypassAskChain(g_datasource_thread);
		g_buffer_barrier_instance->get_main_chain()->register_next_chain(
			g_buffer_bypass_ask_instance);
		g_buffer_bypass_ask_instance->get_main_chain()
			->register_next_chain(g_data_connector_ask_instance);
	} else {
		g_key_route_ask_instance =
			new KeyRouteAskChain(g_buffer_multi_thread,
					     TableDefinitionManager::instance()
						     ->get_cur_table_def()
						     ->key_format());
		if (!check_and_create()) {
			log4cplus_error("check_and_create error");
			return DTC_CODE_FAILED;
		} else {
			log4cplus_debug("check_and_create ok");
		}

		std::vector<ClusterNode> clusterConf;
		if (!parse_cluster_config(&clusterConf)) {
			log4cplus_error("parse_cluster_config error");
			return DTC_CODE_FAILED;
		} else {
			log4cplus_debug("parse_cluster_config ok");
		}

		g_key_route_ask_instance->init(clusterConf);
		if (g_key_route_ask_instance->load_node_state_if_any() != 0) {
			log4cplus_error("key route init error!");
			return DTC_CODE_FAILED;
		}

		log4cplus_debug("keyRoute->do_init ok");

		g_buffer_barrier_instance->get_main_chain()->register_next_chain(
			g_key_route_ask_instance);
		g_key_route_ask_instance->get_main_chain()->register_next_chain(
			g_buffer_process_ask_instance);
		g_key_route_ask_instance->get_remote_chain()
			->register_next_chain(g_remote_dtc_instance);
		g_buffer_process_ask_instance->get_remote_chain()
			->register_next_chain(g_remote_dtc_instance);
		g_buffer_process_ask_instance->get_hotbackup_chain()
			->register_next_chain(g_hot_backup_ask_instance);
		if (g_datasource_mode == DTC_MODE_CACHE_ONLY) {
			g_black_hole_ask_instance =
				new BlackHoleAskChain(g_datasource_thread);
			g_buffer_process_ask_instance->get_main_chain()
				->register_next_chain(
					g_black_hole_ask_instance);
		} else if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION) {
			if (g_buffer_process_ask_instance->update_mode() ||
			    g_buffer_process_ask_instance->is_mem_dirty()) {
				g_connector_barrier_instance =
					new BarrierAskAnswerChain(
						g_datasource_thread,
						iMaxBarrierCount, iMaxKeyCount,
						BarrierAskAnswerChain::IN_BACK);
				g_buffer_process_ask_instance->get_main_chain()
					->register_next_chain(
						g_connector_barrier_instance);
				g_connector_barrier_instance->get_main_chain()
					->register_next_chain(
						g_data_connector_ask_instance);
			} else {
				g_buffer_process_ask_instance->get_main_chain()
					->register_next_chain(
						g_data_connector_ask_instance);
			}
		} else {
			log4cplus_error("g_datasource_mode error:%d",
					g_datasource_mode);
			return DTC_CODE_FAILED;
		}
	}

	g_system_command_ask_instance = SystemCommandAskChain::get_instance(
		g_buffer_multi_thread ?: g_datasource_thread);
	if (NULL == g_system_command_ask_instance) {
		log4cplus_error(
			"create system command failed, errno[%d], msg[%s]",
			errno, strerror(errno));
		return DTC_CODE_FAILED;
	}

	g_system_command_ask_instance->get_main_chain()->register_next_chain(
		g_buffer_barrier_instance);
	log4cplus_debug("bind server control ok");

	g_job_hub_ask_instance = new JobHubAskChain(
		g_buffer_multi_thread ?: g_datasource_thread);
	g_job_hub_ask_instance->get_main_chain()->register_next_chain(
		g_system_command_ask_instance);

	g_agent_hub_ask_instance = new AgentHubAskChain(
		g_buffer_multi_thread ?: g_datasource_thread);
	g_agent_hub_ask_instance->get_main_chain()->register_next_chain(
		g_job_hub_ask_instance);

	agent_listener = new AgentListenPool();
	if (agent_listener->register_entrance_chain_multi_thread(
		    g_dtc_config, g_agent_hub_ask_instance) < 0)
		return DTC_CODE_FAILED;

	int open_cnt = stat_open_fd();
	g_max_conn_cnt =
		g_dtc_config->get_int_val("cache", "MaxFdCount", 10240) -
		open_cnt - 10; // reserve 10 fds
	if (g_max_conn_cnt < 0) {
		log4cplus_error("MaxFdCount should large than %d",
				open_cnt + 10);
		return DTC_CODE_FAILED;
	}

	if (g_datasource_thread)
		g_datasource_thread->running_thread();

	if (g_remote_thread)
		g_remote_thread->running_thread();

	if (g_hot_backup_thread)
		g_hot_backup_thread->running_thread();

	if (g_buffer_multi_thread)
		g_buffer_multi_thread->running_thread();

	agent_listener->running_all_threads();

	return DTC_CODE_SUCCESS;
}
