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
*/

#include "main_supply.h"

extern PollerBase *g_buffer_multi_thread;

extern int init_plugin;
extern int cache_key;
extern int g_datasource_mode;
extern int async_update;
extern int g_target_new_hash;
extern int g_hash_changing;
extern int enable_plugin;
extern PollerBase *g_datasource_thread;
extern DataConnectorAskChain *g_data_connector_ask_instance;
extern ListenerPool *listener;
extern PluginManager *main_plugin_mgr;
extern PollerBase *g_hot_backup_thread;
extern DTCConfig *g_dtc_config;
extern PollerBase *g_main_thread;
extern PollerBase *g_remote_thread;
extern RemoteDtcAskAnswerChain *g_remote_dtc_instance;
extern ListenerPool *main_listener;

extern BufferProcessAskChain *g_buffer_process_ask_instance;
extern HotBackupAskChain *g_hot_backup_ask_instance;
extern BarrierAskAnswerChain *g_buffer_barrier_instance;
extern KeyRouteAskChain *g_key_route_ask_instance;
extern BarrierAskAnswerChain *g_connector_barrier_instance;
extern BufferBypassAskChain *g_buffer_bypass_ask_instance;
extern AgentHubAskChain *g_agent_hub_ask_instance;
extern JobHubAskChain *g_job_hub_ask_instance;
extern BlackHoleAskChain *g_black_hole_ask_instance;

extern void StopTaskExecutor(void);

int plugin_start(void)
{
	init_plugin = 0;
	main_plugin_mgr = PluginManager::instance();
	if (NULL == main_plugin_mgr) {
		log4cplus_error("create PluginManager instance failed.");
		return DTC_CODE_FAILED;
	}

	if (main_plugin_mgr->open(g_dtc_config->get_int_val(
		    "cache", "PluginNetworkMode", 0)) != 0) {
		log4cplus_error("init plugin manager failed.");
		return DTC_CODE_FAILED;
	}

	init_plugin = 1;
	return DTC_CODE_SUCCESS;
}

int plugin_stop(void)
{
	main_plugin_mgr->close();
	PluginManager::destory();
	main_plugin_mgr = NULL;
	return DTC_CODE_SUCCESS;
}

int stat_open_fd()
{
	int count = 0;
	for (int i = 0; i < 1000; i++) {
		if (fcntl(i, F_GETFL, 0) != -1)
			count++;
	}
	return count;
}

int init_cache_mode()
{
	g_datasource_mode = g_dtc_config->get_int_val("cache", "DTC_MODE",
						      DTC_MODE_CACHE_ONLY);
	switch (g_datasource_mode) {
	case DTC_MODE_DATABASE_ADDITION:
		log4cplus_info("dtc datasource mode: %s(%d)",
			       "DTC_MODE_DATABASE_ADDITION", g_datasource_mode);
		break;
	case DTC_MODE_CACHE_ONLY:
		log4cplus_info("dtc datasource mode: %s(%d)",
			       "DTC_MODE_CACHE_ONLY", g_datasource_mode);
		break;
	case DTC_MODE_DATABASE_ONLY:
		log4cplus_info("dtc datasource mode: %s(%d)",
			       "DTC_MODE_DATABASE_ONLY", g_datasource_mode);
		break;
	default:
		log4cplus_error("datasource config error: %d",
				g_datasource_mode);
		return DTC_CODE_FAILED;
	}

	async_update = g_dtc_config->get_int_val("cache", "DelayUpdate", 0);
	if (async_update < 0 || async_update > 1) {
		log4cplus_error("Invalid DelayUpdate value");
		return DTC_CODE_FAILED;
	}

	const char *keyStr = g_dtc_config->get_str_val("cache", "DTCID");
	if (keyStr == NULL) {
		cache_key = 0;
	} else if (!strcasecmp(keyStr, "none") &&
		   g_datasource_mode != DTC_MODE_DATABASE_ONLY) {
		log4cplus_error(
			"Can not set DTC_MODE=(DTC_MODE_CACHE_ONLY|DTC_MODE_DATABASE_ADDITION) and DTCID=NONE together.");
		return DTC_CODE_FAILED;
	} else if (isdigit(keyStr[0])) {
		cache_key = strtol(keyStr, NULL, 0);
	} else {
		log4cplus_error("Invalid DTCID value \"%s\"", keyStr);
		return DTC_CODE_FAILED;
	}

	if (g_datasource_mode == DTC_MODE_DATABASE_ONLY && async_update) {
		log4cplus_error("can't DelayUpdate when DTCID set to NONE");
		return DTC_CODE_FAILED;
	}

	if (g_datasource_mode != DTC_MODE_DATABASE_ONLY && cache_key == 0)
		log4cplus_info("DTCID not set, cache data is volatile");

	if (g_datasource_mode == DTC_MODE_CACHE_ONLY)
		log4cplus_info("disable data source, cache data is volatile");

	return DTC_CODE_SUCCESS;
}

int init_main_chain_thread()
{
	g_main_thread = new PollerBase("dtc-thread-main");
	if (g_main_thread == NULL)
		return DTC_CODE_FAILED;
	if (g_main_thread->initialize_thread() == DTC_CODE_FAILED)
		return DTC_CODE_FAILED;

	return DTC_CODE_SUCCESS;
}

int init_hotbackup_chain_thread()
{
	log4cplus_debug("StartHotbackThread begin");
	g_hot_backup_thread = new PollerBase("dtc-thread-hotbackup");
	g_hot_backup_ask_instance = new HotBackupAskChain(g_hot_backup_thread);

	if (g_hot_backup_thread == NULL || g_hot_backup_ask_instance == NULL) {
		log4cplus_error(
			"hot backup thread or instance created failed.");
		return DTC_CODE_FAILED;
	}

	if (g_hot_backup_thread->initialize_thread() == DTC_CODE_FAILED) {
		log4cplus_error("init hotback thread fail");
		return DTC_CODE_FAILED;
	}
	if (g_hot_backup_ask_instance->do_init(
		    g_dtc_config->get_size_val("cache", "BinlogTotalSize",
					       BINLOG_MAX_TOTAL_SIZE, 'M'),
		    g_dtc_config->get_size_val("cache", "BinlogOneSize",
					       BINLOG_MAX_SIZE, 'M')) == -1) {
		log4cplus_error("hotbackProcess init fail");
		return DTC_CODE_FAILED;
	}

	log4cplus_debug("StartHotbackThread end");
	return DTC_CODE_SUCCESS;
}

int init_buffer_process_ask_chain_thread()
{
	log4cplus_error("init_buffer_process_ask_chain_thread start");
	g_buffer_multi_thread = new PollerBase("dtc-multi-thread-cache");
	g_buffer_process_ask_instance = new BufferProcessAskChain(
		g_buffer_multi_thread,
		TableDefinitionManager::instance()->get_cur_table_def(),
		async_update ? MODE_ASYNC : MODE_SYNC);
	g_buffer_process_ask_instance->set_limit_node_size(
		g_dtc_config->get_int_val("cache", "LimitNodeSize",
					  100 * 1024 * 1024));
	g_buffer_process_ask_instance->set_limit_node_rows(
		g_dtc_config->get_int_val("cache", "LimitNodeRows", 0));
	g_buffer_process_ask_instance->set_limit_empty_nodes(
		g_dtc_config->get_int_val("cache", "LimitEmptyNodes", 0));

	if (g_buffer_multi_thread->initialize_thread() == DTC_CODE_FAILED) {
		return DTC_CODE_FAILED;
	}

	unsigned long long cache_size =
		g_dtc_config->get_size_val("cache", "MAX_USE_MEM_MB", 0, 'M');
	if (cache_size <= (50ULL << 20)) // 50M
	{
		log4cplus_error("MAX_USE_MEM_MB too small");
		return DTC_CODE_FAILED;
	} else if (sizeof(long) == 4 && cache_size >= 4000000000ULL) {
		log4cplus_error("MAX_USE_MEM_MB %lld too large", cache_size);
	} else if (g_buffer_process_ask_instance->set_buffer_size_and_version(
			   cache_size,
			   g_dtc_config->get_int_val("cache", "CacheShmVersion",
						     4)) == DTC_CODE_FAILED) {
		return DTC_CODE_FAILED;
	}

	/* disable async transaction log */
	g_buffer_process_ask_instance->disable_async_log(1);

	int lruLevel =
		g_dtc_config->get_int_val("cache", "disable_lru_update", 0);
	if (g_datasource_mode == DTC_MODE_CACHE_ONLY) {
		if (g_buffer_process_ask_instance->enable_no_db_mode() < 0) {
			return DTC_CODE_FAILED;
		}
		if (g_dtc_config->get_int_val("cache", "disable_auto_purge",
					      0) > 0) {
			g_buffer_process_ask_instance->disable_auto_purge();
			// lruLevel = 3; /* LRU_WRITE */
		}
		int autoPurgeAlertTime = g_dtc_config->get_int_val(
			"cache", "AutoPurgeAlertTime", 0);
		g_buffer_process_ask_instance->set_date_expire_alert_time(
			autoPurgeAlertTime);
		if (autoPurgeAlertTime > 0 &&
		    TableDefinitionManager::instance()
				    ->get_cur_table_def()
				    ->lastcmod_field_id() <= 0) {
			log4cplus_error(
				"Can't start AutoPurgeAlert without lastcmod field");
			return DTC_CODE_FAILED;
		}
	}
	g_buffer_process_ask_instance->disable_lru_update(lruLevel);
	g_buffer_process_ask_instance->enable_lossy_data_source(
		g_dtc_config->get_int_val("cache", "LossyDataSource", 0));

	if (async_update != MODE_SYNC && cache_key == 0) {
		log4cplus_error(
			"Anonymous shared memory don't support DelayUpdate");
		return DTC_CODE_FAILED;
	}

	int iAutoDeleteDirtyShm = g_dtc_config->get_int_val(
		"cache", "AutoDeleteDirtyShareMemory", 0);
	/*disable empty node filter*/
	if (g_buffer_process_ask_instance->open_init_buffer(
		    cache_key, 0, iAutoDeleteDirtyShm) == DTC_CODE_FAILED) {
		return DTC_CODE_FAILED;
	}

	if (g_buffer_process_ask_instance->update_mode() ||
	    g_buffer_process_ask_instance->is_mem_dirty()) // asyncUpdate active
	{
		if (TableDefinitionManager::instance()
			    ->get_cur_table_def()
			    ->uniq_fields() < 1) {
			log4cplus_error("DelayUpdate needs uniq-field(s)");
			return DTC_CODE_FAILED;
		}

		if (g_datasource_mode == DTC_MODE_CACHE_ONLY) {
			if (g_buffer_process_ask_instance->update_mode()) {
				log4cplus_error(
					"Can't start async mode when disableDataSource.");
				return DTC_CODE_FAILED;
			} else {
				log4cplus_error(
					"Can't start disableDataSource with shm dirty,please flush async shm to db first or delete shm");
				return DTC_CODE_FAILED;
			}
		} else {
			if ((TableDefinitionManager::instance()
				     ->get_cur_table_def()
				     ->compress_field_id() >= 0)) {
				log4cplus_error(
					"sorry,DTC just support compress in disableDataSource mode now.");
				return DTC_CODE_FAILED;
			}
		}

		/*marker is the only source of flush speed calculattion, inc precision to 10*/
		g_buffer_process_ask_instance->set_flush_parameter(
			g_dtc_config->get_int_val("cache", "MarkerPrecision",
						  10),
			g_dtc_config->get_int_val("cache", "MaxFlushSpeed", 1),
			g_dtc_config->get_int_val("cache", "MinDirtyTime",
						  3600),
			g_dtc_config->get_int_val("cache", "MaxDirtyTime",
						  43200));

		g_buffer_process_ask_instance->set_drop_count(
			g_dtc_config->get_int_val("cache", "MaxDropCount",
						  1000));
	} else {
		if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION)
			g_data_connector_ask_instance->disable_commit_group();
	}

	if (g_buffer_process_ask_instance->set_insert_order(dbConfig->ordIns) <
	    0)
		return DTC_CODE_FAILED;

	log4cplus_error("init_buffer_process_ask_chain_thread end");

	return DTC_CODE_SUCCESS;
}

int collect_load_config(DbConfig *dbconfig)
{
	if (g_datasource_mode == DTC_MODE_CACHE_ONLY)
		return DTC_CODE_SUCCESS;

	if (!g_data_connector_ask_instance)
		return DTC_CODE_FAILED;

	if (dbconfig == NULL) {
		log4cplus_error("dbconfig == NULL");
		return DTC_CODE_FAILED;
	}

	if (g_data_connector_ask_instance->renew_config(dbconfig)) {
		log4cplus_error("helperunit renew config error!");
		return DTC_CODE_FAILED;
	}

	return DTC_CODE_SUCCESS;
}

int init_remote_dtc_chain_thread()
{
	log4cplus_debug("init_remote_dtc_chain_thread begin");
	g_remote_thread = new PollerBase("dtc-thread-remote");
	g_remote_dtc_instance = new RemoteDtcAskAnswerChain(
		g_remote_thread,
		g_dtc_config->get_int_val("cache", "HelperCountPerGroup", 16));
	if (g_remote_thread->initialize_thread() == DTC_CODE_FAILED) {
		log4cplus_error("init remote thread error");
		return DTC_CODE_FAILED;
	}

	//get helper timeout
	int timeout = g_dtc_config->get_int_val("cache", "HelperTimeout", 30);
	int retry = g_dtc_config->get_int_val("cache", "HelperRetryTimeout", 1);
	int connect =
		g_dtc_config->get_int_val("cache", "HelperConnectTimeout", 10);

	g_remote_dtc_instance->set_timer_handler(
		g_remote_thread->get_timer_list(timeout),
		g_remote_thread->get_timer_list(connect),
		g_remote_thread->get_timer_list(retry));
	log4cplus_debug("init_remote_dtc_chain_thread end");

	return DTC_CODE_SUCCESS;
}

int init_remote_dtc_chain(PollerBase *thread)
{
	log4cplus_debug("init_remote_dtc_chain begin");

	g_remote_dtc_instance = new RemoteDtcAskAnswerChain(
		thread,
		g_dtc_config->get_int_val("cache", "HelperCountPerGroup", 16));

	//get helper timeout
	int timeout = g_dtc_config->get_int_val("cache", "HelperTimeout", 30);
	int retry = g_dtc_config->get_int_val("cache", "HelperRetryTimeout", 1);
	int connect =
		g_dtc_config->get_int_val("cache", "HelperConnectTimeout", 10);

	g_remote_dtc_instance->set_timer_handler(
		thread->get_timer_list(timeout),
		thread->get_timer_list(connect), thread->get_timer_list(retry));
	log4cplus_debug("init_remote_dtc_chain end");

	return DTC_CODE_SUCCESS;
}
int init_data_connector_chain_thread()
{
	log4cplus_debug("init_data_connector_chain_thread begin");
	if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION) {
		g_data_connector_ask_instance = new DataConnectorAskChain();
		if (g_data_connector_ask_instance->load_config(
			    dbConfig, TableDefinitionManager::instance()
					      ->get_cur_table_def()
					      ->key_format()) == -1) {
			return DTC_CODE_FAILED;
		}
	}

	//get helper timeout
	int timeout = g_dtc_config->get_int_val("cache", "HelperTimeout", 30);
	int retry = g_dtc_config->get_int_val("cache", "HelperRetryTimeout", 1);
	int connect =
		g_dtc_config->get_int_val("cache", "HelperConnectTimeout", 10);

	g_datasource_thread = new PollerBase("dtc-thread-datasource");
	if (g_datasource_thread->initialize_thread() == DTC_CODE_FAILED)
		return DTC_CODE_FAILED;

	if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION)
		g_data_connector_ask_instance->set_timer_handler(
			g_datasource_thread->get_timer_list(timeout),
			g_datasource_thread->get_timer_list(connect),
			g_datasource_thread->get_timer_list(retry));
	log4cplus_debug("init_data_connector_chain_thread end");

	return DTC_CODE_SUCCESS;
}

int init_buffer_process_ask_chain(PollerBase *thread)
{
	log4cplus_error("init_buffer_process_ask_chain start");
	g_buffer_process_ask_instance = new BufferProcessAskChain(
		thread, TableDefinitionManager::instance()->get_cur_table_def(),
		async_update ? MODE_ASYNC : MODE_SYNC);
	g_buffer_process_ask_instance->set_limit_node_size(
		g_dtc_config->get_int_val("cache", "LimitNodeSize",
					  100 * 1024 * 1024));
	g_buffer_process_ask_instance->set_limit_node_rows(
		g_dtc_config->get_int_val("cache", "LimitNodeRows", 0));
	g_buffer_process_ask_instance->set_limit_empty_nodes(
		g_dtc_config->get_int_val("cache", "LimitEmptyNodes", 0));

	unsigned long long cache_size =
		g_dtc_config->get_size_val("cache", "MAX_USE_MEM_MB", 0, 'M');
	if (cache_size <= (50ULL << 20)) // 50M
	{
		log4cplus_error("MAX_USE_MEM_MB too small");
		return DTC_CODE_FAILED;
	} else if (sizeof(long) == 4 && cache_size >= 4000000000ULL) {
		log4cplus_error("MAX_USE_MEM_MB %lld too large", cache_size);
	} else if (g_buffer_process_ask_instance->set_buffer_size_and_version(
			   cache_size,
			   g_dtc_config->get_int_val("cache", "CacheShmVersion",
						     4)) == DTC_CODE_FAILED) {
		return DTC_CODE_FAILED;
	}

	/* disable async transaction log */
	g_buffer_process_ask_instance->disable_async_log(1);

	int lruLevel =
		g_dtc_config->get_int_val("cache", "disable_lru_update", 0);
	if (g_datasource_mode == DTC_MODE_CACHE_ONLY) {
		if (g_buffer_process_ask_instance->enable_no_db_mode() < 0) {
			return DTC_CODE_FAILED;
		}
		if (g_dtc_config->get_int_val("cache", "disable_auto_purge",
					      0) > 0) {
			g_buffer_process_ask_instance->disable_auto_purge();
			// lruLevel = 3; /* LRU_WRITE */
		}
		int autoPurgeAlertTime = g_dtc_config->get_int_val(
			"cache", "AutoPurgeAlertTime", 0);
		g_buffer_process_ask_instance->set_date_expire_alert_time(
			autoPurgeAlertTime);
		if (autoPurgeAlertTime > 0 &&
		    TableDefinitionManager::instance()
				    ->get_cur_table_def()
				    ->lastcmod_field_id() <= 0) {
			log4cplus_error(
				"Can't start AutoPurgeAlert without lastcmod field");
			return DTC_CODE_FAILED;
		}
	}
	g_buffer_process_ask_instance->disable_lru_update(lruLevel);
	g_buffer_process_ask_instance->enable_lossy_data_source(
		g_dtc_config->get_int_val("cache", "LossyDataSource", 0));

	if (async_update != MODE_SYNC && cache_key == 0) {
		log4cplus_error(
			"Anonymous shared memory don't support DelayUpdate");
		return DTC_CODE_FAILED;
	}

	int iAutoDeleteDirtyShm = g_dtc_config->get_int_val(
		"cache", "AutoDeleteDirtyShareMemory", 0);
	/*disable empty node filter*/
	if (g_buffer_process_ask_instance->open_init_buffer(
		    cache_key, 0, iAutoDeleteDirtyShm) == DTC_CODE_FAILED) {
		return DTC_CODE_FAILED;
	}

	if (g_buffer_process_ask_instance->update_mode() ||
	    g_buffer_process_ask_instance->is_mem_dirty()) // asyncUpdate active
	{
		if (TableDefinitionManager::instance()
			    ->get_cur_table_def()
			    ->uniq_fields() < 1) {
			log4cplus_error("DelayUpdate needs uniq-field(s)");
			return DTC_CODE_FAILED;
		}

		switch (g_datasource_mode) {
		case DTC_MODE_CACHE_ONLY:
			if (g_buffer_process_ask_instance->update_mode()) {
				log4cplus_error(
					"Can't start async mode when disableDataSource.");
				return DTC_CODE_FAILED;
			} else {
				log4cplus_error(
					"Can't start disableDataSource with shm dirty,please flush async shm to db first or delete shm");
				return DTC_CODE_FAILED;
			}
			break;
		case DTC_MODE_DATABASE_ADDITION:
			if ((TableDefinitionManager::instance()
				     ->get_cur_table_def()
				     ->compress_field_id() >= 0)) {
				log4cplus_error(
					"sorry,DTC just support compress in disableDataSource mode now.");
				return DTC_CODE_FAILED;
			}
			break;
		default:
			log4cplus_error("datasource mode error:%d",
					g_datasource_mode);
			return DTC_CODE_FAILED;
		}

		/*marker is the only source of flush speed calculattion, inc precision to 10*/
		g_buffer_process_ask_instance->set_flush_parameter(
			g_dtc_config->get_int_val("cache", "MarkerPrecision",
						  10),
			g_dtc_config->get_int_val("cache", "MaxFlushSpeed", 1),
			g_dtc_config->get_int_val("cache", "MinDirtyTime",
						  3600),
			g_dtc_config->get_int_val("cache", "MaxDirtyTime",
						  43200));

		g_buffer_process_ask_instance->set_drop_count(
			g_dtc_config->get_int_val("cache", "MaxDropCount",
						  1000));
	} else {
		if (g_datasource_mode == DTC_MODE_DATABASE_ADDITION)
			g_data_connector_ask_instance->disable_commit_group();
	}

	if (g_buffer_process_ask_instance->set_insert_order(dbConfig->ordIns) <
	    0)
		return DTC_CODE_FAILED;

	log4cplus_debug("init_buffer_process_ask_chain end");

	return DTC_CODE_SUCCESS;
}

int init_data_connector_ask_chain(PollerBase *thread)
{
	log4cplus_debug("init_data_connector_ask_chain begin");

	g_data_connector_ask_instance = new DataConnectorAskChain();
	if (g_data_connector_ask_instance->load_config(
		    dbConfig, TableDefinitionManager::instance()
				      ->get_cur_table_def()
				      ->key_format()) == -1) {
		return DTC_CODE_FAILED;
	}
	//get helper timeout
	int timeout = g_dtc_config->get_int_val("cache", "HelperTimeout", 30);
	int retry = g_dtc_config->get_int_val("cache", "HelperRetryTimeout", 1);
	int connect =
		g_dtc_config->get_int_val("cache", "HelperConnectTimeout", 10);

	g_data_connector_ask_instance->set_timer_handler(
		thread->get_timer_list(timeout),
		thread->get_timer_list(connect), thread->get_timer_list(retry));

	g_data_connector_ask_instance->do_attach(thread);
	if (g_datasource_mode == DTC_MODE_DATABASE_ONLY) {
		g_data_connector_ask_instance->disable_commit_group();
	}
	log4cplus_debug("init_data_connector_ask_chain end");

	return DTC_CODE_SUCCESS;
}
//获取、配置基础信息
int init_config_info()
{
	// mkdir("/usr/local/dtc/stat", 0777);
	// mkdir("/usr/local/dtc/data", 0777);

	g_hash_changing = g_dtc_config->get_int_val("cache", "HashChanging", 0);
	g_target_new_hash =
		g_dtc_config->get_int_val("cache", "TargetNewHash", 0);

	DTCGlobal::pre_alloc_nodegroup_count =
		g_dtc_config->get_int_val("cache", "PreAllocNGNum", 1024);
	DTCGlobal::pre_alloc_nodegroup_count =
		DTCGlobal::pre_alloc_nodegroup_count <= 1 ?
			1 :
			DTCGlobal::pre_alloc_nodegroup_count >= (1 << 12) ?
			1 :
			DTCGlobal::pre_alloc_nodegroup_count;

	DTCGlobal::min_chunk_size_ =
		g_dtc_config->get_int_val("cache", "MinChunkSize", 0);
	if (DTCGlobal::min_chunk_size_ < 0) {
		DTCGlobal::min_chunk_size_ = 0;
	}

	DTCGlobal::pre_purge_nodes_ =
		g_dtc_config->get_int_val("cache", "pre_purge_nodes", 0);
	if (DTCGlobal::pre_purge_nodes_ < 0) {
		DTCGlobal::pre_purge_nodes_ = 0;
	} else if (DTCGlobal::pre_purge_nodes_ > 10000) {
		DTCGlobal::pre_purge_nodes_ = 10000;
	}

	RELATIVE_HOUR_CALCULATOR->set_base_hour(
		g_dtc_config->get_int_val("cache", "RelativeYear", 2014));

	log4cplus_info("Table %s: key/field# %d/%d, keysize %d",
		       dbConfig->tblName,
		       TableDefinitionManager::instance()
			       ->get_cur_table_def()
			       ->key_fields(),
		       TableDefinitionManager::instance()
				       ->get_cur_table_def()
				       ->num_fields() +
			       1,
		       TableDefinitionManager::instance()
			       ->get_cur_table_def()
			       ->max_key_size());

	return DTC_CODE_SUCCESS;
}

void free_all_resource()
{
	//stop plugin
	if (enable_plugin && init_plugin) {
		plugin_stop();
	}

	DELETE(main_listener);

	if (g_buffer_multi_thread) {
		g_buffer_multi_thread->interrupt();
	}
	if (g_hot_backup_thread) {
		g_hot_backup_thread->interrupt();
	}
	if (g_datasource_thread) {
		g_datasource_thread->interrupt();
	}

	if (g_remote_thread) {
		g_remote_thread->interrupt();
	}

	if (g_main_thread) {
		g_main_thread->interrupt();
	}

	StopTaskExecutor();

	DELETE(g_buffer_process_ask_instance);
	DELETE(g_data_connector_ask_instance);
	DELETE(g_buffer_barrier_instance);
	DELETE(g_key_route_ask_instance);
	DELETE(g_connector_barrier_instance);
	DELETE(g_buffer_bypass_ask_instance);
	DELETE(g_hot_backup_ask_instance);
	DELETE(g_remote_dtc_instance);
	DELETE(g_black_hole_ask_instance);
	DELETE(g_agent_hub_ask_instance);
	DELETE(g_job_hub_ask_instance);

	DELETE(g_buffer_multi_thread);
	DELETE(g_datasource_thread);
	DELETE(g_remote_thread);
	DELETE(g_main_thread);
	DELETE(g_hot_backup_thread);
	g_stat_mgr.stop_background_thread();
	log4cplus_info("--------%s-v%s END!--------", project_name, version);
	daemon_cleanup();
#if MEMCHECK
	dump_non_delete();
	log4cplus_debug("memory allocated %lu virtual %lu", count_alloc_size(),
			count_virtual_size());
#endif
}