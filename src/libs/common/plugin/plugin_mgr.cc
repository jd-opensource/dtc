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
#include <dlfcn.h>
#include <errno.h>
#include <string.h>

#include "../config/config.h"
#include "plugin_listener_pool.h"
#include "plugin_worker.h"
#include "plugin_mgr.h"
#include "algorithm/singleton.h"
#include "mem_check.h"
#include "../log/log.h"

extern DTCConfig *g_dtc_config;
typedef void (*so_set_network_mode_t)(void);
typedef void (*so_set_strings_t)(const char *);

PluginManager::PluginManager(void)
	: _dll(NULL), _config(NULL), _plugin_name(NULL),
	  _plugin_listener_pool(NULL), _worker_notifier(NULL),
	  _worker_number(1), _sb_if_handle(NULL), _plugin_timer(NULL)
{
}

PluginManager::~PluginManager(void)
{
}

PluginManager *PluginManager::instance(void)
{
	return Singleton<PluginManager>::instance();
}

void PluginManager::destory(void)
{
	return Singleton<PluginManager>::destory();
}

int PluginManager::open(int mode)
{
	//create worker notifier
	NEW(worker_notify_t, _worker_notifier);
	if (NULL == _worker_notifier) {
		log4cplus_error("create worker notifier failed, msg:%s",
				strerror(errno));
		return -1;
	}

	if (load_plugin_depend(mode) != 0) {
		return -1;
	}

	//create dll info
	_dll = (dll_func_t *)MALLOC(sizeof(dll_func_t));
	if (NULL == _dll) {
		log4cplus_error("malloc dll_func_t object failed, msg:%s",
				strerror(errno));
		return -1;
	}
	memset(_dll, 0x00, sizeof(dll_func_t));

	_plugin_name = g_dtc_config->get_str_val("cache", "PluginName");
	if (NULL == _plugin_name) {
		log4cplus_error("PluginName[%p] is invalid", _plugin_name);
		return -1;
	}

	//get plugin config file
	const char *_s1 = "cache";
	const char *_s2 = "PluginName";
	PluginGlobal::_plugin_conf_file = g_dtc_config->get_str_val(_s1, _s2);
	if (NULL == PluginGlobal::_plugin_conf_file) {
		log4cplus_info("PluginConfigFile[%p] is invalid",
			       PluginGlobal::_plugin_conf_file);
	}

	if (register_plugin(_plugin_name) != 0) {
		log4cplus_error("register plugin[%s] failed", _plugin_name);
		return -1;
	}

	//invoke handle_init
	if (NULL != _dll->handle_init) {
		const char *plugin_config_file[2] = {
			(const char *)PluginGlobal::_plugin_conf_file, NULL
		};
		if (_dll->handle_init(1, (char **)plugin_config_file,
				      PROC_MAIN) != 0) {
			log4cplus_error(
				"invoke plugin[%s]::handle_init() failed",
				_plugin_name);
			return -1;
		}
	}

	//crate PluginListenerPool object
	NEW(PluginListenerPool, _plugin_listener_pool);
	if (NULL == _plugin_listener_pool) {
		log4cplus_error(
			"create PluginListenerPool object failed, msg:%s",
			strerror(errno));
		return -1;
	}
	if (_plugin_listener_pool->do_bind(g_dtc_config) != 0) {
		return -1;
	}

	//create worker pool
	if (create_worker_pool(g_dtc_config->get_int_val(
		    "cache", "PluginWorkerNumber")) != 0) {
		log4cplus_error("create plugin worker pool failed.");
		return -1;
	}

	//create plugin timer notify
	if (_dll->handle_timer_notify) {
		if (create_plugin_timer(g_dtc_config->get_int_val(
			    "cache", "PluginTimerNotifyInterval", 10)) != 0) {
			log4cplus_error("create usr timer notify failed.");
			return -1;
		}
	}

	return 0;
}

int PluginManager::close(void)
{
	//stop plugin timer
	if (_plugin_timer) {
		_plugin_timer->interrupt();
		DELETE(_plugin_timer);
		_plugin_timer = NULL;
	}

	//stop worker
	_worker_notifier->Stop(PLUGIN_STOP_REQUEST);

	//destroy worker
	for (int i = 0; i < _worker_number; ++i) {
		if (NULL != _plugin_worker_pool[i]) {
			_plugin_worker_pool[i]->interrupt();
		}

		DELETE(_plugin_worker_pool[i]);
	}
	FREE(_plugin_worker_pool);
	_plugin_worker_pool = NULL;

	//delete plugin listener pool
	DELETE(_plugin_listener_pool);

	//unregister plugin
	if (NULL != _dll->handle_fini) {
		_dll->handle_fini(PROC_MAIN);
	}
	unregister_plugin();
	FREE_CLEAR(_dll);

	//destroy work notifier
	DELETE(_worker_notifier);

	return 0;
}

int PluginManager::load_plugin_depend(int mode)
{
	char *error = NULL;
	so_set_network_mode_t so_set_network_mode = NULL;
	so_set_strings_t so_set_server_address = NULL;
	so_set_strings_t so_set_server_tablename = NULL;

	//load server bench so
	void *sb_if_handle =
		dlopen(SERVER_BENCH_SO_FILE, RTLD_NOW | RTLD_GLOBAL);
	if ((error = dlerror()) != NULL) {
		log4cplus_error("so file[%s] dlopen error, %s",
				SERVER_BENCH_SO_FILE, error);
		return -1;
	}

	//load dtc api so
	void *dtcapi_handle = dlopen(DTC_API_SO_FILE, RTLD_NOW | RTLD_GLOBAL);
	if ((error = dlerror()) != NULL) {
		log4cplus_error("so file[%s] dlopen error, %s", DTC_API_SO_FILE,
				error);
		return -1;
	}

	//init plugin logger
	const char *plugin_log_path =
		g_dtc_config->get_str_val("cache", "PluginLogPath");
	if (NULL == plugin_log_path) {
		plugin_log_path = (const char *)"../log";
	}
	const char *plugin_log_name =
		g_dtc_config->get_str_val("cache", "PluginLogName");
	if (NULL == plugin_log_name) {
		plugin_log_name = (const char *)"plugin_";
	}
	int plugin_log_level = g_dtc_config->get_idx_val(
		"cache", "LOG_LEVEL",
		((const char *const[]){ "emerg", "alert", "crit", "error",
					"warning", "notice", "info", "debug",
					NULL }),
		6);
	int plugin_log_size =
		g_dtc_config->get_int_val("cache", "PluginLogSize", 1 << 28);

	DLFUNC(sb_if_handle, _sb_if_handle, plugin_log_init_t, "log_init");
	if (_sb_if_handle(plugin_log_path, plugin_log_level, plugin_log_size,
			  plugin_log_name) != 0) {
		log4cplus_error("init plugin logger failed.");
		return -1;
	}

	//set_network_mode
	DLFUNC(dtcapi_handle, so_set_network_mode, so_set_network_mode_t,
	       "set_network_mode");
	if (so_set_network_mode && mode) {
		so_set_network_mode();
	}

	//set_server_address
	DLFUNC(dtcapi_handle, so_set_server_address, so_set_strings_t,
	       "set_server_address");
	if (so_set_server_address) {
		log4cplus_debug("set server address:%s",
				g_dtc_config->get_str_val("cache",
							  "BIND_ADDR"));
		so_set_server_address(
			g_dtc_config->get_str_val("cache", "BIND_ADDR"));
	}
	//set_server_tablename
	DLFUNC(dtcapi_handle, so_set_server_tablename, so_set_strings_t,
	       "set_server_tablename");
	if (so_set_server_tablename) {
		if (!g_dtc_config->get_str_val("TABLE_CONF", "table_name")) {
			log4cplus_error("can't find tablename in table.yaml");
			return 0;
		}
		log4cplus_debug("set server tablename:%s",
				g_dtc_config->get_str_val("TABLE_CONF",
							  "table_name"));
		so_set_server_tablename(
			g_dtc_config->get_str_val("TABLE_CONF", "table_name"));
	}

	return 0;

out:
	return -1;
}

int PluginManager::register_plugin(const char *file_name)
{
	char *error = NULL;
	int ret_code = -1;

	_dll->handle = dlopen(file_name, RTLD_NOW);
	if ((error = dlerror()) != NULL) {
		log4cplus_error("dlopen error, %s", error);
		goto out;
	}

	DLFUNC_NO_ERROR(_dll->handle, _dll->handle_init, handle_init_t,
			"handle_init");
	DLFUNC_NO_ERROR(_dll->handle, _dll->handle_fini, handle_fini_t,
			"handle_fini");
	DLFUNC_NO_ERROR(_dll->handle, _dll->handle_open, handle_open_t,
			"handle_open");
	DLFUNC_NO_ERROR(_dll->handle, _dll->handle_close, handle_close_t,
			"handle_close");
	DLFUNC_NO_ERROR(_dll->handle, _dll->handle_timer_notify,
			handle_timer_notify_t, "handle_timer_notify");
	DLFUNC(_dll->handle, _dll->handle_input, handle_input_t,
	       "handle_input");
	DLFUNC(_dll->handle, _dll->handle_process, handle_process_t,
	       "handle_process");

	ret_code = 0;

out:
	if (0 == ret_code) {
		log4cplus_info("open plugin:%s successful.", file_name);
	} else {
		log4cplus_info("open plugin:%s failed.", file_name);
	}

	return ret_code;
}

void PluginManager::unregister_plugin(void)
{
	if (NULL != _dll) {
		if (NULL != _dll->handle) {
			dlclose(_dll->handle);
		}

		memset(_dll, 0x00, sizeof(dll_func_t));
	}

	return;
}

int PluginManager::create_worker_pool(int worker_number)
{
	_worker_number = worker_number;

	if (_worker_number < 1 ||
	    _worker_number > PluginGlobal::_max_worker_number) {
		log4cplus_warning("worker number[%d] is invalid, default[%d]",
				  _worker_number,
				  PluginGlobal::_default_worker_number);
		_worker_number = PluginGlobal::_default_worker_number;
	}

	_plugin_worker_pool =
		(Thread **)calloc(_worker_number, sizeof(Thread *));
	if (NULL == _plugin_worker_pool) {
		log4cplus_error("calloc worker pool failed, msg:%s",
				strerror(errno));
		return -1;
	}

	for (int i = 0; i < _worker_number; ++i) {
		char name[32];
		snprintf(name, sizeof(name) - 1, "plugin_worker_%d", i);

		NEW(PluginWorkerThread(name, _worker_notifier, i),
		    _plugin_worker_pool[i]);
		if (NULL == _plugin_worker_pool[i]) {
			log4cplus_error("create %s failed, msg:%s", name,
					strerror(errno));
			return -1;
		}

		if (_plugin_worker_pool[i]->initialize_thread() != 0) {
			log4cplus_error("%s initialize failed.", name);
			return -1;
		}

		_plugin_worker_pool[i]->running_thread();
	}

	return 0;
}

int PluginManager::create_plugin_timer(const int interval)
{
	char name[32] = { '\0' };
	int plugin_timer_interval = 0;

	plugin_timer_interval = (interval < 1) ? 1 : interval;
	snprintf(name, sizeof(name) - 1, "%s", "plugin_timer");

	NEW(PluginTimer(name, plugin_timer_interval), _plugin_timer);
	if (NULL == _plugin_timer) {
		log4cplus_error("create %s failed, msg:%s", name,
				strerror(errno));
		return -1;
	}

	if (_plugin_timer->initialize_thread() != 0) {
		log4cplus_error("%s initialize failed.", name);
		return -1;
	}

	_plugin_timer->running_thread();

	return 0;
}
