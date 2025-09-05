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
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <sched.h>

#include "plugin_agent_mgr.h"
#include "plugin_worker.h"
#include "plugin_global.h"
#include "mem_check.h"
#include "../log/log.h"

PluginWorkerThread::PluginWorkerThread(const char *name,
				       worker_notify_t *worker_notify,
				       int seq_no)
	: Thread(name, Thread::ThreadTypeSync), _worker_notify(worker_notify),
	  _seq_no(seq_no)
{
}

PluginWorkerThread::~PluginWorkerThread()
{
}

int PluginWorkerThread::initialize(void)
{
	_dll = PluginAgentManager::instance()->get_dll();
	if ((NULL == _dll) || (NULL == _dll->handle_init) ||
	    (NULL == _dll->handle_process)) {
		log4cplus_error("get server bench handle failed.");
		return -1;
	}

	return 0;
}

void PluginWorkerThread::Prepare(void)
{
	const char *plugin_config_file[2] = {
		(const char *)PluginGlobal::_plugin_conf_file, NULL
	};
	if (_dll->handle_init(1, (char **)plugin_config_file,
			      _seq_no + PROC_WORK) != 0) {
		log4cplus_error("invoke handle_init() failed, worker[%d]",
				_seq_no);
		return;
	}

	return;
}

void *PluginWorkerThread::do_process(void)
{
	plugin_request_t *plugin_request = NULL;

	while (!stopping()) {
		plugin_request = _worker_notify->Pop();

		if (PLUGIN_STOP_REQUEST == plugin_request) {
			break;
		}

		if (NULL == plugin_request) {
			log4cplus_error(
				"the worker_notify::Pop() invalid, plugin_request=%p, worker[%d]",
				plugin_request, _gettid_());
			continue;
		}

		if (stopping()) {
			break;
		}

		plugin_request->handle_process();
	}

	if (NULL != _dll->handle_fini) {
		_dll->handle_fini(_seq_no + PROC_WORK);
	}

	return NULL;
}
