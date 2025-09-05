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
#include "agent_listen_pool.h"
#include "agent_listener.h"
#include "job_entrance_ask_chain.h"
#include "poll/poller_base.h"
#include "config/config.h"
#include "task/task_request.h"
#include "log/log.h"
#include "stat_dtc.h"
#include "../config/dbconfig.h"
#include "yaml-cpp/yaml.h"

AgentListenPool::AgentListenPool()
{
	memset(thread, 0, sizeof(PollerBase *) * MAX_AGENT_LISTENER);
	memset(job_entrance_ask_instance, 0,
	       sizeof(JobEntranceAskChain *) * MAX_AGENT_LISTENER);
	memset(listener, 0, sizeof(AgentListener *) * MAX_AGENT_LISTENER);

	StatCounter stat_agent_cur_conn_count =
		g_stat_mgr.get_stat_int_counter(AGENT_CONN_COUNT);
	stat_agent_cur_conn_count = 0;
}

AgentListenPool::~AgentListenPool()
{
	for (int i = 0; i < MAX_AGENT_LISTENER; i++) {
		if (thread[i])
			thread[i]->interrupt();
		delete listener[i];
		delete job_entrance_ask_instance[i];
	}
}

int AgentListenPool::register_entrance_chain_multi_thread(
	DTCConfig *gc, JobAskInterface<DTCJobOperation> *next_chain)
{
	char bindstr[64];
	std::string bindaddr;
	const char *errmsg = NULL;
	char thread_name[64];
	int checktime;
	int blog;

	checktime = gc->get_int_val("cache", "AgentRcvBufCheck", 5);
	blog = gc->get_int_val("cache", "AgentListenBlog", 256);

	for (int i = 0; i < MAX_AGENT_LISTENER; i++) {
		if (i == 0)
			snprintf(bindstr, sizeof(bindstr), "BIND_ADDR");
		else
			snprintf(bindstr, sizeof(bindstr), "BIND_ADDR%d", i);

		bindaddr = DbConfig::get_bind_addr(gc->get_config_node());
		if (bindaddr.length() == 0)
			continue;

		if ((errmsg = socket_address[i].set_address(
			     bindaddr.c_str(), (const char *)NULL)))
			continue;

		snprintf(thread_name, sizeof(thread_name), "dtc-thread-main-%d",
			 i);
		thread[i] = new PollerBase(thread_name);
		if (thread[i] == NULL) {
			log4cplus_error(
				"no mem to create multi-thread main thread %d",
				i);
			return -1;
		}
		if (thread[i]->initialize_thread() < 0) {
			log4cplus_error(
				"multi-thread main thread %d init error", i);
			return -1;
		}

		job_entrance_ask_instance[i] =
			new JobEntranceAskChain(thread[i], checktime);
		if (job_entrance_ask_instance[i] == NULL) {
			log4cplus_error("no mem to new agent client unit %d",
					i);
			return -1;
		}
		job_entrance_ask_instance[i]
			->get_main_chain()
			->register_next_chain(next_chain);

		listener[i] = new AgentListener(thread[i],
						job_entrance_ask_instance[i],
						socket_address[i]);
		if (listener[i] == NULL) {
			log4cplus_error("no mem to new agent listener %d", i);
			return -1;
		}
		if (listener[i]->do_bind(blog) < 0) {
			log4cplus_error("agent listener %d bind error", i);
			return -1;
		}

		if (listener[i]->attach_thread() < 0)
			return -1;
	}

	return 0;
}

int AgentListenPool::register_entrance_chain(
	DTCConfig *gc, JobAskInterface<DTCJobOperation> *next_chain,
	PollerBase *bind_thread)
{
	std::string bindaddr;
	const char *errmsg = NULL;
	int checktime;
	int blog;

	checktime = gc->get_int_val("cache", "AgentRcvBufCheck", 5);
	blog = gc->get_int_val("cache", "AgentListenBlog", 256);

	bindaddr = DbConfig::get_bind_addr(gc->get_config_node());
	if (bindaddr == "") {
		log4cplus_error("get cache BIND_ADDR configure failed");
		return -1;
	}

	if ((errmsg = socket_address[0].set_address(bindaddr.c_str(),
						    (const char *)NULL))) {
		log4cplus_error("socket_address[0] setaddress failed");
		return -1;
	}

	thread[0] = bind_thread;

	job_entrance_ask_instance[0] =
		new JobEntranceAskChain(thread[0], checktime);
	if (job_entrance_ask_instance[0] == NULL) {
		log4cplus_error("no mem to new agent client unit");
		return -1;
	}
	job_entrance_ask_instance[0]->get_main_chain()->register_next_chain(
		next_chain);

	listener[0] = new AgentListener(thread[0], job_entrance_ask_instance[0],
					socket_address[0]);
	if (listener[0] == NULL) {
		log4cplus_error("no mem to new agent listener");
		return -1;
	}
	if (listener[0]->do_bind(blog) < 0) {
		log4cplus_error("agent listener bind error");
		return -1;
	}
	if (listener[0]->attach_thread() < 0)
		return -1;

	return 0;
}

int AgentListenPool::running_all_threads()
{
	for (int i = 0; i < MAX_AGENT_LISTENER; i++) {
		if (thread[i])
			thread[i]->running_thread();
	}

	return 0;
}

int AgentListenPool::Match(const char *host, const char *port)
{
	for (int i = 0; i < MAX_AGENT_LISTENER; i++) {
		if (listener[i] == NULL)
			continue;
		if (socket_address[i].Match(host, port))
			return 1;
	}
	return 0;
}
