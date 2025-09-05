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

#include <stdio.h>
#include <map>

#include "compiler.h"
#include "container.h"
#include "version.h"
#include "table/table_def.h"
#include "dtc_error_code.h"
#include "listener/listener_pool.h"
#include "request/request_threading.h"
#include "task/task_multi_unit.h"
#include "../libs/dep/dtcint.h"
#include "agent/agent_listen_pool.h"
#include "table/table_def_manager.h"

class DTCTaskExecutor : public IDTCTaskExecutor,
			public ThreadingOutputDispatcher<DTCJobOperation> {
    public:
	virtual NCResultInternal *task_execute(NCRequest &rq,
					       const DTCValue *kptr);
};

NCResultInternal *DTCTaskExecutor::task_execute(NCRequest &rq,
						const DTCValue *kptr)
{
	NCResultInternal *res = new NCResultInternal(rq.table_definition_);
	if (res->Copy(rq, kptr) < 0)
		return res;
	res->set_owner_info(this, 0, NULL);
	switch (ThreadingOutputDispatcher<DTCJobOperation>::do_execute(
		(DTCJobOperation *)res)) {
	case 0: // OK
		res->process_internal_result(res->Timestamp());
		break;
	case -1: // no side effect
		res->set_error(-EC_REQUEST_ABORTED, "API::sending",
			       "Server Shutdown");
		break;
	case -2:
	default: // result unknown, leak res by purpose
		 //new NCResult(-EC_REQUEST_ABORTED, "API::recving", "Server Shutdown");
		log4cplus_error(
			"(-EC_REQUEST_ABORTED, API::sending, Server Shutdown");
		break;
	}
	return res;
}

class DTCInstance : public IDTCService {
    public:
	AgentListenPool *ports;
	DTCTaskExecutor *executor;
	int mypid;

    public:
	DTCInstance();
	virtual ~DTCInstance();

	virtual const char *query_version_string(void);
	virtual const char *query_service_type(void);
	virtual const char *query_instance_name(void);

	virtual DTCTableDefinition *query_table_definition(void);
	virtual DTCTableDefinition *query_admin_table_definition(void);
	virtual IDTCTaskExecutor *query_task_executor(void);
	virtual int match_listening_ports(const char *, const char * = NULL);

	int IsOK(void) const
	{
		return this != NULL && ports != NULL && executor != NULL &&
		       getpid() == mypid;
	}
};

extern ListenerPool *main_listener;
DTCInstance::DTCInstance(void)
{
	ports = NULL;
	executor = NULL;
	mypid = getpid();
}

DTCInstance::~DTCInstance(void)
{
}

const char *DTCInstance::query_version_string(void)
{
	return version_detail;
}

const char *DTCInstance::query_service_type(void)
{
	return "core";
}

const char *DTCInstance::query_instance_name(void)
{
	return TableDefinitionManager::instance()
		->get_cur_table_def()
		->table_name();
}

DTCTableDefinition *DTCInstance::query_table_definition(void)
{
	return TableDefinitionManager::instance()->get_cur_table_def();
}

DTCTableDefinition *DTCInstance::query_admin_table_definition(void)
{
	return TableDefinitionManager::instance()->get_hot_backup_table_def();
}

IDTCTaskExecutor *DTCInstance::query_task_executor(void)
{
	return executor;
}

int DTCInstance::match_listening_ports(const char *host, const char *port)
{
	return ports->Match(host, port);
}

struct nocase {
	bool operator()(const char *const &a, const char *const &b) const
	{
		return strcasecmp(a, b) < 0;
	}
};
typedef std::map<const char *, DTCInstance, nocase> instmap_t;
static instmap_t instMap;

extern "C" __EXPORT IInternalService *
_QueryInternalService(const char *name, const char *instance)
{
	instmap_t::iterator i;

	if (!name || !instance)
		return NULL;

	if (strcasecmp(name, "core") != 0)
		return NULL;

	/* not found */
	i = instMap.find(instance);
	if (i == instMap.end())
		return NULL;

	DTCInstance &inst = i->second;
	if (inst.IsOK() == 0)
		return NULL;

	return &inst;
}

void init_task_executor(const char *name, AgentListenPool *listener,
			JobAskInterface<DTCJobOperation> *main_chain)
{
	log4cplus_debug("name: %s, pool: %p, job: %p", name, listener, main_chain);
	if (NCResultInternal::verify_class() == 0) {
		log4cplus_error(
			"Inconsistent class NCResultInternal detected, internal API disabled");
		return;
	}
	// this may cause memory leak, but this is small
	char *tablename = (char *)malloc(strlen(name) + 1);
	memset(tablename, 0, strlen(name) + 1);
	strncpy(tablename, name, strlen(name));

	DTCInstance &inst = instMap[tablename];
	inst.ports = listener;
	DTCTaskExecutor *executor = new DTCTaskExecutor();
	JobHubAskChain *batcher =
		new JobHubAskChain(main_chain->get_owner_thread());
	batcher->get_main_chain()->register_next_chain(main_chain);
	executor->register_next_chain(batcher);
	inst.executor = executor;
	log4cplus_info("Internal Job Executor initialized");
}

void StopTaskExecutor(void)
{
	instmap_t::iterator i;
	for (i = instMap.begin(); i != instMap.end(); i++) {
		if (i->second.executor)
			i->second.executor->Stop();
	}
}
