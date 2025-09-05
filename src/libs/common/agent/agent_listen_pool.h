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
#ifndef __AGENT_LISTEN_POOL_H__
#define __AGENT_LISTEN_POOL_H__

#include "request/request_base.h"
#include "socket/socket_addr.h"

#define MAX_AGENT_LISTENER 10

class JobEntranceAskChain;
class AgentListener;
class PollerBase;
class DTCConfig;
class DTCJobOperation;
class AgentListenPool {
    private:
	SocketAddress socket_address[MAX_AGENT_LISTENER];
	PollerBase *thread[MAX_AGENT_LISTENER];
	JobEntranceAskChain *job_entrance_ask_instance[MAX_AGENT_LISTENER];
	AgentListener *listener[MAX_AGENT_LISTENER];

    public:
	AgentListenPool();
	~AgentListenPool();

	int register_entrance_chain_multi_thread(
		DTCConfig *gc, JobAskInterface<DTCJobOperation> *next_chain);
	int
	register_entrance_chain(DTCConfig *gc,
				JobAskInterface<DTCJobOperation> *next_chain,
				PollerBase *bind_thread);
	int running_all_threads();
	int Match(const char *host, const char *port);
};

#endif
