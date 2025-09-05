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
* Author:  Yangshuang, yangshuang68@jd.com    Wuxinzhen, wuxinzhen1@jd.com
*/
#ifndef __AGENT_UNIT_H__
#define __AGENT_UNIT_H__

#include "request/request_base.h"

class PollerBase;
class DTCJobOperation;

class AgentHubAskChain : public JobAskInterface<DTCJobOperation> {
    private:
	PollerBase *ownerThread;
	ChainJoint<DTCJobOperation> main_chain;

    public:
	AgentHubAskChain(PollerBase *o);
	virtual ~AgentHubAskChain();

	inline void register_next_chain(JobAskInterface<DTCJobOperation> *p)
	{
		main_chain.register_next_chain(p);
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}

	virtual void job_ask_procedure(DTCJobOperation *curr);
};

#endif
