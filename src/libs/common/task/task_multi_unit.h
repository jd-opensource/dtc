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
#ifndef __JOB_MULTI_ASK_CHAIN__
#define __JOB_MULTI_ASK_CHAIN__

#include "request/request_base.h"
#include "multi_request.h"

class DTCJobOperation;

class JobHubAskChain : public JobAskInterface<DTCJobOperation> {
    public:
	JobHubAskChain(PollerBase *o)
		: JobAskInterface<DTCJobOperation>(o), main_chain(o),
		  fastUpdate(0){};
	virtual ~JobHubAskChain(void);

	void register_next_chain(JobAskInterface<DTCJobOperation> *p)
	{
		main_chain.register_next_chain(p);
	}
	void push_task_queue(DTCJobOperation *req)
	{
		main_chain.indirect_notify(req);
	}
	void enable_fast_update(void)
	{
		fastUpdate = 1;
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}

    private:
	ChainJoint<DTCJobOperation> main_chain;
	virtual void job_ask_procedure(DTCJobOperation *);
	int fastUpdate;
};

#endif
