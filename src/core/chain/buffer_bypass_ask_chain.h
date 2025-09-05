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

#ifndef __BUFFER_BYPASS_ASK_CHAIN__
#define __BUFFER_BYPASS_ASK_CHAIN__

#include <task/task_request.h>

class BufferBypassAskChain : public JobAskInterface<DTCJobOperation> {
    public:
	BufferBypassAskChain(PollerBase *o)
		: JobAskInterface<DTCJobOperation>(o), main_chain(o){};
	virtual ~BufferBypassAskChain(void);
	void register_next_chain(JobAskInterface<DTCJobOperation> *p)
	{
		main_chain.register_next_chain(p);
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}

    private:
	ChainJoint<DTCJobOperation> main_chain;

	virtual void job_ask_procedure(DTCJobOperation *);
};

#endif