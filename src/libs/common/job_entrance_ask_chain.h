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
#ifndef __JOB_ENTRANCE_ASK_CHAIN__
#define __JOB_ENTRANCE_ASK_CHAIN__

#include "request/request_base_all.h"
#include "stat_dtc.h"

class TimerList;
class PollerBase;
class DTCJobOperation;
class JobEntranceAskChain {
    public:
	JobEntranceAskChain(PollerBase *t, int c);
	virtual ~JobEntranceAskChain();

	inline TimerList *get_timer_list()
	{
		return tlist;
	}
	void register_next_chain(JobAskInterface<DTCJobOperation> *proc)
	{
		main_chain.register_next_chain(proc);
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}

	inline void start_job_ask_procedure(DTCJobOperation *req)
	{
		log4cplus_debug("enter job_ask_procedure");
		main_chain.job_ask_procedure(req);
		log4cplus_debug("leave job_ask_procedure");
	}

	void record_job_procedure_time(int hit, int type, unsigned int usec);
	void record_job_procedure_time(DTCJobOperation *req);

    private:
	PollerBase *ownerThread;
	ChainJoint<DTCJobOperation> main_chain;
	int check;
	TimerList *tlist;
	StatSample stat_job_procedure_time[8];
};

#endif
