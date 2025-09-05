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
#include "agent_unit.h"
#include "poll/poller_base.h"
#include "task/task_request.h"
#include "log/log.h"

AgentHubAskChain::AgentHubAskChain(PollerBase *o)
	: JobAskInterface<DTCJobOperation>(o), ownerThread(o), main_chain(o)
{
}

AgentHubAskChain::~AgentHubAskChain()
{
}

void AgentHubAskChain::job_ask_procedure(DTCJobOperation *curr)
{
	log4cplus_debug("enter job_ask_procedure");
	curr->copy_reply_for_agent_sub_task();

	//there is a race condition here:
	//curr may be deleted during process (in job->turn_around_job_answer())
	int taskCount = curr->agent_sub_task_count();
	log4cplus_debug("job_ask_procedure job count is %d", taskCount);
	for (int i = 0; i < taskCount; i++) {
		DTCJobOperation *job = NULL;

		if (NULL == (job = curr->curr_agent_sub_task(i)))
			continue;

		if (curr->is_curr_sub_task_processed(i)) {
			log4cplus_debug("job_ask_procedure job reply notify");
			job->turn_around_job_answer();
		}

		else {
			log4cplus_debug("job_ask_procedure next process");
			main_chain.job_ask_procedure(job);
		}
	}
	log4cplus_debug("leave job_ask_procedure");

	return;
}
