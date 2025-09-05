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
#include "job_entrance_ask_chain.h"
#include "timer/timer_list.h"
#include "task/task_request.h"
#include "poll/poller_base.h"

JobEntranceAskChain::JobEntranceAskChain(PollerBase *t, int c)
	: ownerThread(t), main_chain(t), check(c)
{
	tlist = t->get_timer_list(c);

	stat_job_procedure_time[0] = g_stat_mgr.get_sample(REQ_USEC_ALL);
	stat_job_procedure_time[1] = g_stat_mgr.get_sample(REQ_USEC_GET);
	stat_job_procedure_time[2] = g_stat_mgr.get_sample(REQ_USEC_INS);
	stat_job_procedure_time[3] = g_stat_mgr.get_sample(REQ_USEC_UPD);
	stat_job_procedure_time[4] = g_stat_mgr.get_sample(REQ_USEC_DEL);
	stat_job_procedure_time[5] = g_stat_mgr.get_sample(REQ_USEC_FLUSH);
	stat_job_procedure_time[6] = g_stat_mgr.get_sample(REQ_USEC_HIT);
	stat_job_procedure_time[7] = g_stat_mgr.get_sample(REQ_USEC_REPLACE);
}

JobEntranceAskChain::~JobEntranceAskChain()
{
}

void JobEntranceAskChain::record_job_procedure_time(int hit, int type,
						    unsigned int usec)
{
	static const unsigned char cmd2type[] = {
		/*TYPE_PASS*/ 0,
		/*result_code*/ 0,
		/*DTCResultSet*/ 0,
		/*HelperAdmin*/ 0,
		/*Get*/ 1,
		/*Purge*/ 5,
		/*Insert*/ 2,
		/*Update*/ 3,
		/*Delete*/ 4,
		/*Other*/ 0,
		/*Other*/ 0,
		/*Other*/ 0,
		/*Replace*/ 7,
		/*Flush*/ 5,

		/*Invalidate*/ 0,
		/*Monitor*/ 0,
		/*ReloadConfig*/ 0,
		/*Replicate*/ 1,
		/*Migrate*/ 1,
	};
	stat_job_procedure_time[0].push(usec);
	unsigned int t = hit ? 6 : cmd2type[type];
	if (t)
		stat_job_procedure_time[t].push(usec);
}

void JobEntranceAskChain::record_job_procedure_time(DTCJobOperation *req)
{
	record_job_procedure_time(req->flag_is_hit(), req->request_code(),
				  req->responseTimer.live());
}
