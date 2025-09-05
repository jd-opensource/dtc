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

#include <buffer_bypass_ask_chain.h>
#include <buffer_bypass_answer_chain.h>

static BufferBypassAnswerChain g_buffer_bypass_answer_instance;

BufferBypassAskChain::~BufferBypassAskChain(void)
{
}

void BufferBypassAskChain::job_ask_procedure(DTCJobOperation *job_operation)
{
	if (job_operation->is_batch_request()) {
		job_operation->turn_around_job_answer();
		return;
	}

	if (job_operation->count_only() &&
	    (job_operation->requestInfo.limit_start() ||
	     job_operation->requestInfo.limit_count())) {
		job_operation->set_error(
			-EC_BAD_COMMAND, "BufferBypass",
			"There's nothing to limit because no fields required");
		job_operation->turn_around_job_answer();
		return;
	}

	job_operation->mark_as_pass_thru();
	job_operation->push_reply_dispatcher(&g_buffer_bypass_answer_instance);

	main_chain.job_ask_procedure(job_operation);
}
