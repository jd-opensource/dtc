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

#include <buffer_bypass_answer_chain.h>

BufferBypassAnswerChain::~BufferBypassAnswerChain(void)
{
}

void BufferBypassAnswerChain::job_answer_procedure(DTCJobOperation *job)
{
	if (job->result)
		job->pass_all_result(job->result);
	job->turn_around_job_answer();
}
