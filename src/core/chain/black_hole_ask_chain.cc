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

#include <black_hole_ask_chain.h>

BlackHoleAskChain::~BlackHoleAskChain(void)
{
}

void BlackHoleAskChain::job_ask_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("enter job_ask_procedure");
	// preset affected_rows==0 is obsoleted
	// use BlackHole flag instead
	job_operation->mark_as_black_hole();
	job_operation->turn_around_job_answer();
	log4cplus_debug("leave job_ask_procedure");
}
