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
#include <blacklist/blacklist_unit.h>

DTC_USING_NAMESPACE

BlackListUnit::BlackListUnit(TimerList *t) : timer(t)
{
}

BlackListUnit::~BlackListUnit()
{
}

void BlackListUnit::job_timer_procedure(void)
{
	log4cplus_debug("enter timer procedure");
	log4cplus_debug("sched blacklist-expired job");

	/* expire all expired eslot */
	try_expired_blacklist();

	BlackList::dump_all_blslot();

	/* set timer agagin */
	attach_timer(timer);

	log4cplus_debug("leave timer procedure");
	return;
}
