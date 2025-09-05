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
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

#include "poller_base.h"
#include "mem_check.h"
#include "poller.h"
#include "myepoll.h"
#include "../log/log.h"
#include <sched.h>

volatile extern int stop;

PollerBase::PollerBase(const char *name)
	: Thread(name, Thread::ThreadTypeAsync), EpollOperation(1000),
	  poll_timeout(2000)
{
}

PollerBase::~PollerBase()
{
}

int PollerBase::initialize(void)
{
	if (Thread::g_autoconf != NULL) {
		int mp0 = get_max_pollers();
		int mp1;
		mp1 = g_autoconf->get_int_val("MaxIncomingPollers", Name(),
					      mp0);
		log4cplus_debug("autoconf thread %s MaxIncomingPollers %d",
				taskname, mp1);
		if (mp1 > mp0) {
			set_max_pollers(mp1);
		}
	}
	if (initialize_poller_unit() < 0)
		return -1;
	return 0;
}

void *PollerBase::do_process(void)
{
	while (!stopping()) {
		// if previous event loop has no events,
		// don't allow zero epoll wait time
		int timeout = expire_micro_seconds(
			poll_timeout, need_request_events_count == 0);

		// watting for recive new job or epoll time out.
		int interrupted = wait_poller_events(timeout);

		update_now_time(timeout, interrupted);

		if (stopping())
			break;

		// main business process function.
		process_poller_events();

		// Timer process function.
		check_expired(get_now_time());

		TimerUnit::check_ready();
		ReadyUnit::check_ready(get_now_time());

		delay_apply_events();
	}
	return 0;
}
