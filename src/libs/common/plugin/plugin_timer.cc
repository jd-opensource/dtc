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
#include <sched.h>

#include "plugin_agent_mgr.h"
#include "plugin_timer.h"
#include "plugin_global.h"
#include "../log/log.h"

PluginTimer::PluginTimer(const char *name, const int interval)
	: Thread(name, Thread::ThreadTypeSync), _interval(interval)
{
	pthread_mutex_init(&_wake_lock, NULL);
}

PluginTimer::~PluginTimer()
{
}

int PluginTimer::initialize(void)
{
	_dll = PluginAgentManager::instance()->get_dll();
	if ((NULL == _dll) || (NULL == _dll->handle_timer_notify)) {
		log4cplus_error("get server bench handle failed.");
		return -1;
	}

	return pthread_mutex_trylock(&_wake_lock) == 0 ? 0 : -1;
}

void PluginTimer::interrupt(void)
{
	pthread_mutex_unlock(&_wake_lock);

	return Thread::interrupt();
}

static void BlockAllSignals(void)
{
	sigset_t sset;
	sigfillset(&sset);
	sigdelset(&sset, SIGSEGV);
	sigdelset(&sset, SIGBUS);
	sigdelset(&sset, SIGABRT);
	sigdelset(&sset, SIGILL);
	sigdelset(&sset, SIGCHLD);
	sigdelset(&sset, SIGFPE);
	pthread_sigmask(SIG_BLOCK, &sset, &sset);
}

void *PluginTimer::do_process(void)
{
	BlockAllSignals();

	struct timeval tv;
	struct timespec ts;
	time_t nextsec = 0;
	unsigned long long nextnsec = 0;
	int ret_value = 0;

#define ONESEC_NSEC 1000000000ULL
#define ONESEC_MSEC 1000ULL
	gettimeofday(&tv, NULL);
	nextsec = tv.tv_sec;
	nextnsec = (tv.tv_usec + _interval * ONESEC_MSEC) * ONESEC_MSEC;
	while (nextnsec >= ONESEC_NSEC) {
		nextsec++;
		nextnsec -= ONESEC_NSEC;
	}

	ts.tv_sec = nextsec;
	ts.tv_nsec = nextnsec;

	while (!stopping()) {
		if (pthread_mutex_timedlock(&_wake_lock, &ts) == 0) {
			break;
		}

		ret_value = _dll->handle_timer_notify(0, NULL);
		if (0 != ret_value) {
			log4cplus_error(
				"invoke handle_timer_notify failed, return value:%d timer notify[%d]",
				ret_value, _gettid_());
		}

		gettimeofday(&tv, NULL);

		nextsec = tv.tv_sec;
		nextnsec = (tv.tv_usec + _interval * ONESEC_MSEC) * ONESEC_MSEC;
		while (nextnsec >= ONESEC_NSEC) {
			nextsec++;
			nextnsec -= ONESEC_NSEC;
		}

		ts.tv_sec = nextsec;
		ts.tv_nsec = nextnsec;
	}

	pthread_mutex_unlock(&_wake_lock);

	return NULL;
}
