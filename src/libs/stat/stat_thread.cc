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
* 
*/
#include <sys/mman.h>
#include <sys/time.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <mem_check.h>
#include <errno.h>
#include <signal.h>
#include <log/log.h>

#include "stat_thread.h"

StatThread::StatThread()
{
	pthread_mutex_init(&wake_lock_, NULL);
}

StatThread::~StatThread()
{
}

void *StatThread::__thread_entry(void *p)
{
	StatThread *my = (StatThread *)p;
	my->thread_loop();
	return NULL;
}

int StatThread::start_background_thread(void)
{
	P __a(this);

	if (pthread_mutex_trylock(&wake_lock_) == 0) {
		int ret = pthread_create(&thread_id_, 0, __thread_entry,
					 (void *)this);
		if (ret != 0) {
			errno = ret;
			return -1;
		}
	}
	return 0;
}

int StatThread::stop_background_thread(void)
{
	P __a(this);

	if (pthread_mutex_trylock(&wake_lock_) == 0) {
		pthread_mutex_unlock(&wake_lock_);
		return 0;
	}
	pthread_mutex_unlock(&wake_lock_);
	int ret = pthread_join(thread_id_, 0);
	if (ret == 0)
		log4cplus_info("Thread[stat] stopped.");
	else
		log4cplus_info("cannot stop thread[stat]");
	return ret;
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

void StatThread::thread_loop(void)
{
	log4cplus_info("thread stat[%d] started.", _gettid_());

	BlockAllSignals();

	time_t next = 0;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	next = (tv.tv_sec / 10) * 10 + 10;
	struct timespec ts;
	ts.tv_sec = next;
	ts.tv_nsec = 0;

	while (pthread_mutex_timedlock(&wake_lock_, &ts) != 0) {
		gettimeofday(&tv, NULL);
		if (tv.tv_sec >= next) {
			run_job_once();
			gettimeofday(&tv, NULL);
			next = (tv.tv_sec / 10) * 10 + 10;
		}
		ts.tv_sec = next;
		ts.tv_nsec = 0;
	}
	pthread_mutex_unlock(&wake_lock_);
}
