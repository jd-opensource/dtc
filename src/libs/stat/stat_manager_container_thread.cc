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
#include "stat_manager_container_thread.h"

StatManagerContainerThread *StatManagerContainerThread::getInstance()
{
	static StatManagerContainerThread msStatThread;
	return &msStatThread;
}

StatManagerContainerThread::StatManagerContainerThread()
{
	pthread_mutex_init(&wake_lock_, NULL);
}

StatManagerContainerThread::~StatManagerContainerThread()
{
	std::map<uint32_t, StatManager *>::iterator iter =
		stat_Managers_map_.begin();
	std::map<uint32_t, StatManager *>::const_iterator end_iter =
		stat_Managers_map_.end();
	for (; iter != end_iter; ++iter) {
		iter->second->clear();
	}
}

void *StatManagerContainerThread::enter_thread(void *p)
{
	StatManagerContainerThread *my = (StatManagerContainerThread *)p;
	my->loop_thread();
	return NULL;
}

int StatManagerContainerThread::start_background_thread(void)
{
	P __a(this);
	if (pthread_mutex_trylock(&wake_lock_) == 0) {
		int ret = pthread_create(&thread_id_, 0, enter_thread,
					 (void *)this);
		if (ret != 0) {
			errno = ret;
			return -1;
		}
	}
	return 0;
}

int StatManagerContainerThread::stop_background_thread(void)
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

void StatManagerContainerThread::loop_thread(void)
{
	log4cplus_info("thread stat[%d] started.", _gettid_());

	BlockAllSignals();

	time_t next = 0;
	struct timeval time_value;
	gettimeofday(&time_value, NULL);
	next = (time_value.tv_sec / 10) * 10 + 10;
	struct timespec ts;
	ts.tv_sec = next;
	ts.tv_nsec = 0;

	while (pthread_mutex_timedlock(&wake_lock_, &ts) != 0) {
		gettimeofday(&time_value, NULL);
		if (time_value.tv_sec >= next) {
			stat_manager_container_thread_mutex_lock_.lock();
			std::map<uint32_t, StatManager *>::iterator iter =
				stat_Managers_map_.begin();
			std::map<uint32_t, StatManager *>::const_iterator
				end_iter = stat_Managers_map_.end();

			for (; iter != end_iter; ++iter) {
				iter->second->run_job_once();
			}
			stat_manager_container_thread_mutex_lock_.unlock();

			gettimeofday(&time_value, NULL);
			next = (time_value.tv_sec / 10) * 10 + 10;
		}
		ts.tv_sec = next;
		ts.tv_nsec = 0;
	}
	pthread_mutex_unlock(&wake_lock_);
}

int StatManagerContainerThread::add_stat_manager(uint32_t moudleId,
						 StatManager *stat)
{
	ScopedLock autoLock(stat_manager_container_thread_mutex_lock_);
	if (stat_Managers_map_.find(moudleId) != stat_Managers_map_.end())
		return -1;

	stat_Managers_map_[moudleId] = stat;
	return 0;
}

int StatManagerContainerThread::delete_stat_manager(uint32_t moudleId)
{
	ScopedLock autoLock(stat_manager_container_thread_mutex_lock_);
	std::map<uint32_t, StatManager *>::iterator i =
		stat_Managers_map_.find(moudleId);
	if (i != stat_Managers_map_.end()) {
		delete i->second;
		stat_Managers_map_.erase(i);

		return 0;
	}
	return -1;
}

StatCounter StatManagerContainerThread::operator()(int moduleId, int cId)
{
	StatCounter dummy;
	std::map<uint32_t, StatManager *>::iterator i =
		stat_Managers_map_.find(moduleId);
	if (i != stat_Managers_map_.end()) {
		return i->second->get_stat_int_counter(cId);
	}
	return dummy;
}
