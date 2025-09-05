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
#include <sched.h>

#include "thread.h"
#include "mem_check.h"
#include "../log/log.h"
#include <sched.h>

__attribute__((__weak__)) volatile extern int stop;
Thread::thread_map_t Thread::_thread_map;
pthread_mutex_t Thread::_thread_map_lock = PTHREAD_MUTEX_INITIALIZER;

Thread *Thread::FindThreadByName(const char *name)
{
	Thread::LOCK_THREAD_MAP();
	thread_map_t::const_iterator i = _thread_map.find(name);
	Thread *ret = i == _thread_map.end() ? NULL : i->second;
	Thread::UNLOCK_THREAD_MAP();
	return ret;
}

__attribute__((__weak__)) void DaemonCrashed(int);

#if HAS_TLS
static __thread Thread *currentThread __TLS_MODEL;

extern "C" void crash_hook(int signo)
{
	if (currentThread != NULL)
		currentThread->crash_hook(signo);
}
#endif

void *Thread::Entry(void *thread)
{
	Thread *my = (Thread *)thread;
	my->prepare_internal();
	void *ret = my->do_process();
	my->Cleanup();
	return ret;
}

Thread::Thread(const char *name, int type)
{
	tid = 0;
	pid = 0;
	tasktype = type;
	taskname = STRDUP(name);
	stacksize = 0;
	cpumask = 0;

	stopped = 0;
	stopPtr = &stopped;

	pthread_mutex_init(&runlock, NULL);

	Thread::LOCK_THREAD_MAP();
	_thread_map[taskname] = this;
	Thread::UNLOCK_THREAD_MAP();
}

Thread::~Thread()
{
	pthread_mutex_destroy(&runlock);
	FREE_IF(taskname);
}

void Thread::set_stack_size(int size)
{
	if (size < 0) // default size
		size = 0;
	else if (size > 64 << 20) // max 64M
		size = 64 << 20;

	// round to 64K
	if ((size & 0xFFFF) != 0)
		size = (size & ~0xFFFF) + 0x10000;
	stacksize = size;
}

AutoConfig *Thread::g_autoconf = NULL;

void Thread::auto_config_stack_size(void)
{
	unsigned long long size = g_autoconf->get_size_val(
		"ThreadStackSize", taskname, stacksize, 'M');

	if (size > (1 << 30))
		size = 1 << 30;

	set_stack_size((int)size);
	log4cplus_debug("autoconf thread %s ThreadStackSize %d", taskname,
			stacksize);
}

void Thread::auto_config_cpu_mask(void)
{
#ifdef CPU_ZERO
	log4cplus_debug("autoconf thread %s ThreadCPUMask %llx", taskname,
			(unsigned long long)cpumask);
#endif
}

void Thread::auto_config(void)
{
	if (g_autoconf) {
		switch (tasktype) {
		case ThreadTypeSync:
		case ThreadTypeAsync:
			auto_config_stack_size();
			break;
		default:
			break;
		}
		auto_config_cpu_mask();
	}
}

int Thread::initialize_thread(void)
{
	int ret = initialize();
	if (ret < 0)
		return -1;
	switch (tasktype) {
	case 0: // root
		tid = pthread_self();
		auto_config();
#ifdef CPU_ZERO
		if (cpumask != 0) {
			uint64_t temp[2] = { cpumask, 0 };
			sched_setaffinity(pid, sizeof(temp),
					  (cpu_set_t *)&temp);
		}
#endif
		break;

	case -1: // world class
		tid = 0;
		break;

	case 1: // async
	case 2: // sync
		auto_config();
		pthread_mutex_lock(&runlock);
		pthread_attr_t attr;
		pthread_attr_init(&attr);
		if (stacksize) {
			pthread_attr_setstacksize(&attr, stacksize);
		}
		pthread_create(&tid, &attr, Entry, this);
		break;
	}
	return 0;
}

void Thread::running_thread()
{
	switch (tasktype) {
	case -1:
		// error
		return;
	case 0:
		stopPtr = &stop;
		prepare_internal();
		do_process();
		Cleanup();
		break;
	case 1:
	case 2:
		pthread_mutex_unlock(&runlock);
		break;
	}
}

void Thread::prepare_internal(void)
{
	pid = _gettid_();
	log4cplus_info("thread %s[%d] started", taskname, pid);
	sigset_t sset;
	sigemptyset(&sset);
	sigaddset(&sset, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sset, &sset);
#ifdef CPU_ZERO
	if (cpumask != 0) {
		uint64_t temp[2] = { cpumask, 0 };
		sched_setaffinity(pid, sizeof(temp), (cpu_set_t *)&temp);
	}
#endif
	Prepare();
	pthread_mutex_lock(&runlock);

#if HAS_TLS
	currentThread = this;
#endif
}

void Thread::interrupt(void)
{
	if (this == NULL || stopped || !tid)
		return;

	int ret;
	stopped = 1;

	if (tasktype > 0) {
		pthread_kill(tid, SIGINT);
		pthread_mutex_unlock(&runlock);

		if ((ret = pthread_join(tid, 0)) != 0) {
			log4cplus_warning("Thread [%s] join failed %d.", Name(),
					  ret);
		} else {
			log4cplus_info("Thread [%s] stopped.", Name());
		}
	}
}

int Thread::initialize(void)
{
	return 0;
}

void Thread::Prepare(void)
{
}

void *Thread::do_process(void)
{
	while (!stopping())
		pause();
	return NULL;
}

void Thread::Cleanup(void)
{
}

void Thread::crash_hook(int signo)
{
	// mark mine is stopped
	this->stopped = 1;
	// global stopping
	if (&DaemonCrashed != 0) {
		DaemonCrashed(signo);
	}
	log4cplus_error("Ouch......, I crashed, hang and stopping server");
	pthread_exit(NULL);
	while (1)
		pause();
}

Thread Thread::TheWorldThread("world", Thread::ThreadTypeWorld);
