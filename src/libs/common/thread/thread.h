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
#ifndef __GENERICTHREAD_H__
#define __GENERICTHREAD_H__

#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <linux/unistd.h>

#include "algorithm/timestamp.h"
#include "../poll/poller.h"
#include "../timer/timer_list.h"
#include "../config/config.h"

class Thread {
    public:
	enum { ThreadTypeWorld = -1,
	       ThreadTypeProcess = 0,
	       ThreadTypeAsync = 1,
	       ThreadTypeSync = 2 };

    public:
	static AutoConfig *g_autoconf;
	static void set_auto_config_instance(AutoConfig *ac)
	{
		g_autoconf = ac;
	};

    public:
	Thread(const char *name, int type = 0);
	virtual ~Thread();

	int initialize_thread();
	void running_thread();
	const char *Name(void)
	{
		return taskname;
	}
	int Pid(void) const
	{
		return stopped ? 0 : pid;
	}
	void set_stack_size(int);
	int stopping(void)
	{
		return *stopPtr;
	}
	virtual void interrupt(void);
	virtual void crash_hook(int);

	Thread *the_world(void)
	{
		return &TheWorldThread;
	};

    protected:
	char *taskname;
	pthread_t tid;
	pthread_mutex_t runlock;
	int stopped;
	volatile int *stopPtr;
	int tasktype; // 0-->main, 1--> epoll, 2-->sync, -1, world
	int stacksize;
	uint64_t cpumask;
	int pid;

    protected:
	virtual int initialize(void);
	virtual void Prepare(void);
	virtual void *do_process(void);
	virtual void Cleanup(void);

    protected:
	struct cmp {
		bool operator()(const char *const &a,
				const char *const &b) const
		{
			return strcmp(a, b) < 0;
		}
	};
	typedef std::map<const char *, Thread *, cmp> thread_map_t;
	static thread_map_t _thread_map;
	static pthread_mutex_t _thread_map_lock;

	static void LOCK_THREAD_MAP()
	{
		pthread_mutex_lock(&_thread_map_lock);
	}
	static void UNLOCK_THREAD_MAP()
	{
		pthread_mutex_unlock(&_thread_map_lock);
	}

    public:
	static Thread *FindThreadByName(const char *name);

    private:
	static Thread TheWorldThread;
	static void *Entry(void *thread);
	void prepare_internal(void);
	void auto_config(void);
	void auto_config_stack_size(void);
	void auto_config_cpu_mask(void);
};

#endif
