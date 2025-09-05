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
#ifndef THREAD_CPU_STAT_H
#define THREAD_CPU_STAT_H

#include "../log/log.h"
#include "../poll/poller_base.h"
#include "stat_dtc.h"
#include <string>
#define THREAD_CPU_STAT_MAX_ERR 6

#define CPU_STAT_INVALID 0
#define CPU_STAT_NOT_INIT 1
#define CPU_STAT_INIT 2
#define CPU_STAT_FINE 3
#define CPU_STAT_ILL 4

typedef struct one_thread_cpu_stat {
	int _fd; //proc fd thread hold
	int _pid; //thread pid
	const char *_thread_name;
	double _last_time;
	double _this_time;
	unsigned long long _last_cpu_time; //previous cpu time of this thread
	unsigned long long _this_cpu_time; //this cpu time of this thread
	StatCounter _pcpu; //cpu percentage
	unsigned int _err_times; //max error calculate time
	unsigned int _status; //status
	struct one_thread_cpu_stat *_next;

    public:
	int init(const char *thread_name, int thread_idx, int pid);
	int read_cpu_time(void);
	void cal_pcpu(void);
	void do_stat();
} one_thread_cpu_stat_t;

class thread_cpu_stat {
    private:
	one_thread_cpu_stat_t *_stat_list_head;

    public:
	thread_cpu_stat()
	{
		_stat_list_head = NULL;
	}
	~thread_cpu_stat()
	{
	}
	int init();
	void do_stat();

    private:
	int add_cpu_stat_object(const char *thread_name, int thread_idx);
};

#endif
