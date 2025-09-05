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
#include <sys/param.h> //for HZ
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "../poll/poller_base.h"
#include "thread_cpu_stat.h"

int thread_cpu_stat::init()
{
	add_cpu_stat_object("worker", WORKER_THREAD_CPU_STAT);
	add_cpu_stat_object("cache", CACHE_THREAD_CPU_STAT);
	add_cpu_stat_object("source", DATA_SOURCE_CPU_STAT);
	add_cpu_stat_object("inc0", INC_THREAD_CPU_STAT_0);
	add_cpu_stat_object("inc1", INC_THREAD_CPU_STAT_1);
	add_cpu_stat_object("inc2", INC_THREAD_CPU_STAT_2);
	add_cpu_stat_object("inc3", INC_THREAD_CPU_STAT_3);
	add_cpu_stat_object("inc4", INC_THREAD_CPU_STAT_4);
	add_cpu_stat_object("inc5", INC_THREAD_CPU_STAT_4);
	add_cpu_stat_object("inc6", INC_THREAD_CPU_STAT_6);
	add_cpu_stat_object("inc7", INC_THREAD_CPU_STAT_7);
	add_cpu_stat_object("inc8", INC_THREAD_CPU_STAT_8);
	add_cpu_stat_object("inc9", INC_THREAD_CPU_STAT_9);

	return 0;
}

//function: Create CPU statistics structure and initialize basic items
//input:    None
//output:   return -1: initialization failed
//          return  0: initialization successful
int thread_cpu_stat::add_cpu_stat_object(const char *thread_name, int stat_idx)
{
	Thread *thread = Thread::FindThreadByName(thread_name);
	if (thread == NULL || thread->Pid() <= 0) {
		return -1;
	}

	one_thread_cpu_stat_t *new_cpu_stat = new one_thread_cpu_stat_t;
	if (!new_cpu_stat) {
		log4cplus_warning("thread create new cpu stat error");
		return -1;
	}

	new_cpu_stat->_next = _stat_list_head;
	_stat_list_head = new_cpu_stat;

	return new_cpu_stat->init(thread_name, stat_idx, thread->Pid());
}

//function: Initialize basic items
//input:    None
//output:   return -1: initialization failed
//          return  0: initialization successful
int one_thread_cpu_stat::init(const char *thread_name, int stat_idx, int pid)
{
	_pid = pid;
	_thread_name = thread_name;
	_last_time = 0;
	_this_time = 0;
	_last_cpu_time = 0;
	_this_cpu_time = 0;
	_pcpu = g_stat_mgr.get_stat_int_counter(stat_idx);
	_pcpu = 100;
	_err_times = 0;
	_status = CPU_STAT_INVALID;

	int mainpid = getpid();
	char pro_stat_file[256];
	snprintf(pro_stat_file, 255, "/proc/%d/job/%d/stat", mainpid, _pid);
	_fd = open(pro_stat_file, O_RDONLY);
	if (_fd < 0) {
		log4cplus_warning("open proc stat of thread %d failed", _pid);
	} else {
		int ret = read_cpu_time();
		if (ret < 0) {
			_status = CPU_STAT_NOT_INIT;
			log4cplus_warning("thread [%d] cpu stat init error",
					  _pid);
		} else {
			_last_cpu_time = _this_cpu_time;
			_last_time = _this_time;
			_status = CPU_STAT_INIT;
		}
	}

	return 0;
}

static inline void skip_word(char **p)
{
	*p = strchr(*p, ' ');
	*p = *p + 1;
}

int one_thread_cpu_stat::read_cpu_time(void)
{
	int ret = lseek(_fd, 0, SEEK_SET);
	if (ret < 0) {
		log4cplus_warning("lseek proc file to begain failed");
		return -1;
	}

	char stat_buffer[1024];
	ret = read(_fd, stat_buffer, 1023);
	if (ret < 0) {
		log4cplus_warning("proc file read errro");
		return -1;
	}

	char *p = stat_buffer;
	skip_word(&p); /*skip pid*/
	skip_word(&p); /*skip cmd name*/
	skip_word(&p); /*skip status*/
	skip_word(&p); /*skip ppid*/
	skip_word(&p); /*skip pgrp*/
	skip_word(&p); /*skip session*/
	skip_word(&p); /*skip tty*/
	skip_word(&p); /*skip tty pgrp*/
	skip_word(&p); /*skip flags*/
	skip_word(&p); /*skip min flt*/
	skip_word(&p); /*skip cmin flt*/
	skip_word(&p); /*skip maj flt*/
	skip_word(&p); /*skip cmaj flt*/

	unsigned long long utime = 0;
	unsigned long long stime = 0;

	char *q = strchr(p, ' ');
	*q = 0;
	utime = atoll(p);
	*q = ' ';

	skip_word(&p); /*skip utime*/

	q = strchr(p, ' ');
	*q = 0;
	stime = atoll(p);
	*q = ' ';

	_this_cpu_time = utime + stime;

	struct timeval now;
	gettimeofday(&now, NULL);
	_this_time = now.tv_sec + now.tv_usec * 1e-6;

	return 0;
}

void one_thread_cpu_stat::cal_pcpu(void)
{
	double time_diff = _this_time - _last_time;
	time_diff *= HZ;

	if (time_diff > 0) {
		_pcpu = (int)(10000 *
			      ((_this_cpu_time - _last_cpu_time) / time_diff));
	}
	//else, keep previous cpu percentage

	_last_cpu_time = _this_cpu_time;
	_last_time = _this_time;
	return;
}

#define READ_CPU_TIME_ERROR                                                    \
	log4cplus_warning("thread %s[%d] read cpu time error", _thread_name,   \
			  _pid)

void one_thread_cpu_stat::do_stat()
{
	int ret = 0;
	switch (_status) {
	case CPU_STAT_INVALID:
		break;

	case CPU_STAT_NOT_INIT:
		ret = read_cpu_time();
		if (ret < 0) {
			READ_CPU_TIME_ERROR;
		} else {
			_last_cpu_time = _this_cpu_time;
			_last_time = _this_time;
			_status = CPU_STAT_INIT;
		}
		break;

	case CPU_STAT_INIT:
		ret = read_cpu_time();
		if (ret < 0) {
			READ_CPU_TIME_ERROR;
		} else {
			cal_pcpu();
			_err_times = 0;
			_status = CPU_STAT_FINE;
		}
		break;

	case CPU_STAT_FINE:
		ret = read_cpu_time();
		if (ret < 0) {
			READ_CPU_TIME_ERROR;
			_err_times++;
			if (_err_times >= THREAD_CPU_STAT_MAX_ERR) {
				log4cplus_warning(
					"max err, transfer to ill status");
				_pcpu = 10000;
				_status = CPU_STAT_ILL;
			}
		} else {
			cal_pcpu();
			_err_times = 0;
		}
		break;
	case CPU_STAT_ILL:
		ret = read_cpu_time();
		if (ret < 0) {
			READ_CPU_TIME_ERROR;
		} else {
			cal_pcpu();
			_err_times = 0;
			_status = CPU_STAT_FINE;
		}
		break;
	default:
		log4cplus_warning("unkown cpu stat status");
		break;
	} //end of this thread statistic

	return;
}

void thread_cpu_stat::do_stat()
{
	one_thread_cpu_stat_t *cpu_stat = _stat_list_head;
	while (cpu_stat) {
		cpu_stat->do_stat();
		cpu_stat = cpu_stat->_next;
	} //end of all thread statistic

	return;
}