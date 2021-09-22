/*
* Copyright [2021] JD.com, Inc.
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
#ifndef __HELPER_GROUP_H__
#define __HELPER_GROUP_H__

#include "queue/lqueue.h"
#include "../poll/poller_base.h"

#include "list/list.h"
#include "value.h"
#include "request/request_base.h"
#include "stat_dtc.h"

class ConnectorClient;
class HelperClientList;
class DTCJobOperation;

class ConnectorGroup : private TimerObject,
		       public JobAskInterface<DTCJobOperation> {
    public:
	ConnectorGroup(const char *sockpath, const char *name, int hc, int qs,
		       int statIndex);
	~ConnectorGroup();

	int queue_full(void) const
	{
		return queue.Count() >= queueSize;
	}
	int queue_empty(void) const
	{
		return queue.Count() == 0;
	}
	int has_free_helper(void) const
	{
		return queue.Count() == 0 && freeHelper.ListEmpty();
	}

	/* process or queue a job */
	virtual void job_ask_procedure(DTCJobOperation *);

	int do_attach(PollerBase *, LinkQueue<DTCJobOperation *>::allocator *);
	int connect_helper(int);
	const char *sock_path(void) const
	{
		return sockpath;
	}

	void set_timer_handler(TimerList *recv, TimerList *conn,
			       TimerList *retry)
	{
		recvList = recv;
		connList = conn;
		retryList = retry;
		attach_timer(retryList);
	}

	void queue_back_task(DTCJobOperation *);
	void request_completed(ConnectorClient *);
	void connection_reset(ConnectorClient *);
	void check_queue_expire(void);
	void dump_state(void);

	void add_ready_helper();
	void dec_ready_helper();

    private:
	virtual void job_timer_procedure(void);
	/* trying pop job and process */
	void flush_task(uint64_t time);
	/* process a job, must has free helper */
	void process_task(DTCJobOperation *t);
	const char *get_name() const
	{
		return name;
	}
	void record_response_delay(unsigned int t);
	int accept_new_request_fail(DTCJobOperation *);
	void group_notify_helper_reload_config(DTCJobOperation *job);
	void process_reload_config(DTCJobOperation *job);

    public:
	TimerList *recvList;
	TimerList *connList;
	TimerList *retryList;

    private:
	LinkQueue<DTCJobOperation *> queue;
	int queueSize;

	int helperCount;
	int helperMax;
	int readyHelperCnt;
	char *sockpath;
	char name[24];

	HelperClientList *helperList;
	ListObject<HelperClientList> freeHelper;

    public:
	ConnectorGroup *fallback;

    public:
	void record_process_time(int type, unsigned int msec);
	/* queue当前长度 */
	int queue_count(void) const
	{
		return queue.Count();
	}
	/* queue最大长度*/
	int queue_max_count(void) const
	{
		return queueSize;
	}

    private:
	/* 平均请求时延 */
	double average_delay;

	StatSample statTime[6];
};

#endif
