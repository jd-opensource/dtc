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

#ifndef __TASK_REQUEST_PENDINGLIST_H
#define __TASK_REQUEST_PENDINGLIST_H

#include "timer/timer_list.h"
#include "namespace.h"
#include "task/task_request.h"
#include <list>

DTC_BEGIN_NAMESPACE
/*
 * 请求挂起列表。
 *
 * 如果发现请求暂时没法满足，则挂起，直到
 *     1. 超时
 *     2. 条件满足被唤醒
 */
class BufferProcessAskChain;
class CacheBase;
class TaskReqeust;
class TimerObject;
class TaskPendingList : private TimerObject {
    public:
	TaskPendingList(JobAskInterface<DTCJobOperation> *o, int timeout = 5);
	~TaskPendingList();

	void add2_list(DTCJobOperation *); //加入pending list
	void Wakeup(void); //唤醒队列中的所有task

    private:
	virtual void job_timer_procedure(void);

    private:
	TaskPendingList(const TaskPendingList &);
	const TaskPendingList &operator=(const TaskPendingList &);

    private:
	int _timeout;
	TimerList *_timelist;
	JobAskInterface<DTCJobOperation> *_owner;
	int _wakeup;
	typedef std::pair<DTCJobOperation *, time_t> slot_t;
	std::list<slot_t> _pendlist;
};

DTC_END_NAMESPACE

#endif
