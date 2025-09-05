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
#ifndef __H_DTC_REQUEST_TEMP_H__
#define __H_DTC_REQUEST_TEMP_H__

#include <endian.h>
#include <stdlib.h>

#include "queue/lqueue.h"
#include "../log/log.h"
#include "../timer/timer_list.h"

template <typename T> class JobTunnel;

class PollerBase;

template <typename T> class JobAskBase {
    public:
	PollerBase *owner;

    public:
	inline JobAskBase(PollerBase *p = NULL) : owner(p)
	{
	}
	virtual inline ~JobAskBase()
	{
	}

	inline PollerBase *get_owner_thread(void)
	{
		return owner;
	}
	inline void attach_thread(PollerBase *thread)
	{
		if (owner == NULL)
			owner = thread;
	}

	virtual void job_ask_procedure(T) = 0;
};

template <typename T> class JobAskInterface : public JobAskBase<T *> {
    public:
	inline JobAskInterface(PollerBase *p = NULL) : JobAskBase<T *>(p)
	{
	}
	virtual inline ~JobAskInterface()
	{
	}
};

template <typename T> class JobAnswerInterface {
    public:
	inline JobAnswerInterface()
	{
	}
	virtual inline ~JobAnswerInterface()
	{
	}

	virtual void job_answer_procedure(T *) = 0;
};

template <typename T> class ChainJoint : private TimerObject {
    private:
	PollerBase *owner;
	JobAskInterface<T> *next_instance;
	LinkQueue<T *> queue;
	typename LinkQueue<T *>::allocator allocator;

    protected:
	bool use_queue;

    private:
	inline virtual void job_timer_procedure(void)
	{
		log4cplus_debug("enter timer procedure");
		T *p;
		while ((p = queue.Pop()) != NULL)
			job_ask_procedure(p);
		log4cplus_debug("leave timer procedure");
	}

    public:
	inline ChainJoint(PollerBase *o)
		: owner(o), next_instance(NULL), queue(&allocator),
		  use_queue(true)
	{
	}
	inline ~ChainJoint()
	{
	}
	inline PollerBase *get_owner_thread(void)
	{
		return owner;
	}
	void set_owner_thread(PollerBase* out_owner) {
		owner = out_owner;
	}
	inline void register_next_chain(JobAskInterface<T> *instance)
	{
		if (owner == instance->get_owner_thread()) {
			next_instance = instance;
		} else {
			JobTunnel<T> *job_tunnel = new JobTunnel<T>();
			job_tunnel->dig_tunnel(this, instance);
		}
	}
	inline void job_ask_procedure(T *p)
	{
		next_instance->job_ask_procedure(p);
	}
	inline void indirect_notify(T *p)
	{
		if (use_queue) {
			queue.Push(p);
			attach_ready_timer(reinterpret_cast<TimerUnit*>(owner));
		} else {
			job_ask_procedure(p);
		}
	}
	inline JobAskInterface<T> *get_next_chain_instance(void)
	{
		return next_instance;
	}
	inline void disable_use_queue(void)
	{
		use_queue = false;
	}
};

template <typename T, int max_count = 10> class TaskReplyList {
    public:
	JobAnswerInterface<T> *proc[max_count];
	int count;

    public:
	inline TaskReplyList(void)
	{
		count = 0;
	};
	inline virtual ~TaskReplyList(void){};
	inline void Clean()
	{
		memset(proc, 0, sizeof(JobAnswerInterface<T> *) * max_count);
		count = 0;
	}
	inline void copy_reply_path(TaskReplyList<T, 10> *org)
	{
		if (org && org->proc) {
			// Copy only the minimum between source and destination sizes
			int copy_count = (10 < max_count) ? 10 : max_count;
			memcpy(proc, org->proc,
			       sizeof(JobAnswerInterface<T> *) * copy_count);
		}
		count = org ? org->count : 0;
	}
	inline int Push(JobAnswerInterface<T> *p)
	{
		if (count >= max_count)
			return -1;
		proc[count++] = p;
		return 0;
	}
	inline JobAnswerInterface<T> *Pop(void)
	{
		return count == 0 ? NULL : proc[--count];
	}
	inline void Dump(void)
	{
		for (int i = 0; i < count; i++)
			log4cplus_debug("reply proc%d: %p", i, proc[i]);
	}
	inline void push_reply_dispatcher(JobAnswerInterface<T> *proc)
	{
		if (proc == NULL)
			static_cast<T *>(this)->Panic(
				"push_reply_dispatcher: dispatcher is NULL, check your code");
		else if (Push(proc) != 0)
			static_cast<T *>(this)->Panic(
				"push_reply_dispatcher: push queue failed, possible memory exhausted");
	}
	inline void turn_around_job_answer(void)
	{
		JobAnswerInterface<T> *proc = Pop();
		if (proc == NULL)
			static_cast<T *>(this)->Panic(
				"turn_around_job_answer: no more dispatcher, possible double reply");
		else
			proc->job_answer_procedure(static_cast<T *>(this));
	}
	void Panic(const char *msg);
};

template <typename T, int max_count>
void TaskReplyList<T, max_count>::Panic(const char *msg)
{
	log4cplus_error("Internal Error Encountered: %s", msg);
}

#endif
