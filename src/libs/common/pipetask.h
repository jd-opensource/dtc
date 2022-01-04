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
#ifndef __H_DTC_PIPETASK_TEMP_H__
#define __H_DTC_PIPETASK_TEMP_H__

#include "log/log.h"
#include "pipequeue.h"
#include "compiler.h"

template <typename T> class JobAskInterface;
template <typename T> class JobAnswerInterface;
template <typename T> class ChainJoint;

template <typename T>
class TaskIncomingPipe : public PipeQueue<T, TaskIncomingPipe<T> > {
    public:
	TaskIncomingPipe(void)
	{
	}
	virtual ~TaskIncomingPipe()
	{
	}
	inline void job_ask_procedure(T p)
	{
		proc->job_ask_procedure(p);
	}

    public:
	JobAskBase<T> *proc;
};

template <typename T>
class TaskReturnPipe : public PipeQueue<T *, TaskReturnPipe<T> > {
    public:
	TaskReturnPipe(){};
	virtual ~TaskReturnPipe(){};
	inline void job_ask_procedure(T *p)
	{
		p->turn_around_job_answer();
	}
};

template <typename T>
class JobTunnel : public JobAskInterface<T>, public JobAnswerInterface<T> {
    private:
	static LinkQueue<JobTunnel<T> *> pipelist;

    public:
	inline JobTunnel()
	{
		pipelist.Push(this);
	}
	~JobTunnel()
	{
	}

	virtual void job_ask_procedure(T *p)
	{
		p->push_reply_dispatcher(this);
		incQueue.Push(p);
	}
	virtual void job_answer_procedure(T *p)
	{
		retQueue.Push(p);
	}
	inline int dig_tunnel(ChainJoint<T> *from, JobAskInterface<T> *to)
	{
		JobAskInterface<T>::owner = from->get_owner_thread();

		incQueue.attach_poller(from->get_owner_thread(),
				       to->get_owner_thread());
		retQueue.attach_poller(to->get_owner_thread(),
				       from->get_owner_thread());

		from->register_next_chain(this);
		incQueue.proc = to;
		from->disable_use_queue();
		return 0;
	}

	static inline void destroy_all(void)
	{
		JobTunnel *p;
		while ((p = pipelist.Pop()) != NULL) {
			delete p;
		}
	}

    private:
	TaskIncomingPipe<T *> incQueue;
	TaskReturnPipe<T> retQueue;
};

template <typename T> LinkQueue<JobTunnel<T> *> JobTunnel<T>::pipelist;

#endif
