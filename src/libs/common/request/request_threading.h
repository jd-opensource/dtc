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
#ifndef __H_DTC_REQUEST_THREAD_H__
#define __H_DTC_REQUEST_THREAD_H__

#include "queue/mtpqueue.h"
#include "queue/wait_queue.h"
#include "../request/request_base_all.h"

template <typename T> class ThreadingOutputDispatcher {
    private: // internal implementation
	class InternalTaskDispatcher
		: public ThreadingPipeQueue<T *, InternalTaskDispatcher> {
	    public:
		JobAskInterface<T> *proc;

	    public:
		InternalTaskDispatcher() : proc(0)
		{
		}
		virtual ~InternalTaskDispatcher()
		{
		}
		void job_ask_procedure(T *p)
		{
			proc->job_ask_procedure(p);
		};
	};

	class InternalReplyDispatcher : public JobAnswerInterface<T>,
					public threading_wait_queue<T *> {
	    public:
		InternalReplyDispatcher *freenext;
		InternalReplyDispatcher *allnext;

	    public:
		InternalReplyDispatcher() : freenext(0), allnext(0)
		{
		}
		virtual ~InternalReplyDispatcher()
		{
		}
		virtual void job_answer_procedure(T *p)
		{
			this->Push(p);
		};
	};

    private:
	InternalTaskDispatcher incQueue;
	pthread_mutex_t lock;
	// lock management, protect free_list and allList
	inline void Lock(void)
	{
		pthread_mutex_lock(&lock);
	}
	inline void Unlock(void)
	{
		pthread_mutex_unlock(&lock);
	}
	InternalReplyDispatcher *volatile free_list;
	InternalReplyDispatcher *volatile allList;
	volatile int stopping;

    public:
	ThreadingOutputDispatcher() : free_list(0), allList(0), stopping(0)
	{
		pthread_mutex_init(&lock, NULL);
	}
	~ThreadingOutputDispatcher()
	{
		InternalReplyDispatcher *q;
		while (allList) {
			q = allList;
			allList = q->allnext;
			delete q;
		}
		pthread_mutex_destroy(&lock);
	}

	void Stop(void)
	{
		stopping = 1;
		InternalReplyDispatcher *p;
		for (p = allList; p; p = p->allnext)
			p->Stop(NULL);
	}

	int Stopping(void)
	{
		return stopping;
	}

	int do_execute(T *p)
	{
		InternalReplyDispatcher *q;

		// aborted without side-effect
		if (Stopping())
			return -1;

		/* freelist was set to NULL by other threads when locked by other threads */
		Lock();
		if (free_list) {
			q = free_list;
			free_list = q->freenext;
			q->Clear();
			q->freenext = NULL;
		} else {
			q = new InternalReplyDispatcher();
			q->allnext = allList;
			allList = q;
		}
		Unlock();

		p->push_reply_dispatcher(q);
		incQueue.Push(p);
		// has side effect
		if (q->Pop() == NULL) {
			// q leaked by purpose
			// because an pending reply didn't Popped
			return -2;
		}

		Lock();
		q->freenext = free_list;
		free_list = q;
		Unlock();
		return 0;
	}

	int register_next_chain(JobAskInterface<T> *to)
	{
		log4cplus_debug("Create register_next_chain to thread %s",
				to->get_owner_thread()->Name());
		incQueue.proc = to;
		return incQueue.attach_poller(to->get_owner_thread());
	}
};

#endif
