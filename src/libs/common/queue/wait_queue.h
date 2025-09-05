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
#ifndef __WAIT_MTQUEUE_H__
#define __WAIT_MTQUEUE_H__

#include <pthread.h>
#include "queue/lqueue.h"

// typename T must be simple data type with 1,2,4,8,16 bytes
template <typename T> class threading_wait_queue {
    private:
	typename LinkQueue<T>::allocator alloc;
	LinkQueue<T> queue;
	pthread_mutex_t lock;
	pthread_cond_t wait;
	volatile int stopping;
	T stopval;

    private:
	// lock management
	inline void Lock(void)
	{
		pthread_mutex_lock(&lock);
	}
	inline void Unlock(void)
	{
		pthread_mutex_unlock(&lock);
	}

	// queue management
	inline void Wait(void)
	{
		pthread_cond_wait(&wait, &lock);
	}
	inline void Wake(void)
	{
		pthread_cond_signal(&wait);
	}
	inline void WakeAll(void)
	{
		pthread_cond_broadcast(&wait);
	}

    public:
	inline threading_wait_queue() : queue(&alloc)
	{
		pthread_mutex_init(&lock, NULL);
		pthread_cond_init(&wait, NULL);
		stopping = 0;
	}
	inline ~threading_wait_queue()
	{
		pthread_cond_destroy(&wait);
		pthread_mutex_destroy(&lock);
	}

	inline int Stopping(void)
	{
		return stopping;
	}
	inline void Stop(T val = 0)
	{
		stopping = 1;
		stopval = val;
		WakeAll();
	}
	inline void Clear(void)
	{
		Lock();
		while (queue.Count() > 0) {
			queue.Pop();
		}
		Unlock();
	}
	inline T Pop(void)
	{
		T p;
		Lock();
		while (!Stopping() && queue.Count() <= 0) {
			Wait();
		}
		p = Stopping() ? stopval : queue.Pop();
		Unlock();
		return p;
	}
	inline int Push(T p)
	{
		int ret;

		Lock();
		ret = queue.Push(p);
		Unlock();
		Wake();
		return ret;
	}
	inline int Unshift(T p)
	{
		int ret;

		Lock();
		ret = queue.Push(p);
		Unlock();
		Wake();
		return ret;
	}
	inline int Count(void)
	{
		int qsz;

		Lock();
		qsz = queue.Count();
		Unlock();
		return qsz;
	}
	inline int queue_empty(void)
	{
		return Count() == 0;
	}
};

#endif
