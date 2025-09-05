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
#ifndef __PIPE_MTQUEUE_H__
#define __PIPE_MTQUEUE_H__

#include <unistd.h>
#include <pthread.h>
#include "poll/poller.h"
#include "queue/lqueue.h"
#include "log/log.h"

// typename T must be simple data type with 1,2,4,8,16 bytes
template <typename T, typename C> class ThreadingPipeQueue : EpollBase {
    private:
	typename LinkQueue<T>::allocator alloc;
	LinkQueue<T> queue;
	pthread_mutex_t lock;
	int wakefd;

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

	// pipe management
	inline void Wake()
	{
		char c = 0;
		ssize_t result = write(wakefd, &c, 1);
		(void)result; // Silence unused result warning
	}
	inline void Discard()
	{
		char buf[256];
		int n;
		do {
			n = read(netfd, buf, sizeof(buf));
		} while (n > 0);
	}

	// reader implementation
	virtual void hangup_notify(void)
	{
	}
	virtual void input_notify(void)
	{
		log4cplus_debug("enter input_notify.");
		T p;
		int n = 0;
		Lock();
		while (++n <= 64 && queue.Count() > 0) {
			p = queue.Pop();
			Unlock();
			// running job in unlocked mode
			static_cast<C *>(this)->job_ask_procedure(p);
			Lock();
		}
		if (queue.Count() <= 0)
			Discard();
		Unlock();
		log4cplus_debug("leave input_notify.");
	}

    public:
	ThreadingPipeQueue() : queue(&alloc), wakefd(-1)
	{
		pthread_mutex_init(&lock, NULL);
	}
	~ThreadingPipeQueue()
	{
		pthread_mutex_destroy(&lock);
	}
	inline int attach_poller(EpollOperation *thread)
	{
		int fd[2];
		int ret = pipe(fd);
		if (ret != 0)
			return ret;

		wakefd = fd[1];
		netfd = fd[0];
		enable_input();
		return EpollBase::attach_poller(thread);
	}
	inline int Push(T p)
	{
		int qsz;
		int ret;

		Lock();
		qsz = queue.Count();
		ret = queue.Push(p);
		Unlock();
		if (qsz == 0)
			Wake();
		return ret;
	}
	inline int Unshift(T p)
	{
		int qsz;
		int ret;

		Lock();
		qsz = queue.Count();
		ret = queue.Unshift(p);
		Unlock();
		if (qsz == 0)
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
