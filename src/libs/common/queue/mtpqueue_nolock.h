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
#ifndef __PIPE_MTQUEUE_NOLOCK_H__
#define __PIPE_MTQUEUE_NOLOCK_H__

#include <unistd.h>
#include <pthread.h>
#include "poll/poller.h"
#include "queue/lqueue.h"
#include "lock_free_queue.h"
#include "log/log.h"

#define CAS(a_ptr, a_oldVal, a_newVal)                                         \
	__sync_bool_compare_and_swap(a_ptr, a_oldVal, a_newVal)

/*
 * Provided for business class inheritance
 */
class BaseTask {
    private:
	int syncflag;
	int cmd;

    public:
	BaseTask() : syncflag(0), cmd(0)
	{
		//do nothing
	}
	virtual ~BaseTask()
	{
		//do nothing
	}
	void set_sync_flag(int flag)
	{
		this->syncflag = flag;
	}
	int get_sync_flag()
	{
		return this->syncflag;
	}
	void set_cmd(int flag)
	{
		this->cmd = flag;
	}
	int get_cmd()
	{
		return this->cmd;
	}
};

template <typename C> class ThreadingPipeNoLockQueue : EpollBase {
    private:
	LockFreeQueue<BaseTask *> queue;
	int wakefd;
	int resp_wakefd;
	int resp_netfd;
	volatile uint32_t queueSize;

    private:
	// pipe management
	inline void Wake()
	{
		char c = 0;
		write(wakefd, &c, 1);
	}

	inline void Response()
	{
		char c = 0;
		write(resp_wakefd, &c, 1);
	}

	inline void Discard()
	{
		char buf[1];
		int n;
		n = read(netfd, buf, sizeof(buf));
		log4cplus_debug("the byte read from pipe is %d", n);
	}

	// reader implementation
	virtual void hangup_notify(void)
	{
	}
	virtual void input_notify(void)
	{
		log4cplus_debug("enter input_notify.");
		BaseTask *p;
		int n = 0;
		log4cplus_debug("in the input_notify the queue size is %d ",
				Count());
		while (++n <= 64 && queueSize > 0) {
			int ret = queue.de_queue(p);
			if (true == ret) {
				__sync_fetch_and_sub(&queueSize, 1);
				static_cast<C *>(this)->job_ask_procedure(p);
				if (p->get_sync_flag() != 0) {
					Response();
				}
			}
			// running job in unlocked mode
		}
		log4cplus_debug("in the input_notify the queue size is %d ",
				Count());
		if (queueSize <= 0) {
			log4cplus_debug("Discard been called %d", Count());
			Discard();
		}
		log4cplus_debug("leave input_notify.");
	}

    public:
	ThreadingPipeNoLockQueue() : wakefd(-1), resp_wakefd(-1), resp_netfd(-1)
	{
	}
	~ThreadingPipeNoLockQueue()
	{
	}

	inline int attach_poller(EpollOperation *thread)
	{
		int fd_response[2];
		int ret = pipe(fd_response);
		if (ret != 0)
			return ret;
		resp_wakefd = fd_response[1];
		resp_netfd = fd_response[0];

		int fd_send[2];
		ret = pipe(fd_send);
		if (ret != 0)
			return ret;

		wakefd = fd_send[1];
		netfd = fd_send[0];
		enable_input();
		return EpollBase::attach_poller(thread);
	}

	inline int Push(BaseTask *p)
	{
		uint32_t qsz;
		int ret;
		ret = queue.en_queue(p);
		if (true == ret) {
			qsz = __sync_fetch_and_add(&queueSize, 1);
			if (qsz == 0)
				Wake();
		}
		if (p->get_sync_flag() != 0) {
			char buf[1];
			int n;
			n = read(resp_netfd, buf, sizeof(buf));
			log4cplus_debug("resp from other thread!");
		}
		return ret;
	}

	inline int Count(void)
	{
		return queueSize;
	}

	inline int queue_empty(void)
	{
		return Count() == 0;
	}
};

#endif
