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

#ifndef __BARRIER_ASK_ANSWER_CHAIN__
#define __BARRIER_ASK_ANSWER_CHAIN__

#include <stdint.h>
#include <list/list.h>
#include "task/task_request.h"
#include "timer/timer_list.h"
#include "fence_queue.h"
#include "stat_dtc.h"

#define BARRIER_HASH_MAX 1024 * 8

class DTCJobOperation;
class PollerBase;
class BarrierAskAnswerChain;

class BarrierAskAnswerChain : public JobAskInterface<DTCJobOperation>,
			      public JobAnswerInterface<DTCJobOperation> {
    public:
	enum E_BARRIER_UNIT_PLACE { IN_FRONT, IN_BACK };
	BarrierAskAnswerChain(PollerBase *, int max, int maxkeycount,
			      E_BARRIER_UNIT_PLACE place);
	~BarrierAskAnswerChain();

	virtual void job_ask_procedure(DTCJobOperation *);
	virtual void job_answer_procedure(DTCJobOperation *);

	void chain_request(DTCJobOperation *p)
	{
		p->push_reply_dispatcher(this);
		main_chain.job_ask_procedure(p);
	}

	void queue_request(DTCJobOperation *p)
	{
		p->push_reply_dispatcher(this);
		main_chain.indirect_notify(p);
	}

	PollerBase *owner_thread(void) const
	{
		return owner;
	}
	void attach_free_barrier(BarrierQueue *);
	int max_count_by_key(void) const
	{
		return max_key_count_;
	}
	void register_next_chain(JobAskInterface<DTCJobOperation> *p)
	{
		main_chain.register_next_chain(p);
	}
	int barrier_count() const
	{
		return count;
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}

    protected:
	int count;
	LinkQueue<DTCJobOperation *>::allocator task_queue_allocator;
	ListObject<BarrierQueue> free_list;
	ListObject<BarrierQueue> hash_slot_[BARRIER_HASH_MAX];
	int max_barrier;

	BarrierQueue *get_barrier(unsigned long key);
	BarrierQueue *get_barrier_by_idx(unsigned long idx);
	int key2idx(unsigned long key)
	{
		return key % BARRIER_HASH_MAX;
	}

    private:
	int max_key_count_;

	ChainJoint<DTCJobOperation> main_chain;

	//stat
	StatCounter stat_barrier_count;
	StatCounter stat_barrier_max_task;
};

#endif
