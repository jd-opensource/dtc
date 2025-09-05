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

#ifndef __BARRIER_QUEUE_H__
#define __BARRIER_QUEUE_H__

#include <list/list.h>
#include <queue/lqueue.h>

class DTCJobOperation;
class BarrierAskAnswerChain;
class BarrierQueue;

class BarrierQueue : public ListObject<BarrierQueue>,
		     public LinkQueue<DTCJobOperation *> {
    public:
	friend class BarrierAskAnswerChain;

	inline BarrierQueue(LinkQueue<DTCJobOperation *>::allocator *a = NULL)
		: LinkQueue<DTCJobOperation *>(a), key_(0)
	{
	}
	inline ~BarrierQueue(){};

	inline unsigned long key() const
	{
		return key_;
	}
	inline void set_key(unsigned long k)
	{
		key_ = k;
	}

    private:
	unsigned long key_;
};

#endif
