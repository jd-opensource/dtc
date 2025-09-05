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
#ifndef LOCK_FREE_QUEUE_H
#define LOCK_FREE_QUEUE_H

#include <stdint.h>

#define LOCK_FREE_QUEUE_DEFAULT_SIZE 65536
#define CAS(a_ptr, a_oldVal, a_newVal)                                         \
	__sync_bool_compare_and_swap(a_ptr, a_oldVal, a_newVal)

template <typename ELEM_T, uint32_t Q_SIZE = LOCK_FREE_QUEUE_DEFAULT_SIZE>
class LockFreeQueue {
    public:
	LockFreeQueue();
	~LockFreeQueue();
	bool en_queue(const ELEM_T &data);
	bool de_queue(ELEM_T &data);
	uint32_t Size();
	uint32_t queue_size();

    private:
	ELEM_T *m_theQueue;
	uint32_t m_queueSize;
	volatile uint32_t m_readIndex;
	volatile uint32_t m_writeIndex;
	volatile uint32_t m_maximumReadIndex;
	inline uint32_t count_to_index(uint32_t count);
};

#include "LockFreeQueue_Imp.h"

#endif
