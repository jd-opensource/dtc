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

#ifndef __H_CACHE_FLUSH_H__
#define __H_CACHE_FLUSH_H__

#include "timer/timer_list.h"
#include "queue/lqueue.h"
#include "task/task_request.h"
#include "buffer_process_ask_chain.h"
#include "log/log.h"

class BufferProcessAskChain;

class DTCFlushRequest {
    private:
	BufferProcessAskChain *owner;
	int numReq;
	int badReq;
	DTCJobOperation *wait;

    public:
	friend class BufferProcessAskChain;
	DTCFlushRequest(BufferProcessAskChain *, const char *key);
	~DTCFlushRequest(void);

	const DTCTableDefinition *table_definition(void) const
	{
		return owner->table_definition();
	}

	int flush_row(const RowValue &);
	void complete_row(DTCJobOperation *req, int index);
	int Count(void) const
	{
		return numReq;
	}
};

#endif
