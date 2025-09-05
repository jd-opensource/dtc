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

#ifndef __BUFFER_PROCESS_ANSWER_CHAIN__
#define __BUFFER_PROCESS_ANSWER_CHAIN__

#include <sys/mman.h>
#include <time.h>

#include "protocol.h"
#include "value.h"
#include "field/field.h"
#include "section.h"
#include "table/table_def.h"
#include "task/task_request.h"
#include "list/list.h"
#include "fence_queue.h"
#include "buffer_pond.h"
#include "poll/poller_base.h"
#include "config/dbconfig.h"
#include "queue/lqueue.h"
#include "stat_dtc.h"
#include "data_process.h"
#include "empty_filter.h"
#include "namespace.h"
#include "task_pendlist.h"
#include "data_chunk.h"
#include "hb_log.h"
#include "lru_bit.h"
#include "hb_feature.h"
#include "blacklist/blacklist_unit.h"
#include "expire_time.h"

DTC_BEGIN_NAMESPACE

class DTCFlushRequest;
class BufferProcessAskChain;
class DTCTableDefinition;
class TaskPendingList;

class BufferProcessAnswerChain : public JobAnswerInterface<DTCJobOperation> {
    private:
	BufferProcessAskChain *buffer_reply_notify_owner_;

    public:
	BufferProcessAnswerChain(BufferProcessAskChain *buffer_process)
		: buffer_reply_notify_owner_(buffer_process)
	{
	}

	virtual ~BufferProcessAnswerChain()
	{
	}

	virtual void job_answer_procedure(DTCJobOperation *);
};

DTC_END_NAMESPACE

#endif
