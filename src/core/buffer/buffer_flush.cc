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

#include "buffer_flush.h"
#include "buffer_process_ask_chain.h"
#include "global.h"

DTCFlushRequest::DTCFlushRequest(BufferProcessAskChain *o, const char *key)
	: owner(o), numReq(0), badReq(0), wait(NULL)
{
}

DTCFlushRequest::~DTCFlushRequest()
{
	if (wait) {
		wait->turn_around_job_answer();
		wait = NULL;
	}
}

class DropDataReply : public JobAnswerInterface<DTCJobOperation> {
    public:
	DropDataReply()
	{
	}
	virtual void job_answer_procedure(DTCJobOperation *job_operation);
};

void DropDataReply::job_answer_procedure(DTCJobOperation *job_operation)
{
	DTCFlushRequest *req = job_operation->OwnerInfo<DTCFlushRequest>();
	if (req == NULL)
		delete job_operation;
	else
		req->complete_row(job_operation, job_operation->owner_index());
}

static DropDataReply dropReply;

int DTCFlushRequest::flush_row(const RowValue &row)
{
	DTCJobOperation *pJob = new DTCJobOperation;
	if (pJob == NULL) {
		log4cplus_error(
			"cannot flush row, new job error, possible memory exhausted\n");
		return -1;
	}

	if (pJob->Copy(row) < 0) {
		log4cplus_error("cannot flush row, from: %s   error: %s \n",
				pJob->resultInfo.error_from(),
				pJob->resultInfo.error_message());
		return -1;
	}
	pJob->set_request_type(TaskTypeCommit);
	pJob->push_reply_dispatcher(&dropReply);
	pJob->set_owner_info(this, numReq, NULL);
	owner->inc_async_flush_stat();
	//TaskTypeCommit never expired
	//pJob->set_expire_time(3600*1000/*ms*/);
	numReq++;
	owner->push_flush_queue(pJob);
	return 0;
}

void DTCFlushRequest::complete_row(DTCJobOperation *req, int index)
{
	delete req;
	numReq--;
	if (numReq == 0) {
		if (wait) {
			wait->turn_around_job_answer();
			wait = NULL;
		}
		owner->complete_flush_request(this);
	}
}
