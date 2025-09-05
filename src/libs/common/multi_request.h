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
#ifndef __H_MULTI_REQUEST_H__
#define __H_MULTI_REQUEST_H__

#include "request/request_base_all.h"

class JobHubAskChain;
class DTCJobOperation;
union DTCValue;

class MultiRequest {
    private:
	JobHubAskChain *owner;
	DTCJobOperation *wait;
	DTCValue *keyList;
	unsigned char *keyMask;
	int doneReq;
	int totalReq;
	int subReq;
	int firstPass;
	int keyFields;
	int internal;

    public:
	friend class JobHubAskChain;
	MultiRequest(JobHubAskChain *o, DTCJobOperation *job);
	~MultiRequest(void);

	int decode_key_list(void);
	int split_task(void);
	int total_count(void) const
	{
		return totalReq;
	}
	int remain_count(void) const
	{
		return totalReq - doneReq;
	}
	void second_pass(int err);
	void complete_task(DTCJobOperation *req, int index);

    public:
	DTCValue *get_key_value(int i);
	void set_key_completed(int i);
	int is_key_completed(int i);
	void complete_waiter(void);
};

class MultiTaskReply : public JobAnswerInterface<DTCJobOperation> {
    public:
	MultiTaskReply()
	{
	}
	virtual void job_answer_procedure(DTCJobOperation *job_operation);
};

#endif
