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
#include "multi_request.h"
#include "task/task_request.h"
#include "task/task_multi_unit.h"
#include "key_list.h"
#include "mem_check.h"

static MultiTaskReply multiTaskReply;

MultiRequest::MultiRequest(JobHubAskChain *o, DTCJobOperation *job)
	: owner(o), wait(job), keyList(NULL), keyMask(NULL), doneReq(0),
	  totalReq(0), subReq(0), firstPass(1), keyFields(0), internal(0)
{
}

MultiRequest::~MultiRequest()
{
	if (wait) {
		wait->turn_around_job_answer();
		wait = NULL;
	}
	if (internal == 0)
		FREE_IF(keyList);
	FREE_IF(keyMask);
}

void MultiTaskReply::job_answer_procedure(DTCJobOperation *job_operation)
{
	MultiRequest *req = job_operation->OwnerInfo<MultiRequest>();
	if (req == NULL)
		delete job_operation;
	else
		req->complete_task(job_operation, job_operation->owner_index());
}

DTCValue *MultiRequest::get_key_value(int i)
{
	return &keyList[i * keyFields];
}

void MultiRequest::set_key_completed(int i)
{
	FD_SET(i, (fd_set *)keyMask);
	doneReq++;
}

int MultiRequest::is_key_completed(int i)
{
	return FD_ISSET(i, (fd_set *)keyMask);
}

int MultiRequest::decode_key_list(void)
{
	if (!wait->flag_multi_key_val()) // single job
		return 0;

	const DTCTableDefinition *table_definition_ = wait->table_definition();

	keyFields = table_definition_->key_fields();
	if (wait->internal_key_val_list()) {
		// embeded API
		totalReq = wait->internal_key_val_list()->KeyCount();
		// Q&D discard const here
		// this keyList member can't be const,
		// but actually readonly after init
		keyList =
			(DTCValue *)&wait->internal_key_val_list()->Value(0, 0);
		internal = 1;
	} else {
		// from network
		uint8_t fieldID[keyFields];
		Array keyNameList(*(wait->key_name_list()));
		Array keyValList(*(wait->key_val_list()));
		DTCBinary keyName;
		for (int i = 0; i < keyFields; i++) {
			if (keyNameList.Get(keyName) != 0) {
				log4cplus_error(
					"get key name[%d] error, key field count:%d",
					i, table_definition_->key_fields());
				return -1;
			}
			fieldID[i] = table_definition_->field_id(keyName.ptr);
		}
		if (keyNameList.Get(keyName) == 0) {
			log4cplus_error("bogus key name: %.*s", keyName.len,
					keyName.ptr);
			return -1;
		}

		totalReq = wait->versionInfo.get_tag(11)->u64;
		keyList = (DTCValue *)MALLOC(totalReq * keyFields *
					     sizeof(DTCValue));
		for (int i = 0; i < totalReq; i++) {
			DTCValue *keyVal = get_key_value(i);
			for (int j = 0; j < keyFields; j++) {
				int fid = fieldID[j];
				switch (table_definition_->field_type(fid)) {
				case DField::Signed:
				case DField::Unsigned:
					if (keyValList.Get(keyVal[fid].u64) !=
					    0) {
						log4cplus_error(
							"get key value[%d][%d] error",
							i, j);
						return -2;
					}
					break;

				case DField::String:
				case DField::Binary:
					if (keyValList.Get(keyVal[fid].bin) !=
					    0) {
						log4cplus_error(
							"get key value[%d][%d] error",
							i, j);
						return -2;
					}
					break;

				default:
					log4cplus_error(
						"invalid key type[%d][%d]", i,
						j);
					return -3;
				}
			}
		}
	}

	//	keyMask = (unsigned char *)CALLOC(1, (totalReq*keyFields+7)/8);
	//	8 bytes aligned Awaste some memory. FD_SET operate memory by 8bytes
	keyMask = (unsigned char *)CALLOC(
		8, (((totalReq * keyFields + 7) / 8) + 7) / 8);
	return totalReq;
}

int MultiRequest::split_task(void)
{
	log4cplus_debug("split_task begin, totalReq: %d", totalReq);
	for (int i = 0; i < totalReq; i++) {
		if (is_key_completed(i))
			continue;

		DTCValue *keyVal = get_key_value(i);
		DTCJobOperation *pJob = new DTCJobOperation;
		if (pJob == NULL) {
			log4cplus_error("%s: %m", "new job error");
			return -1;
		}
		if (pJob->Copy(*wait, keyVal) < 0) {
			log4cplus_error("copy job error: %s",
					pJob->resultInfo.error_message());
			delete pJob;
			return -1;
		}

		pJob->set_owner_info(this, i, wait->OwnerAddress());
		pJob->push_reply_dispatcher(&multiTaskReply);
		owner->push_task_queue(pJob);
		subReq++;
	}

	log4cplus_debug("split_task end, subReq: %d", subReq);
	return 0;
}

void MultiRequest::complete_task(DTCJobOperation *req, int index)
{
	log4cplus_debug("MultiRequest::complete_task start, index: %d", index);
	if (wait) {
		if (wait->result_code() >= 0 && req->result_code() < 0) {
			wait->set_error_dup(req->resultInfo.result_code(),
					    req->resultInfo.error_from(),
					    req->resultInfo.error_message());
		}

		int ret;
		if ((ret = wait->merge_result(*req)) != 0) {
			wait->set_error(ret, "multi_request",
					"merge result error");
		}
	}

	delete req;

	set_key_completed(index);
	subReq--;

	// Note: If CTaskMultiplexer is placed in cache thread execution, it will cause each split task to go directly to cache_process and execute here; then split the second task. This will cause problems with this judgment logic.
	// Currently CTaskMultiplexer is bound to the incoming thread, so there's no problem
	if (firstPass == 0 && subReq == 0) {
		complete_waiter();
		delete this;
	}
	log4cplus_debug("MultiRequest::complete_task end, subReq: %d", subReq);
}

void MultiRequest::complete_waiter(void)
{
	if (wait) {
		wait->turn_around_job_answer();
		wait = 0;
	}
}

void MultiRequest::second_pass(int err)
{
	firstPass = 0;
	if (subReq == 0) {
		// no sub-request present, complete whole request
		complete_waiter();
		delete this;
	} else if (err) {
		// mark all request is done except sub-requests
		doneReq = totalReq - subReq;
		complete_waiter();
	}
	return;
}

int DTCJobOperation::set_batch_cursor(int index)
{
	int err = 0;

	MultiRequest *mreq = get_batch_key_list();
	if (mreq == NULL)
		return -1;

	if (index < 0 || index >= mreq->total_count()) {
		key = NULL;
		multi_key = NULL;
		return -1;
	} else {
		DTCValue *keyVal = mreq->get_key_value(index);
		int kf = table_definition()->key_fields();

		/* switch request_key() */
		key = &keyVal[0];
		if (kf > 1) {
			/* switch multi-fields key */
			multi_key = keyVal;
		}
		err = build_packed_key();
		if (err < 0) {
			log4cplus_error(
				"build packed key error, error from: %s, error message: %s",
				resultInfo.error_from(),
				resultInfo.error_message());
			return -1;
		}
	}
	return 0;
}

void DTCJobOperation::done_batch_cursor(int index)
{
	MultiRequest *mreq = get_batch_key_list();
	if (mreq == NULL)
		return;

	mreq->set_key_completed(index);
}
