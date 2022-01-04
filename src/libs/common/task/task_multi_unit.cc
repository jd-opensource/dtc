/*
* Copyright [2021] JD.com, Inc.
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
#include "task/task_request.h"
#include "task/task_multi_unit.h"
#include "log/log.h"

class ReplyMultiplexer : public JobAnswerInterface<DTCJobOperation> {
    public:
	ReplyMultiplexer(void)
	{
	}
	virtual ~ReplyMultiplexer(void);
	virtual void job_answer_procedure(DTCJobOperation *job);
};

ReplyMultiplexer::~ReplyMultiplexer(void)
{
}

void ReplyMultiplexer::job_answer_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("ReplyMultiplexer::job_answer_procedure start");
	MultiRequest *req = job_operation->get_batch_key_list();
	/* reset BatchKey state */
	job_operation->set_batch_cursor(-1);
	if (job_operation->result_code() < 0) {
		req->second_pass(-1);
	} else if (req->remain_count() <= 0) {
		req->second_pass(0);
	} else if (req->split_task() != 0) {
		log4cplus_error("split job error");
		job_operation->set_error(-ENOMEM, __FUNCTION__,
					 "split job error");
		req->second_pass(-1);
	} else {
		req->second_pass(0);
	}
	log4cplus_debug("ReplyMultiplexer::job_answer_procedure end");
}

static ReplyMultiplexer replyMultiplexer;

JobHubAskChain::~JobHubAskChain(void)
{
}

void JobHubAskChain::job_ask_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("JobHubAskChain enter job_ask_procedure");
	log4cplus_debug("multi key flag: %d",
			job_operation->flag_multi_key_val());

	if (!job_operation->flag_multi_key_val()) {
		// single job, no dispatcher needed
		main_chain.job_ask_procedure(job_operation);
		return;
	}

	switch (job_operation->request_code()) {
	case DRequest::Insert:
	case DRequest::Replace:
		if (job_operation->table_definition()->has_auto_increment()) {
			log4cplus_error(
				"table has autoincrement field, multi-insert/replace not support");
			job_operation->set_error(
				-EC_TOO_MANY_KEY_VALUE, __FUNCTION__,
				"table has autoincrement field, multi-insert/replace not support");
			job_operation->turn_around_job_answer();
			return;
		}
		break;

	case DRequest::Get:
		if (job_operation->requestInfo.limit_start() != 0 ||
		    job_operation->requestInfo.limit_count() != 0) {
			log4cplus_error("multi-job not support limit()");
			job_operation->set_error(
				-EC_TOO_MANY_KEY_VALUE, __FUNCTION__,
				"multi-job not support limit()");
			job_operation->turn_around_job_answer();
			return;
		}
	case DRequest::Delete:
	case DRequest::Purge:
	case DRequest::Update:
	case DRequest::Flush:
		break;

	default:
		log4cplus_error(
			"multi-job not support other than get/insert/update/delete/purge/replace/flush request");
		job_operation->set_error(-EC_TOO_MANY_KEY_VALUE, __FUNCTION__,
					 "bad batch request type");
		job_operation->turn_around_job_answer();
		return;
	}

	MultiRequest *req = new MultiRequest(this, job_operation);
	if (req == NULL) {
		log4cplus_error("new MultiRequest error: %m");
		job_operation->set_error(-ENOMEM, __FUNCTION__,
					 "new MultiRequest error");
		job_operation->turn_around_job_answer();
		return;
	}

	if (req->decode_key_list() <= 0) {
		/* empty batch or errors */
		job_operation->turn_around_job_answer();
		delete req;
		return;
	}

	job_operation->set_batch_key_list(req);
	replyMultiplexer.job_answer_procedure(job_operation);

	log4cplus_debug("JobHubAskChain enter job_ask_procedure");
	return;
}
