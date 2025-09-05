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

#include "log/log.h"
#include "buffer_process_ask_chain.h"
#include <daemon/daemon.h>
#include "buffer_remoteLog.h"

void HotBackReplay::job_answer_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("job_answer_procedure, request type %d",
			job_operation->request_type());
	int iRet = job_operation->result_code();
	if (0 != iRet) {
		if ((-ETIMEDOUT == iRet) || (-EC_INC_SYNC_STAGE == iRet) ||
		    (-EC_FULL_SYNC_STAGE == iRet)) {
			log4cplus_debug(
				"hotback job , normal fail: from %s msg %s, request type %d",
				job_operation->resultInfo.error_from(),
				job_operation->resultInfo.error_message(),
				job_operation->request_type());
		} else {
			log4cplus_error(
				"hotback job fail: from %s msg %s, request type %d",
				job_operation->resultInfo.error_from(),
				job_operation->resultInfo.error_message(),
				job_operation->request_type());
		}
	}

	if ((TaskTypeWriteHbLog == job_operation->request_type()) ||
	    (TaskTypeWriteLruHbLog == job_operation->request_type())) {
		/*only delete job */
		log4cplus_debug("write hotback job reply ,just delete job");
		delete job_operation;
		return;
	}
	log4cplus_debug("read hotback job ,reply to client");
	job_operation->turn_around_job_answer();
}

void FlushReplyNotify::job_answer_procedure(DTCJobOperation *job_operation)
{
	flush_reply_notify_owner_->transaction_begin(job_operation);
	if (job_operation->result_code() < 0) {
		flush_reply_notify_owner_->deal_flush_exeption(*job_operation);
	} else if (job_operation->result_code() > 0) {
		log4cplus_info("result_code() > 0: from %s msg %s",
			       job_operation->resultInfo.error_from(),
			       job_operation->resultInfo.error_message());
	}
	if (job_operation->result_code() >= 0 &&
	    flush_reply_notify_owner_->reply_flush_answer(*job_operation) !=
		    DTC_CODE_BUFFER_SUCCESS) {
		if (job_operation->result_code() >= 0)
			job_operation->set_error(
				-EC_SERVER_ERROR, "reply_flush_answer",
				flush_reply_notify_owner_->last_error_message());
	}

	job_operation->turn_around_job_answer();
	flush_reply_notify_owner_->transaction_end();
}
