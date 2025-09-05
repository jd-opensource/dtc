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

#include "hot_backup_ask_chain.h"
#include "poll/poller_base.h"
#include "task/task_request.h"
#include "log/log.h"
#include "hotback_task.h"

extern DTCTableDefinition *g_table_def[];

HotBackupAskChain::HotBackupAskChain(PollerBase *o)
	: JobAskInterface<DTCJobOperation>(o), ownerThread_(o), main_chain(o),
	  taskPendList_(this),
	  hbLog_(TableDefinitionManager::instance()->get_hot_backup_table_def())
{
}

HotBackupAskChain::~HotBackupAskChain()
{
}
void HotBackupAskChain::job_ask_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("enter job_ask_procedure");
	log4cplus_debug("request type is %d ", job_operation->request_type());
	THBResult result = HB_PROCESS_ERROR;
	switch (job_operation->request_type()) {
	case TaskTypeWriteHbLog: {
		result = write_hb_log_process(*job_operation);
		break;
	}
	case TaskTypeReadHbLog: {
		result = read_hb_log_process(*job_operation);
		break;
	}
	case TaskTypeWriteLruHbLog: {
		result = write_lru_hb_log_process(*job_operation);
		break;
	}
	case TaskTypeRegisterHbLog: {
		result = register_hb_log_process(*job_operation);
		break;
	}
	case TaskTypeQueryHbLogInfo: {
		result = query_hb_log_info_process(*job_operation);
		break;
	}
	default: {
		job_operation->set_error(-EBADRQC, "hb process",
					 "invalid hb cmd code");
		log4cplus_info("invalid hb cmd code[%d]",
			       job_operation->request_type());
		job_operation->turn_around_job_answer();
		return;
	}
	}

	if (HB_PROCESS_PENDING == result) {
		log4cplus_debug("hb job is pending ");
		return;
	}
	log4cplus_debug("hb job reply");
	job_operation->turn_around_job_answer();
	log4cplus_debug("leave job_ask_procedure");
	return;
}

bool HotBackupAskChain::do_init(uint64_t total, off_t max_size)
{
	log4cplus_debug("total: %lu, max_size: %ld", total, max_size);
	if (hbLog_.init("../log/hblog", "hblog", total, max_size)) {
		log4cplus_error("hotback process for hblog init failed");
		return false;
	}

	return true;
}

THBResult HotBackupAskChain::write_hb_log_process(DTCJobOperation &job)
{
	if (0 != hbLog_.write_update_log(job)) {
		job.set_error(-EC_ERR_HOTBACK_WRITEUPDATE, "HBProcess",
			      "write_hb_log_process fail");
		return HB_PROCESS_ERROR;
	}
	taskPendList_.Wakeup();
	return HB_PROCESS_OK;
}

THBResult HotBackupAskChain::write_lru_hb_log_process(DTCJobOperation &job)
{
	if (0 != hbLog_.write_lru_hb_log(job)) {
		job.set_error(-EC_ERR_HOTBACK_WRITELRU, "HBProcess",
			      "write_lru_hb_log_process fail");
		return HB_PROCESS_ERROR;
	}
	return HB_PROCESS_OK;
}

THBResult HotBackupAskChain::read_hb_log_process(DTCJobOperation &job)
{
	log4cplus_debug("read Hb log begin ");
	JournalID hb_jid = job.versionInfo.hot_backup_id();
	log4cplus_debug("request serial:%d , offset:%d",hb_jid.serial , hb_jid.offset);
	JournalID write_jid = hbLog_.get_writer_jid();
	log4cplus_debug("local write serial:%d , offset:%d" , write_jid.serial , write_jid.offset);

	if (hb_jid.GE(write_jid)) {
		taskPendList_.add2_list(&job);
		return HB_PROCESS_PENDING;
	}

	if (hbLog_.Seek(hb_jid)) {
		job.set_error(-EC_BAD_HOTBACKUP_JID, "HBProcess",
			      "read_hb_log_process jid overflow");
		return HB_PROCESS_ERROR;
	}

	job.prepare_result_no_limit();

	int count =
		hbLog_.task_append_all_rows(job, job.requestInfo.limit_count());
	if (count >= 0) {
		statIncSyncStep_.push(count);
	} else {
		job.set_error(-EC_ERROR_BASE, "HBProcess",
			      "read_hb_log_process,decode binlog error");
		return HB_PROCESS_ERROR;
	}

	job.versionInfo.set_hot_backup_id((uint64_t)hbLog_.get_reader_jid());
	return HB_PROCESS_OK;
}
THBResult HotBackupAskChain::register_hb_log_process(DTCJobOperation &job)
{
	JournalID client_jid = job.versionInfo.hot_backup_id();
	JournalID master_jid = hbLog_.get_writer_jid();
	log4cplus_info(
		"hb register, client[serial=%u, offset=%u], master[serial=%u, offset=%u]",
		client_jid.serial, client_jid.offset, master_jid.serial,
		master_jid.offset);

	//full sync
	if (client_jid.Zero()) {
		log4cplus_info("full-sync stage.");
		job.versionInfo.set_hot_backup_id((uint64_t)master_jid);
		job.set_error(-EC_FULL_SYNC_STAGE, "HBProcess",
			      "Register,full-sync stage");
		return HB_PROCESS_ERROR;
	} else {
		//inc sync
		if (hbLog_.Seek(client_jid) == 0) {
			log4cplus_info("inc-sync stage.");
			job.versionInfo.set_hot_backup_id((uint64_t)client_jid);
			job.set_error(-EC_INC_SYNC_STAGE, "HBProcess",
				      "register, inc-sync stage");
			return HB_PROCESS_ERROR;
		}
		//error
		else {
			log4cplus_info("err-sync stage.");
			job.versionInfo.set_hot_backup_id((uint64_t)0);
			job.set_error(-EC_ERR_SYNC_STAGE, "HBProcess",
				      "register, err-sync stage");
			return HB_PROCESS_ERROR;
		}
	}
}
THBResult HotBackupAskChain::query_hb_log_info_process(DTCJobOperation &job)
{
	struct DTCServerInfo s_info;
	memset(&s_info, 0x00, sizeof(s_info));
	s_info.version = 0x1;

	JournalID jid = hbLog_.get_writer_jid();
	s_info.binlog_id = jid.Serial();
	s_info.binlog_off = jid.get_offset();
	job.resultInfo.set_server_info(&s_info);
	return HB_PROCESS_OK;
}
