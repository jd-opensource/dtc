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

#ifndef __HOT_BACKUP_ASK_CHAIN__
#define __HOT_BACKUP_ASK_CHAIN__

#include "request/request_base.h"
#include "hb_log.h"
#include "task_pendlist.h"
#include "stat_manager.h"
#include <map>

class PollerBase;
class DTCJobOperation;
enum THBResult {
	HB_PROCESS_ERROR = -1,
	HB_PROCESS_OK = 0,
	HB_PROCESS_PENDING = 2,
};

class HotBackupAskChain : public JobAskInterface<DTCJobOperation> {
    public:
	HotBackupAskChain(PollerBase *o);
	virtual ~HotBackupAskChain();

	virtual void job_ask_procedure(DTCJobOperation *job_operation);
	bool do_init(uint64_t total, off_t max_size);

    private:
	/*concrete hb operation*/
	THBResult write_hb_log_process(DTCJobOperation &job);
	THBResult read_hb_log_process(DTCJobOperation &job);
	THBResult write_lru_hb_log_process(DTCJobOperation &job);
	THBResult register_hb_log_process(DTCJobOperation &job);
	THBResult query_hb_log_info_process(DTCJobOperation &job);

    private:
	PollerBase *ownerThread_;
	ChainJoint<DTCJobOperation> main_chain;
	TaskPendingList taskPendList_;
	HBLog hbLog_;
	StatSample statIncSyncStep_;
};

#endif
