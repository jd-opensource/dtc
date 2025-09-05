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
#ifndef __DATA_CONNECTOR_ASK_CHAIN__
#define __DATA_CONNECTOR_ASK_CHAIN__

#include "config/dbconfig.h"
#include "poll/poller_base.h"
#include "request/request_base.h"
#include "../daemons/daemon_listener.h"
#include <stat_dtc.h>

class ConnectorGroup;
class TimerList;
class KeyHelper;

enum { MASTER_READ_GROUP_COLUMN = 0,
       MASTER_WRITE_GROUP_COLUMN = 1,
       MASTER_COMMIT_GROUP_COLUMN = 2,
       SLAVE_READ_GROUP_COLUMN = 3,
};
class DataConnectorAskChain : public JobAskInterface<DTCJobOperation> {
    public:
	DataConnectorAskChain();
	~DataConnectorAskChain();

	void BindHbLogDispatcher(JobAskInterface<DTCJobOperation>* p_task_dispatcher) {
		p_task_dispatcher_ = p_task_dispatcher;
	};

	int load_config(struct DbConfig *cfg, int ks, int idx = 0);
	int renew_config(struct DbConfig *cfg);
	void collect_notify_helper_reload_config(DTCJobOperation *job);
	int build_master_group_mapping(int idx = 0);
	int build_helper_object(int idx = 0);
	int notify_watch_dog(StartHelperPara *para);
	int Cleanup();
	int Cleanup2();
	int start_listener(DTCJobOperation *job);
	bool is_commit_full(DTCJobOperation *job);
	bool Dispatch(DTCJobOperation *t);
	int do_attach(PollerBase *thread, int idx = 0);
	void set_timer_handler(TimerList *recv, TimerList *conn,
			       TimerList *retry, int idx = 0);
	int disable_commit_group(int idx = 0);
	DbConfig *get_db_config(DTCJobOperation *job);
	int migrate_db(DTCJobOperation *t);
	int switch_db(DTCJobOperation *t);

	int has_dummy_machine(void) const
	{
		return hasDummyMachine;
	}

    private:
	virtual void job_ask_procedure(DTCJobOperation *);
	ConnectorGroup *select_group(DTCJobOperation *t);

	void stat_helper_group_queue_count(ConnectorGroup **group,
					   unsigned group_count);
	void stat_helper_group_cur_max_queue_count(int iRequestType);
	int get_queue_cur_max_count(int iColumn);

    private:
	struct DbConfig *dbConfig[2];

	int hasDummyMachine;

	ConnectorGroup **groups[2];
#define GMAP_NONE -1
#define GMAP_DUMMY -2
#define GROUP_DUMMY ((ConnectorGroup *)-2)
#define GROUP_READONLY ((ConnectorGroup *)-3)
	short *groupMap[2];
	JobAnswerInterface<DTCJobOperation> *guardReply;
	LinkQueue<DTCJobOperation *>::allocator task_queue_allocator;

	TimerList *recvList;
	TimerList *connList;
	TimerList *retryList;

	std::vector<int> newDb;
	std::map<int, int> new2old;
	int tableNo;
	JobAskInterface<DTCJobOperation>* p_task_dispatcher_;

public:
	KeyHelper *guard;

private:
	StatCounter statQueueCurCount; /*Current total queue size of all groups*/
	StatCounter statQueueMaxCount; /*Total configured queue size of all groups*/
	StatCounter statReadQueueCurMaxCount; /*Current maximum queue size of all master read groups on all machines*/
	StatCounter statWriteQueueMaxCount; /*Current maximum queue size of all write groups on all machines*/
	StatCounter statCommitQueueCurMaxCount; /*Current maximum queue size of all commit groups on all machines*/
	StatCounter statSlaveReadQueueMaxCount; /*Current maximum queue size of all slave read groups on all machines*/
};

#endif
