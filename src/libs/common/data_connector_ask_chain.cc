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
#include <algorithm>
#include "list/list.h"
#include "config/dbconfig.h"
#include "connector/connector_group.h"
#include "data_connector_ask_chain.h"
#include "request/request_base.h"
#include "task/task_request.h"
#include "log/log.h"
#include "key/key_helper.h"
#include "stat_dtc.h"
#include "protocol.h"
#include "dtc_global.h"
#include "listener/listener.h"
#include "helper.h"
#include "socket/unix_socket.h"

extern const char *connector_name[];

class GuardNotify : public JobAnswerInterface<DTCJobOperation> {
    public:
	GuardNotify(DataConnectorAskChain *o) : owner(o)
	{
	}
	~GuardNotify()
	{
	}
	virtual void job_answer_procedure(DTCJobOperation *);

    private:
	DataConnectorAskChain *owner;
};

void GuardNotify::job_answer_procedure(DTCJobOperation *job)
{
	log4cplus_debug("enter job_ask_procedure");
	if (job->result_code() >= 0)
		owner->guard->add_key(job->barrier_key(), job->packed_key());
	job->turn_around_job_answer();
	log4cplus_debug("leave job_ask_procedure");
}

DataConnectorAskChain::DataConnectorAskChain()
	: JobAskInterface<DTCJobOperation>(NULL), hasDummyMachine(0),
	  guardReply(NULL), tableNo(0), guard(NULL)
	  , p_task_dispatcher_(NULL)
{
	dbConfig[0] = NULL;
	dbConfig[1] = NULL;
	groupMap[0] = NULL;
	groupMap[1] = NULL;
	groups[0] = NULL;
	groups[1] = NULL;
	/* Total queue statistics, still meaningful for now, temporarily retained */
	statQueueCurCount = g_stat_mgr.get_stat_int_counter(CUR_QUEUE_COUNT);
	statQueueMaxCount = g_stat_mgr.get_stat_int_counter(MAX_QUEUE_COUNT);

	/* Added statistics for maximum queue length among four groups, used for alert monitoring */
	statReadQueueCurMaxCount = g_stat_mgr.get_stat_int_counter(
		HELPER_READ_GROUR_CUR_QUEUE_MAX_SIZE);
	statWriteQueueMaxCount = g_stat_mgr.get_stat_int_counter(
		HELPER_WRITE_GROUR_CUR_QUEUE_MAX_SIZE);
	statCommitQueueCurMaxCount = g_stat_mgr.get_stat_int_counter(
		HELPER_COMMIT_GROUR_CUR_QUEUE_MAX_SIZE);
	statSlaveReadQueueMaxCount = g_stat_mgr.get_stat_int_counter(
		HELPER_SLAVE_READ_GROUR_CUR_QUEUE_MAX_SIZE);
}

DataConnectorAskChain::~DataConnectorAskChain()
{
	if (groups[0]) {
		for (int i = 0;
		     i < dbConfig[0]->machineCnt * GROUPS_PER_MACHINE; i++)
			DELETE(groups[0][i]);

		FREE_CLEAR(groups[0]);
	}

	FREE_CLEAR(groupMap[0]);
	DELETE(guard);
	DELETE(guardReply);
}

ConnectorGroup *DataConnectorAskChain::select_group(DTCJobOperation *job)
{
	const DTCValue *key = job->request_key();
	uint64_t uk;
	/* key-hash disable */
	if (dbConfig[0]->keyHashConfig.keyHashEnable == 0 || key == NULL) {
		if (NULL == key)
			uk = 0;
		else if (key->s64 < 0)
			uk = 0 - key->s64;
		else
			uk = key->s64;
	} else {
		switch (job->field_type(0)) {
		case DField::Signed:
		case DField::Unsigned:
			uk = dbConfig[0]->keyHashConfig.keyHashFunction(
				(const char *)&(key->u64), sizeof(key->u64),
				dbConfig[0]->keyHashConfig.keyHashLeftBegin,
				dbConfig[0]->keyHashConfig.keyHashRightBegin);
			break;
		case DField::String:
		case DField::Binary:
			uk = dbConfig[0]->keyHashConfig.keyHashFunction(
				key->bin.ptr, key->bin.len,
				dbConfig[0]->keyHashConfig.keyHashLeftBegin,
				dbConfig[0]->keyHashConfig.keyHashRightBegin);
			break;
		default:
			uk = 0;
		}
	}

	if (dbConfig[1]) {
		int idx = uk / dbConfig[1]->dbDiv % dbConfig[1]->dbMod;
		int machineId = groupMap[1][idx];
		ConnectorGroup *ptr = groups[1][machineId * GROUPS_PER_MACHINE];
		if (ptr != NULL && job->request_code() != DRequest::Get)
			return GROUP_READONLY;
	}

	int idx = uk / dbConfig[0]->dbDiv % dbConfig[0]->dbMod;

	int machineId = groupMap[0][idx];
	if (machineId == GMAP_NONE)
		return NULL;
	if (machineId == GMAP_DUMMY)
		return GROUP_DUMMY;

	ConnectorGroup **ptr = &groups[0][machineId * GROUPS_PER_MACHINE];

	if (job->request_code() == DRequest::Get && ptr[GROUPS_PER_ROLE] &&
	    false == guard->in_set(job->barrier_key(), job->packed_key())) {
		int role = 0;
		switch (dbConfig[0]->mach[machineId].mode) {
		case BY_SLAVE:
			role = 1;
			break;

		case BY_DB:
			role = (uk / dbConfig[0]->dbDiv) & 1;

		case BY_TABLE:
			role = (uk / dbConfig[0]->tblDiv) & 1;

		case BY_KEY:
			role = job->barrier_key() & 1;
		}

		return ptr[role * GROUPS_PER_ROLE];
	}

	int g = job->request_type();

	while (--g >= 0) {
		if (ptr[g] != NULL) {
			return ptr[g];
		}
	}
	return NULL;
}

bool DataConnectorAskChain::is_commit_full(DTCJobOperation *job)
{
	if (job->request_code() != DRequest::Replace)
		return false;

	ConnectorGroup *helperGroup = select_group(job);
	if (helperGroup == NULL || helperGroup == GROUP_DUMMY ||
	    helperGroup == GROUP_READONLY)
		return false;

	if (helperGroup->queue_full()) {
		log4cplus_warning("NO FREE COMMIT QUEUE SLOT");
		helperGroup->dump_state();
	}
	return helperGroup->queue_full() ? true : false;
}

int DataConnectorAskChain::Cleanup()
{
	newDb.clear();
	new2old.clear();
	return 0;
}

int DataConnectorAskChain::Cleanup2()
{
	if (groups[1]) {
		for (int i = 0; i < dbConfig[1]->machineCnt; ++i) {
			std::vector<int>::iterator it =
				find(newDb.begin(), newDb.end(), i);
			if (it != newDb.end()) {
				for (int j = 0; j < GROUPS_PER_MACHINE; ++j) {
					DELETE(groups[1][j]);
				}
			}
		}
		FREE_CLEAR(groups[1]);
	}
	FREE_CLEAR(groupMap[1]);
	if (dbConfig[1]) {
		dbConfig[1]->destory();
		dbConfig[1] = NULL;
	}
	return 0;
}

int DataConnectorAskChain::build_helper_object(int idx)
{
	if (groups[idx] != NULL) {
		log4cplus_error("groups[%d] exists", idx);
		return -1;
	}
	groups[idx] = (ConnectorGroup **)CALLOC(sizeof(ConnectorGroup *),
						dbConfig[idx]->machineCnt *
							GROUPS_PER_MACHINE);
	if (!groups[idx]) {
		log4cplus_error("malloc failed, %m");
		return -1;
	}

	DTCConfig* p_dtc_conf = dbConfig[idx]->cfgObj;
	int i_has_hwc = 0;
	if(p_dtc_conf)
	{
		if(p_dtc_conf->get_config_node()["primary"]["hot"])
			i_has_hwc = 1;
	}
	log4cplus_info("enable hwc:%d" , i_has_hwc);

	/* build helper object */
	for (int i = 0; i < dbConfig[idx]->machineCnt; i++) {
		if (dbConfig[idx]->mach[i].helperType == DUMMY_HELPER)
			continue;
		if (idx == 1 &&
		    find(newDb.begin(), newDb.end(), i) == newDb.end()) {
			// if not new db mach, just continue, copy old mach when switch
			continue;
		}
		for (int j = 0; j < GROUPS_PER_MACHINE; j++) {
			if (dbConfig[idx]->mach[i].gprocs[j] == 0)
				continue;
			log4cplus_debug("start worker sequence: %d", j);

			char name[24];
			snprintf(name, sizeof(name), "%d%c%d", i,
				 MACHINEROLESTRING[j / GROUPS_PER_ROLE],
				 j % GROUPS_PER_ROLE);
			groups[idx][i * GROUPS_PER_MACHINE + j] =
				new ConnectorGroup(
					dbConfig[idx]
						->mach[i]
						.role[j / GROUPS_PER_ROLE]
						.path,
					name, dbConfig[idx]->mach[i].gprocs[j],
					dbConfig[idx]->mach[i].gqueues[j],
					DTC_SQL_USEC_ALL,
					i_has_hwc);

			if (j >= GROUPS_PER_ROLE)
				groups[idx][i * GROUPS_PER_MACHINE + j]
					->fallback =
					groups[idx][i * GROUPS_PER_MACHINE];
			log4cplus_debug("start worker %s", name);
		}
	}

	return 0;
}

int DataConnectorAskChain::build_master_group_mapping(int idx)
{
	if (groupMap[idx] != NULL) {
		log4cplus_error("groupMap[%d] exist", idx);
		return -1;
	}
	groupMap[idx] = (short *)MALLOC(sizeof(short) *
					dbConfig[idx]->database_max_count);
	if (groupMap[idx] == NULL) {
		log4cplus_error("malloc error for groupMap[%d]", idx);
		return -1;
	}
	for (int i = 0; i < dbConfig[idx]->database_max_count; i++)
		groupMap[idx][i] = GMAP_NONE;

	log4cplus_info("machine cnt:%d", dbConfig[idx]->machineCnt);
	/* build master group mapping */
	for (int i = 0; i < dbConfig[idx]->machineCnt; i++) {
		int gm_id = i;
		log4cplus_info("helper type:%d", dbConfig[idx]->mach[i].helperType);
		if (dbConfig[idx]->mach[i].helperType == DUMMY_HELPER) {
			gm_id = GMAP_DUMMY;
			hasDummyMachine = 1;
		} else if (dbConfig[idx]->mach[i].procs == 0) {
			log4cplus_error("procs=0 at idx:%d, i: %d", idx, i);
			continue;
		}
		log4cplus_info("mach[%d].dbCnt: %d", i, dbConfig[idx]->mach[i].dbCnt);
		for (int j = 0; j < dbConfig[idx]->mach[i].dbCnt; j++) {
			const int db = dbConfig[idx]->mach[i].dbIdx[j];
			if (groupMap[idx][db] >= 0) {
				log4cplus_error(
					"duplicate machine, db %d machine %d %d",
					db, groupMap[idx][db] + 1, i + 1);
				return -1;
			}
			groupMap[idx][db] = gm_id;
		}
	}
	for (int i = 0; i < dbConfig[idx]->database_max_count; ++i) {
		log4cplus_info("database_max_count:%d, idx: %d", dbConfig[idx]->database_max_count, idx);
		if (groupMap[idx][i] == GMAP_NONE) {
			log4cplus_error(
				"db completeness check error, db %d not found",
				i);
			return -1;
		}
	}
	return 0;
}

DbConfig *DataConnectorAskChain::get_db_config(DTCJobOperation *job)
{
	RowValue row(job->table_definition());
	DTCConfig *config = NULL;
	DbConfig *newdb = NULL;
	// parse db config
	if (!job->request_operation()) {
		log4cplus_error("table.yaml not found when migrate db");
		job->set_error(-EC_DATA_NEEDED, "group collect",
			       "migrate db need table.yaml");
		return NULL;
	}
	job->update_row(row);
	log4cplus_debug("strlen: %ld, row[3].bin.ptr: %s",
			strlen(row[3].bin.ptr), row[3].bin.ptr);
	char *buf = row[3].bin.ptr;
	config = new DTCConfig();
	if (config->load_yaml_buffer(buf) !=
	    0) {
		log4cplus_error(
			"table.yaml illeagl when migrate db, parse error");
		job->set_error(-EC_ERR_MIGRATEDB_ILLEGAL, "group collect",
			       "table.yaml illegal, parse error");
		delete config;
		return NULL;
	}
	if ((newdb = DbConfig::Load(config)) == NULL) {
		log4cplus_error(
			"table.yaml illeagl when migrate db, load error");
		job->set_error(-EC_ERR_MIGRATEDB_ILLEGAL, "group collect",
			       "table.yaml illegal, load error");
		return NULL;
	}
	return newdb;
}

int DataConnectorAskChain::migrate_db(DTCJobOperation *job)
{
	int ret = 0;
	DbConfig *newDbConfig = get_db_config(job);
	if (newDbConfig == NULL)
		return -2;
	if (dbConfig[1]) {
		bool same = dbConfig[1]->Compare(newDbConfig, true);
		newDbConfig->destory();
		if (!same) {
			log4cplus_error("new table.yaml when migrating db");
			job->set_error(-EC_ERR_MIGRATEDB_MIGRATING,
				       "group collect",
				       "new table.yaml when migrating db");
			return -2;
		}
		log4cplus_info("duplicate table.yaml when migrating db");
		job->set_error(-EC_ERR_MIGRATEDB_DUPLICATE, "group collect",
			       "duplicate table.yaml when migrating db");
		return 0;
	}
	// check are others fields same
	if (!newDbConfig->Compare(dbConfig[0], false)) {
		newDbConfig->destory();
		log4cplus_error("new table.yaml does not match old one");
		job->set_error(-EC_ERR_MIGRATEDB_DISTINCT, "group collect",
			       "new table.yaml does not match old one");
		return -2;
	}
	// set read only on new db
	dbConfig[1] = newDbConfig;
	// find new db
	dbConfig[1]->find_new_mach(dbConfig[0], newDb, new2old);
	log4cplus_debug("found %ld new db machine", newDb.size());
	if (newDb.size() == 0) {
		log4cplus_error(
			"table.yaml does not contain new db when migrate db");
		job->set_error(-EC_DATA_NEEDED, "group collect",
			       "table.yaml does not contain new db");
		return -1;
	}
	// check db completeness of new db config
	if (build_master_group_mapping(1) != 0) {
		log4cplus_error("table.yaml db mapping is not complete");
		job->set_error(-EC_DATA_NEEDED, "group collect",
			       "table.yaml db mapping is not complete");
		return -1;
	}

	// save new table.yaml as table%d.conf
	char tableName[64];
	snprintf(tableName, 64, "../conf/dtc%d.conf", tableNo);
	log4cplus_debug("table.yaml: %s", tableName);
	if (dbConfig[1]->cfgObj->Dump(tableName, true) != 0) {
		log4cplus_error("save table.yaml as table2.conf error");
		job->set_error(-EC_SERVER_ERROR, "group collect",
			       "save table.yaml as table2.conf error");
		return -1;
	}

	// start listener, connect db, check access, start worker
	if ((ret = start_listener(job)) != 0)
		return ret;
	++tableNo;

	// start worker and create class member variable
	if (build_helper_object(1) != 0) {
		log4cplus_error("verify connect error: %m");
		job->set_error(-EC_ERR_MIGRATEDB_HELPER, "group collect",
			       "start helper worker error");
		return -1;
	}

	// disable commit as none async
	disable_commit_group(1);
	set_timer_handler(recvList, connList, retryList, 1);

	return 0;
}

int DataConnectorAskChain::switch_db(DTCJobOperation *job)
{
	if (!dbConfig[1]) {
		log4cplus_info("migrate db not start");
		job->set_error(-EC_ERR_MIGRATEDB_NOT_START, "group collect",
			       "migrate db not start");
		return -2;
	}
	DbConfig *newDbConfig = get_db_config(job);
	if (newDbConfig == NULL)
		return -2;
	// check is table same
	bool same = newDbConfig->Compare(dbConfig[1], true);
	newDbConfig->destory();
	if (!same) {
		log4cplus_error("switch db with different table.yaml");
		job->set_error(-EC_ERR_MIGRATEDB_DISTINCT, "group collect",
			       "switch db with different table.yaml");
		return -2;
	}
	// start worker helper
	do_attach(NULL, 1);
	// switch to new, unset read only
	std::swap(dbConfig[0], dbConfig[1]);
	std::swap(groups[0], groups[1]);
	std::swap(groupMap[0], groupMap[1]);
	// copy old client
	for (int i = 0; i < dbConfig[0]->machineCnt; ++i) {
		if (dbConfig[0]->mach[i].helperType == DUMMY_HELPER)
			continue;
		if (find(newDb.begin(), newDb.end(), i) != newDb.end())
			continue;
		memmove(groups[0] + i * GROUPS_PER_MACHINE,
			groups[1] + new2old[i] * GROUPS_PER_MACHINE,
			sizeof(ConnectorGroup *) * GROUPS_PER_MACHINE);
		log4cplus_debug("copy old client ptr: %p",
				*(groups[0] + i * GROUPS_PER_MACHINE));
	}
	// release old
	FREE_CLEAR(groupMap[1]);
	FREE_CLEAR(groups[1]);
	dbConfig[1]->destory();
	dbConfig[1] = NULL;
	// write conf file
	dbConfig[0]->cfgObj->Dump("../conf/dtc.yaml", false);
	Cleanup();

	return 0;
}

int DataConnectorAskChain::notify_watch_dog(StartHelperPara *para)
{
	char buf[16];
	if (sizeof(*para) > 15)
		return -1;
	char *env = getenv(ENV_WATCHDOG_SOCKET_FD);
	int fd = env == NULL ? -1 : atoi(env);
	if (fd > 0) {
		memset(buf, 0, 16);
		buf[0] = WATCHDOG_INPUT_HELPER;
		log4cplus_debug("sizeof(*para): %ld", sizeof(*para));
		memcpy(buf + 1, para, sizeof(*para));
		send(fd, buf, sizeof(buf), 0);
		return 0;
	} else {
		return -2;
	}
}

int DataConnectorAskChain::start_listener(DTCJobOperation *job)
{
	int ret = 0;
	log4cplus_debug("starting new db listener...");
	int nh = 0;
	dbConfig[1]->set_helper_path(getppid());
	for (std::vector<int>::iterator it = newDb.begin(); it != newDb.end();
	     ++it) {
		// start listener
		HELPERTYPE t = dbConfig[1]->mach[*it].helperType;
		log4cplus_debug("helper type = %d", t);
		if (DTC_HELPER >= t)
			continue;
		for (int r = 0; r < ROLES_PER_MACHINE; ++r) {
			int i, n = 0;
			for (i = 0;
			     i < GROUPS_PER_ROLE &&
			     (r * GROUPS_PER_ROLE + i) < GROUPS_PER_MACHINE;
			     ++i)
				n += dbConfig[1]
					     ->mach[*it]
					     .gprocs[r * GROUPS_PER_ROLE + i];
			if (n <= 0)
				continue;
			StartHelperPara para;
			para.type = t;
			para.backlog = n + 1;
			para.mach = *it;
			para.role = r;
			para.conf = DBHELPER_TABLE_NEW;
			para.num = tableNo;
			if ((ret = notify_watch_dog(&para)) < 0) {
				log4cplus_error(
					"notify daemons error for group %d role %d, ret: %d",
					*it, r, ret);
				return -1;
			}
			++nh;
		}
	}
	log4cplus_info("%d helper listener started", nh);
	return 0;
}

void DataConnectorAskChain::job_ask_procedure(DTCJobOperation *job)
{
	if (DRequest::ReloadConfig == job->request_code() &&
	    TaskTypeHelperReloadConfig == job->request_type()) {
		collect_notify_helper_reload_config(job);
		return;
	}

	int ret = 0;
	if (job->request_code() == DRequest::TYPE_SYSTEM_COMMAND) {
		switch (job->requestInfo.admin_code()) {
		case DRequest::SystemCommand::MigrateDB:
			log4cplus_debug(
				"GroupCollect::job_ask_procedure DRequest::TYPE_SYSTEM_COMMAND::MigrateDB");
			ret = migrate_db(job);
			if (ret == -1) {
				Cleanup2();
				Cleanup();
			}
			job->turn_around_job_answer();
			return;

		case DRequest::SystemCommand::MigrateDBSwitch:
			log4cplus_debug(
				"GroupCollect::job_ask_procedure DRequest::TYPE_SYSTEM_COMMAND::MigrateDBSwitch");
			ret = switch_db(job);
			job->turn_around_job_answer();
			return;
		default:
			// this should not happen
			log4cplus_error("unknown admin code: %d",
					job->requestInfo.admin_code());
			job->set_error(-EC_SERVER_ERROR, "helper collect",
				       "unkown admin code");
			job->turn_around_job_answer();
			return;
		}
	}

	ConnectorGroup *helperGroup = select_group(job);

	if (helperGroup == NULL) {
		log4cplus_error("Key not belong to this server");
		job->set_error(-EC_OUT_OF_KEY_RANGE,
			       "GroupCollect::job_ask_procedure",
			       "Key not belong to this server");
		job->turn_around_job_answer();
	} else if (helperGroup == GROUP_DUMMY) {
		job->mark_as_black_hole();
		job->turn_around_job_answer();
	} else if (helperGroup == GROUP_READONLY) {
		log4cplus_debug(
			"try to do non read op on a key which belongs db which is migrating");
		job->set_error(
			-EC_SERVER_ERROR, "helper collect",
			"try to do non read op on a key which belongs db which is migrating");
		job->turn_around_job_answer();
	} else {
		if (job->request_type() == TaskTypeWrite && guardReply != NULL)
			job->push_reply_dispatcher(guardReply);
		helperGroup->job_ask_procedure(job);
	}

	stat_helper_group_queue_count(groups[0], dbConfig[0]->machineCnt *
							 GROUPS_PER_MACHINE);
	stat_helper_group_cur_max_queue_count(job->request_type());
}

int DataConnectorAskChain::load_config(DbConfig *cfg, int keysize, int idx)
{
	dbConfig[0] = cfg;
	int ret = 0;

	if ((ret = build_master_group_mapping(idx)) != 0) {
		log4cplus_error("build master group map error, ret: %d", ret);
		return ret;
	}
	if ((ret = build_helper_object(idx)) != 0) {
		log4cplus_error("build helper object error, ret: %d", ret);
		return ret;
	}

	if (dbConfig[0]->slaveGuard) {
		guard = new KeyHelper(keysize, dbConfig[0]->slaveGuard);
		guardReply = new GuardNotify(this);
	}
	return 0;
}

int DataConnectorAskChain::renew_config(struct DbConfig *cfg)
{
	dbConfig[1] = cfg;
	std::swap(dbConfig[0], dbConfig[1]);
	dbConfig[1]->destory();
	dbConfig[1] = NULL;
	return 0;
}

int DataConnectorAskChain::do_attach(PollerBase *thread, int idx)
{
	if (idx == 0)
		JobAskInterface<DTCJobOperation>::attach_thread(thread);
	for (int i = 0; 
		i < dbConfig[idx]->machineCnt * GROUPS_PER_MACHINE;
	    i++) {
		if (groups[idx][i]) {
			groups[idx][i]->do_attach(owner, &task_queue_allocator);

			assert(p_task_dispatcher_ != NULL);
			groups[idx][i]->BindHbLogDispatcher(p_task_dispatcher_);
		}
	}
	return 0;
}

void DataConnectorAskChain::set_timer_handler(TimerList *recv, TimerList *conn,
					      TimerList *retry, int idx)
{
	if (idx == 0) {
		recvList = recv;
		connList = conn;
		retryList = retry;
	}
	for (int i = 0; i < dbConfig[idx]->machineCnt; i++) {
		if (dbConfig[idx]->mach[i].helperType == DUMMY_HELPER)
			continue;
		for (int j = 0; j < GROUPS_PER_MACHINE; j++) {
			if (dbConfig[idx]->mach[i].gprocs[j] == 0)
				continue;
			if (groups[idx][i * GROUPS_PER_MACHINE + j])
				groups[idx][i * GROUPS_PER_MACHINE + j]
					->set_timer_handler(recv, conn, retry);
		}
	}
}

int DataConnectorAskChain::disable_commit_group(int idx)
{
	if (groups[idx] == NULL)
		return 0;
	for (int i = 2; i < dbConfig[idx]->machineCnt * GROUPS_PER_MACHINE;
	     i += GROUPS_PER_MACHINE) {
		DELETE(groups[idx][i]);
	}
	return 0;
}

void DataConnectorAskChain::stat_helper_group_queue_count(
	ConnectorGroup **groups, unsigned group_count)
{
	unsigned total_queue_count = 0;
	unsigned total_queue_max_count = 0;

	for (unsigned i = 0; i < group_count; ++i) {
		if (groups[i]) {
			total_queue_count += groups[i]->queue_count();
			total_queue_max_count += groups[i]->queue_max_count();
		}
	}

	statQueueCurCount = total_queue_count;
	statQueueMaxCount = total_queue_max_count;
	return;
}

int DataConnectorAskChain::get_queue_cur_max_count(int iColumn)
{
	int max_count = 0;
	if ((iColumn < 0) || (iColumn >= GROUPS_PER_MACHINE)) {
		return max_count;
	}

	for (int row = 0; row < dbConfig[0]->machineCnt; row++) {
		/* Read group is in the first column of group matrix */
		ConnectorGroup *readGroup =
			groups[0][GROUPS_PER_MACHINE * row + iColumn];
		if (NULL == readGroup) {
			continue;
		}
		if (readGroup->queue_count() > max_count) {
			max_count = readGroup->queue_count();
			log4cplus_debug("the group queue max_count is %d ",
					max_count);
		}
	}
	return max_count;
}
/* Pass in request type, only count corresponding values based on request type each time */
void DataConnectorAskChain::stat_helper_group_cur_max_queue_count(
	int iRequestType)
{
	/* Cannot distinguish between master read and slave read based on request type (related to Workload configuration), have to count both master read group and slave read group */
	/* Unless traversing pointer values in group matrix and comparing with group pointer after selectgroup, then comparing matrix columns, which is more complex */
	if (TaskTypeRead == iRequestType) {
		statReadQueueCurMaxCount =
			get_queue_cur_max_count(MASTER_READ_GROUP_COLUMN);
		statSlaveReadQueueMaxCount =
			get_queue_cur_max_count(SLAVE_READ_GROUP_COLUMN);
	}
	if (TaskTypeWrite == iRequestType) {
		statWriteQueueMaxCount =
			get_queue_cur_max_count(MASTER_WRITE_GROUP_COLUMN);
	}

	if (TaskTypeCommit == iRequestType) {
		statCommitQueueCurMaxCount =
			get_queue_cur_max_count(MASTER_COMMIT_GROUP_COLUMN);
	}
}

void DataConnectorAskChain::collect_notify_helper_reload_config(
	DTCJobOperation *job)
{
	unsigned int uiGroupNum = 0;
	unsigned int uiNullGroupNum = 0;

	for (int machineid = 0; machineid < dbConfig[0]->machineCnt;
	     ++machineid) {
		ConnectorGroup **ptr =
			&groups[0][machineid * GROUPS_PER_MACHINE];
		for (int groupid = 0; groupid < GROUPS_PER_MACHINE; ++groupid) {
			++uiGroupNum;
			ConnectorGroup *pHelperGroup = ptr[groupid];
			if (NULL == pHelperGroup ||
			    GROUP_DUMMY == pHelperGroup ||
			    GROUP_READONLY == pHelperGroup) {
				++uiNullGroupNum;
				continue;
			}
			pHelperGroup->job_ask_procedure(job);
		}
	}
	if (uiGroupNum == uiNullGroupNum) {
		log4cplus_error(
			"not have available helpergroup, please check!");
		job->set_error(-EC_NOT_HAVE_AVAILABLE_HELPERGROUP,
			       "helper collect",
			       "not have available helpergroup");
	}
	log4cplus_error(
		"groupcollect notify work helper reload config finished!");
	job->turn_around_job_answer();
}
