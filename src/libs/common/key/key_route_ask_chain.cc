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
#include <cstdio>
#include <cassert>

#include "key_route_ask_chain.h"
#include "file_backed_key_set.h"
#include "table/hotbackup_table_def.h"
#include "../field/field.h"

KeyRouteAskChain::KeyRouteAskChain(PollerBase *owner, int keyFormat)
	: JobAskInterface<DTCJobOperation>(owner), main_chain(owner),
	  remote_chain(owner), m_keyFormat(keyFormat),
	  m_iCSState(CS_NOT_STARTED), m_strCSAddr("")
{
}

void KeyRouteAskChain::job_ask_procedure(DTCJobOperation *t)
{
	log4cplus_debug("enter job_ask_procedure");
	if (t->request_code() == DRequest::TYPE_SYSTEM_COMMAND) {
		log4cplus_debug("system command: %d",
				t->requestInfo.admin_code());
		switch (t->requestInfo.admin_code()) {
		case DRequest::SystemCommand::ReloadClusterNodeList:
			process_reload(t);
			return;
		case DRequest::SystemCommand::SetClusterNodeState:
			process_node_state_change(t);
			return;
		case DRequest::SystemCommand::change_node_address:
			process_change_node_address(t);
			return;
		case DRequest::SystemCommand::GetClusterState:
			process_get_cluster_state(t);
			return;
		case DRequest::SystemCommand::Migrate:
			process_migrate(t);
			return;
		case DRequest::SystemCommand::Cascade:
			process_cascade(t);
			return;
		default:
			main_chain.job_ask_procedure(t);
			return;
		}
	}

	if (t->request_code() == DRequest::TYPE_PASS) {
		//just pass through
		main_chain.job_ask_procedure(t);
		return;
	}

	if (t->packed_key() == NULL) {
		t->set_error(-EC_BAD_OPERATOR, "Key Route",
			     "Batch Request Fast Path Not Supported");
		t->turn_around_job_answer();
		return;
	}

	if (CS_CASCADING == m_iCSState) {
		log4cplus_debug("cascadeing state, addr [%s]",
				m_strCSAddr.c_str());
		if (m_strCSAddr.empty()) {
			t->set_error(-EC_SERVER_ERROR, "key route",
				     "the remote cascade addr is NULL");
			t->turn_around_job_answer();
			return;
		}
		t->set_remote_addr(m_strCSAddr.c_str());
		remote_chain.job_ask_procedure(t);
		return;
	}

	std::string selected = select_node(t->packed_key());
	log4cplus_debug("route to:%s self is:%s", selected.c_str(),
			m_selfName.c_str());
	const char *packedKey = t->packed_key();
	int keyLen = m_keyFormat > 0 ? m_keyFormat :
				       (*(unsigned char *)packedKey) + 1;

	if (selected == m_selfName) {
		main_chain.job_ask_procedure(t);
		return;
	}

	if (accept_key(selected, packedKey, keyLen)) {
		t->set_remote_addr(name_to_addr(selected));
		t->mark_as_pass_thru();
		remote_chain.job_ask_procedure(t);
		return;
	} else if (is_migrating(selected, packedKey, keyLen)) {
		if (t->request_code() != DRequest::Get) {
			t->set_error(
				-EC_SERVER_ERROR, "Key Route",
				"try to do non read op on a key while it is migrating");
			t->turn_around_job_answer();
			return;
		}
	} else {
		MigrationStateMap::iterator i =
			m_serverMigrationState.find(selected);
		if (i->second.state == MS_MIGRATE_READONLY) {
			if (t->request_code() != DRequest::Get) {
				t->set_error(-EC_SERVER_READONLY, "Key Route",
					     "migrate readonly");
				t->turn_around_job_answer();
				return;
			}
		}
	}
	main_chain.job_ask_procedure(t);
	log4cplus_debug("leave job_ask_procedure");
}

//Migrate command need set DTC address in KeyRoute.
//processing method in cache_admin
void KeyRouteAskChain::process_migrate(DTCJobOperation *t)
{
	std::string selected = select_node(t->packed_key());

	log4cplus_debug("migrate to %s", selected.c_str());
	if (selected == m_selfName) {
		const DTCFieldValue *ui = t->request_operation();
		if (ui && ui->field_value(0) &&
		    (ui->field_value(0)->s64 & 0xFF) ==
			    DTCMigrate::FROM_SERVER) {
			main_chain.job_ask_procedure(t);
			return;
		}

		log4cplus_info("key belongs to self,no migrate operate");
		t->set_error(-EC_STATE_ERROR, "Key Route",
			     "key belongs to self");
		t->turn_around_job_answer();
		return;
	}

	MigrationStateMap::iterator iter =
		m_serverMigrationState.find(selected);
	if (iter == m_serverMigrationState.end()) {
		t->set_error(-EC_STATE_ERROR, "Key Route",
			     "internal error: node not in map!");
		t->turn_around_job_answer();
		log4cplus_error(
			"internal error: %s not in serverMigrationState",
			selected.c_str());
		return;
	}

	if (iter->second.state != MS_MIGRATING &&
	    iter->second.state != MS_MIGRATE_READONLY) {
		t->set_error(-EC_STATE_ERROR, "Key Route",
			     "state not migratable");
		t->turn_around_job_answer();
		log4cplus_error("%s not migrating while key migrated!",
				selected.c_str());
		return;
	}
	t->set_remote_addr(name_to_addr(selected));
	main_chain.job_ask_procedure(t);
}

std::string KeyRouteAskChain::select_node(const char *packedKey)
{
	int keyLen = m_keyFormat > 0 ? m_keyFormat :
				       (*(unsigned char *)packedKey) + 1;
	std::string selected;
	//agent don't know how long the key is, so use u64 always
	if (m_keyFormat == 4) {
		uint64_t v = *(uint32_t *)packedKey;
		return m_selector.Select((const char *)&v, sizeof(uint64_t));
	}

	return m_selector.Select(packedKey, keyLen);
}

int KeyRouteAskChain::key_migrated(const char *key)
{
	std::string selected = select_node(key);
	MigrationStateMap::iterator iter =
		m_serverMigrationState.find(selected);
	if (iter == m_serverMigrationState.end()) {
		log4cplus_error(
			"internal error: %s not in serverMigrationState",
			selected.c_str());
		return -1;
	}

	if (iter->second.state != MS_MIGRATING &&
	    iter->second.state != MS_MIGRATE_READONLY) {
		log4cplus_error("%s not migrating while key migrated!",
				selected.c_str());
		return -1;
	}

	return iter->second.migrated->Migrated(key);
}

int KeyRouteAskChain::key_migrating(const char *key)
{
	std::string selected = select_node(key);
	MigrationStateMap::iterator iter =
		m_serverMigrationState.find(selected);
	if (iter == m_serverMigrationState.end()) {
		log4cplus_error(
			"internal error: %s not in serverMigrationState",
			selected.c_str());
		return -1;
	}

	if (iter->second.state != MS_MIGRATING &&
	    iter->second.state != MS_MIGRATE_READONLY) {
		log4cplus_error("%s not migrating while key migrated!",
				selected.c_str());
		return -1;
	}

	return iter->second.migrated->do_insert(key);
}

bool KeyRouteAskChain::accept_key(const std::string &node, const char *key,
				  int len)
{
	MigrationStateMap::iterator iter = m_serverMigrationState.find(node);
	if (iter == m_serverMigrationState.end()) {
		//this should never happen!
		log4cplus_error("can't find %s in state map!", node.c_str());
		//forward it either way
		return true;
	}

	if (iter->second.state == MS_NOT_STARTED)
		return false;

	if (iter->second.state == MS_COMPLETED)
		return true;

	return iter->second.migrated->Contains(key);
}

bool KeyRouteAskChain::is_migrating(const std::string &node, const char *key,
				    int len)
{
	MigrationStateMap::iterator iter = m_serverMigrationState.find(node);
	if (iter == m_serverMigrationState.end()) {
		//this should never happen!
		log4cplus_error("can't find %s in state map!", node.c_str());
		//forward it either way
		return false;
	}
	//migrate direciton stat is migrating or migrate readonly, should judge this key is in migrating
	if ((iter->second.state == MS_MIGRATING) ||
	    (iter->second.state == MS_MIGRATE_READONLY)) {
		return iter->second.migrated->is_migrating(key);
	}
	return false;
}
bool KeyRouteAskChain::is_same_cluster_config(
	std::map<std::string, std::string> &dtcClusterMap,
	const std::string &strDtcName)
{
	if (strDtcName != m_selfName) {
		log4cplus_error(
			"cluster dtc name is not equal, now name[%s], previous name[%s]",
			strDtcName.c_str(), m_selfName.c_str());
		return false;
	}
	if (dtcClusterMap.size() != m_serverMigrationState.size()) {
		log4cplus_error(
			"cluster dtc num is not equal, now size[%ld], previous size[%ld]",
			dtcClusterMap.size(), m_serverMigrationState.size());
		return false;
	}
	MigrationStateMap::const_iterator migrateIter =
		m_serverMigrationState.begin();
	for (; migrateIter != m_serverMigrationState.end(); migrateIter++) {
		std::map<std::string, std::string>::const_iterator dtcmapIter =
			dtcClusterMap.find(migrateIter->first);
		if (dtcmapIter != dtcClusterMap.end()) {
			if (migrateIter->second.addr != dtcmapIter->second) {
				log4cplus_error(
					"cluster dtc addis not equal,dtc name[%s] , previous size[%s], now size[%s]",
					migrateIter->first.c_str(),
					migrateIter->second.addr.c_str(),
					dtcmapIter->second.c_str());
				return false;
			}
		} else {
			log4cplus_error(
				"previous dtc[%s] is not in now dtc cluster",
				migrateIter->first.c_str());
			return false;
		}
	}
	return true;
}
void KeyRouteAskChain::process_reload(DTCJobOperation *t)
{
	//Table switching is not allowed before migration is completed
	if (migration_inprogress()) {
		RowValue row(t->table_definition());
		t->update_row(row);
		log4cplus_debug("get new table, len %d", row[3].bin.len);
		std::string strSelfName;
		std::map<std::string, std::string> dtcClusterMap;
		if (!ClusterConfig::parse_cluster_config(
			    strSelfName, dtcClusterMap, row[3].bin.ptr,
			    row[3].bin.len)) {
			t->set_error(-EC_BAD_INVALID_FIELD, "Key Route",
				     "invalid table");
			t->turn_around_job_answer();
			return;
		}
		if (!is_same_cluster_config(dtcClusterMap, strSelfName)) {
			t->set_error(
				-EC_STATE_ERROR, "Key Route",
				"Reload not possible while migration in-progress");
			t->turn_around_job_answer();
		} else {
			t->prepare_result_no_limit();
			t->turn_around_job_answer();
		}

		return;
	}

	if (!t->request_operation()) {
		t->set_error(-EC_DATA_NEEDED, "Key Route", "table needed");
		t->turn_around_job_answer();
		return;
	}

	RowValue row(t->table_definition());
	t->update_row(row);

	//the table should be placed in the last field
	log4cplus_debug("get new table, len %d", row[3].bin.len);

	std::string strSelfName;
	std::vector<ClusterConfig::ClusterNode> nodes;
	if (!ClusterConfig::parse_cluster_config(
		    strSelfName, &nodes, row[3].bin.ptr, row[3].bin.len)) {
		t->set_error(-EC_BAD_INVALID_FIELD, "Key Route",
			     "invalid table");
		t->turn_around_job_answer();
		return;
	}
	if (!ClusterConfig::save_cluster_config(&nodes, strSelfName)) {
		t->set_error(-EC_SERVER_ERROR, "Key Route",
			     "wirte config to file error");
		t->turn_around_job_answer();
		return;
	}

	m_serverMigrationState.clear();
	m_selector.Clear();

	log4cplus_debug("got new table");
	for (std::vector<ClusterConfig::ClusterNode>::iterator i =
		     nodes.begin();
	     i != nodes.end(); ++i) {
		log4cplus_debug("\tnode %s : %s, self? %s", i->name.c_str(),
				i->addr.c_str(), i->self ? "yes" : "no");
		ServerMigrationState s;
		s.addr = i->addr;
		s.state = i->self ? MS_COMPLETED : MS_NOT_STARTED;
		s.migrated = NULL;
		m_serverMigrationState[i->name] = s;
		if (i->self)
			m_selfName = i->name;
		m_selector.add_node(i->name.c_str());
	}

	save_state_to_file();

	t->prepare_result_no_limit();
	t->turn_around_job_answer();
}

static const char *migrateStateStr[] = {
	"MIGRATION NOT STARTED",
	"MIGRATING",
	"MIGRATION READONLY",
	"MIGRATION COMPLETED",
};

void KeyRouteAskChain::process_get_cluster_state(DTCJobOperation *t)
{
	t->prepare_result_no_limit();
	for (MigrationStateMap::iterator iter = m_serverMigrationState.begin();
	     iter != m_serverMigrationState.end(); ++iter) {
		std::string result = iter->first;
		if (m_selfName == iter->first)
			result += " * ";
		result += "\t";
		result += iter->second.addr;
		result += "\t\t";

		if (iter->second.state < MS_NOT_STARTED ||
		    iter->second.state > MS_COMPLETED)
			result += "!!!INVALID STATE!!!";
		else
			result += migrateStateStr[iter->second.state];

		RowValue row(t->table_definition());
		row[3].Set(result.c_str(), result.size());
		t->append_row(row);
	}

	t->turn_around_job_answer();
}

void KeyRouteAskChain::process_change_node_address(DTCJobOperation *t)
{
	if (!t->request_operation()) {
		t->set_error(-EC_DATA_NEEDED, "Key Route",
			     "name/address needed");
		t->turn_around_job_answer();
		return;
	}

	RowValue row(t->table_definition());
	t->update_row(row);
	std::string name(row[2].bin.ptr, row[2].bin.len);
	std::string addr(row[3].bin.ptr, row[3].bin.len);

	MigrationStateMap::iterator iter = m_serverMigrationState.find(name);
	if (iter == m_serverMigrationState.end()) {
		t->set_error(-EC_BAD_RAW_DATA, "Key Route",
			     "invalid node name");
		t->turn_around_job_answer();
		return;
	}
	if (!ClusterConfig::change_node_address(name, addr)) {
		t->set_error(-EC_SERVER_ERROR, "Key Route",
			     "change node address in config file error");
		t->turn_around_job_answer();
		return;
	}

	iter->second.addr = addr;
	t->prepare_result_no_limit();
	t->turn_around_job_answer();
}

void KeyRouteAskChain::process_node_state_change(DTCJobOperation *t)
{
	if (!t->request_operation()) {
		t->set_error(-EC_DATA_NEEDED, "Key Route", "node name needed.");
		t->turn_around_job_answer();
		return;
	}

	RowValue row(t->table_definition());
	t->update_row(row);
	std::string name(row[2].bin.ptr, row[2].bin.len);
	int newState = row[1].u64;
	MigrationStateMap::iterator iter = m_serverMigrationState.find(name);
	if (newState > MS_COMPLETED || iter == m_serverMigrationState.end()) {
		t->set_error(-EC_BAD_RAW_DATA, "Key Route",
			     "invalid name/state");
		t->turn_around_job_answer();
		return;
	}

	if (iter->second.state == newState) {
		//nothing changed
		t->prepare_result_no_limit();
		t->turn_around_job_answer();
		return;
	}

	if (name == m_selfName) {
		t->set_error(-EC_BAD_RAW_DATA, "Key Route",
			     "can't change self's state");
		t->turn_around_job_answer();
		return;
	}

	switch ((iter->second.state << 4) | newState) {
	case (MS_NOT_STARTED << 4) | MS_MIGRATE_READONLY:
		log4cplus_warning(
			"node[%s] state changed from NOT_STARTED to MIGRATE_READONLY",
			name.c_str());
		//fall through
	case (MS_NOT_STARTED << 4) | MS_MIGRATING:
		iter->second.migrated = new FileBackedKeySet(
			key_list_file_name(name).c_str(),
			t->table_definition()->key_format());
		if (iter->second.migrated->Open() < 0) {
			delete iter->second.migrated;
			iter->second.migrated = NULL;
			t->set_error(-EC_SERVER_ERROR, "Key Route",
				     "open file backed key set failed");
			t->turn_around_job_answer();
			return;
		}
		iter->second.state = newState;
		break;

	case (MS_NOT_STARTED << 4) | MS_COMPLETED:
		//when we add a node to the cluster, only data belongs to the new node need to be migrated
		//other nodes are untouched.
		//this is the case.
		log4cplus_info(
			"node[%s] state changed from NOT_STARTED to COMLETED",
			name.c_str());
		iter->second.state = newState;
		break;

	case (MS_MIGRATING << 4) | MS_NOT_STARTED:
	case (MS_MIGRATE_READONLY << 4) | MS_NOT_STARTED:
		//XXX: should we allow these changes?
		log4cplus_warning(
			"node[%s] state changed from %s to NOT_STARTED!",
			name.c_str(),
			iter->second.state == MS_MIGRATING ?
				"MIGRATING" :
				"MIGRATE_READONLY");
		iter->second.migrated->Clear();
		delete iter->second.migrated;
		iter->second.state = newState;
		break;

	case (MS_MIGRATE_READONLY << 4) | MS_MIGRATING:
		log4cplus_warning(
			"node[%s] state changed from MIGRATE_READONLY to MIGRATING",
			name.c_str());
		//fall through
	case (MS_MIGRATING << 4) | MS_MIGRATE_READONLY:
		//normal change
		iter->second.state = newState;
		break;

	case (MS_MIGRATING << 4) | MS_COMPLETED:
		log4cplus_warning(
			"node[%s] state changed from MIGRATING to COMPLETED",
			name.c_str());
		//fall through
	case (MS_MIGRATE_READONLY << 4) | MS_COMPLETED:
		iter->second.migrated->Clear();
		delete iter->second.migrated;
		iter->second.state = newState;
		break;

	case (MS_COMPLETED << 4) | MS_NOT_STARTED:
		log4cplus_warning(
			"node[%s] state changed from COMPLETED to NOT_STARTED",
			name.c_str());
		iter->second.state = newState;
		break;

	case (MS_COMPLETED << 4) | MS_MIGRATING:
	case (MS_COMPLETED << 4) | MS_MIGRATE_READONLY:
		//XXX: should we allow these changes?
		log4cplus_warning(
			"node[%s] state changed from COMPLATED to %s!",
			name.c_str(),
			newState == MS_MIGRATING ? "MIGRATING" :
						   "MIGRATE_READONLY");
		iter->second.migrated = new FileBackedKeySet(
			key_list_file_name(name).c_str(),
			t->table_definition()->key_format());
		if (iter->second.migrated->Open() < 0) {
			delete iter->second.migrated;
			iter->second.migrated = NULL;
			t->set_error(-EC_SERVER_ERROR, "Key Route",
				     "open file backed key set failed\n");
			t->turn_around_job_answer();
			return;
		}
		iter->second.state = newState;
		break;

	default:
		log4cplus_error("invalid state/new state value : %d --> %d",
				iter->second.state, newState);
		t->set_error(-EC_BAD_RAW_DATA, "Key Route", "invalid state");
		t->turn_around_job_answer();
		return;
	}

	//state changed, save it to file
	save_state_to_file();
	t->prepare_result_no_limit();
	t->turn_around_job_answer();
}

bool KeyRouteAskChain::migration_inprogress()
{
	for (MigrationStateMap::iterator iter = m_serverMigrationState.begin();
	     iter != m_serverMigrationState.end(); ++iter) {
		if (iter->second.state == MS_MIGRATING ||
		    iter->second.state == MS_MIGRATE_READONLY)
			return true;
	}

	return false;
}

static const char *state_file_name = "../data/cluster.stat";

void KeyRouteAskChain::save_state_to_file()
{
	FILE *f = fopen(state_file_name, "wb");
	if (!f) {
		log4cplus_error("open %s for write failed, %m",
				state_file_name);
		return;
	}

	int n = m_serverMigrationState.size();
	fwrite(&n, sizeof(n), 1, f);
	for (MigrationStateMap::iterator iter = m_serverMigrationState.begin();
	     iter != m_serverMigrationState.end(); ++iter) {
		int len = iter->first.size();
		fwrite(&len, sizeof(len), 1, f);
		fwrite(iter->first.c_str(), 1, len, f);
		fwrite(&iter->second.state, sizeof(iter->second.state), 1, f);
	}

	fclose(f);
}

void KeyRouteAskChain::init(const std::vector<ClusterConfig::ClusterNode> &nodes)
{
	for (std::vector<ClusterConfig::ClusterNode>::const_iterator i =
		     nodes.begin();
	     i != nodes.end(); ++i) {
		log4cplus_info("\tnode %s : %s, self? %s", i->name.c_str(),
			       i->addr.c_str(), i->self ? "yes" : "no");
		ServerMigrationState s;
		s.addr = i->addr;
		s.state = i->self ? MS_COMPLETED : MS_NOT_STARTED;
		s.migrated = NULL;
		m_serverMigrationState[i->name] = s;
		if (i->self)
			m_selfName = i->name;
		m_selector.add_node(i->name.c_str());
	}
}

int KeyRouteAskChain::load_node_state_if_any()
{
	FILE *f = fopen(state_file_name, "rb");
	if (!f) //it is ok if the state file is not there
		return 0;

	log4cplus_info("begin loading stat file ...");

	int n = 0;
	assert(fread(&n, sizeof(n), 1, f) == 1);
	if (n != (int)m_serverMigrationState.size()) {
		log4cplus_error("state file and config file mismatch!");
		fclose(f);
		return -1;
	}

	char buf[256]; //name can't exceeds 255
	for (int i = 0; i < n; ++i) {
		int len = 0;
		assert(fread(&len, sizeof(len), 1, f) == 1);
		if (len == 0 || len > (int)sizeof(buf) - 1) {
			log4cplus_error("invalid state file!");
			fclose(f);
			return -1;
		}
		assert((int)fread(buf, 1, len, f) == len);
		buf[len] = 0;
		int state = 0;
		assert(fread(&state, sizeof(state), 1, f) == 1);
		if (state < MS_NOT_STARTED || state > MS_COMPLETED) {
			log4cplus_error("invalid state in state file!");
			fclose(f);
			return -1;
		}

		MigrationStateMap::iterator iter =
			m_serverMigrationState.find(buf);
		if (iter == m_serverMigrationState.end()) {
			log4cplus_error(
				"name %s in state file doesn't exist in config!",
				buf);
			fclose(f);
			return -1;
		}

		iter->second.state = state;
		log4cplus_info("%s %s", iter->first.c_str(),
			       migrateStateStr[state]);
		if (state == MS_MIGRATING || state == MS_MIGRATE_READONLY) {
			iter->second.migrated = new FileBackedKeySet(
				key_list_file_name(iter->first).c_str(),
				m_keyFormat);
			if (iter->second.migrated->Load() < 0) {
				log4cplus_error(
					"load %s migrated key list failed",
					buf);
				fclose(f);
				return -1;
			}
		}
	}

	//we should got EOF
	assert(fread(buf, 1, 1, f) == 0);
	fclose(f);

	log4cplus_info("load state file completed.");
	return 0;
}

void KeyRouteAskChain::process_cascade(DTCJobOperation *t)
{
	if (!t->request_operation()) {
		t->set_error(-EC_DATA_NEEDED, "Key Route",
			     "cascade addr needed.");
		t->turn_around_job_answer();
		return;
	}

	RowValue row(t->table_definition());
	t->update_row(row);
	std::string strAddr(row[2].bin.ptr, row[2].bin.len);
	int newState = row[1].u64;

	if (newState < CS_NOT_STARTED || newState >= CS_MAX) {
		log4cplus_error(
			"cascade mistach invalid state/new state value : %d --> %d",
			m_iCSState, newState);
		t->set_error(-EC_BAD_RAW_DATA, "Key Route",
			     "mistach invalid state");
		t->turn_around_job_answer();
		return;
	}

	if (m_iCSState == newState) {
		log4cplus_error(
			"nothing to changed state/new state value : %d --> %d!",
			m_iCSState, newState);
		t->prepare_result_no_limit();
		t->turn_around_job_answer();
		return;
	}

	switch ((m_iCSState << 4) | newState) {
	case (CS_NOT_STARTED << 4) | CS_CASCADING: {
		log4cplus_error(
			"switch cascade state CS_NOT_STARTED to CS_CASCADING, addr [%s]",
			strAddr.c_str());
		m_iCSState = newState;
		m_strCSAddr = strAddr;
		break;
	}
	case (CS_CASCADING << 4) | CS_NOT_STARTED: {
		log4cplus_error(
			"switch cascade state CS_CASCADING to CS_NOT_STARTED, addr [%s]",
			strAddr.c_str());
		m_iCSState = newState;
		//When switching to unstarted state, set the remote address to empty
		m_strCSAddr = "";
		break;
	}
	default: {
		log4cplus_error(
			"cascade invalid state/new state value : %d --> %d",
			m_iCSState, newState);
		t->set_error(-EC_BAD_RAW_DATA, "Key Route", "invalid state");
		t->turn_around_job_answer();
		return;
	}
	}
	t->prepare_result_no_limit();
	t->turn_around_job_answer();
	return;
}
