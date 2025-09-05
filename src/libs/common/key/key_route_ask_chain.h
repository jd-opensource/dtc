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
#ifndef __KEY_ROUTE_ASK_CHAIN__
#define __KEY_ROUTE_ASK_CHAIN__

#include <map>
#include <string>
#include <vector>

#include "request/request_base.h"
#include "../task/task_request.h"
#include "consistent_hash_selector.h"
#include "config/parse_cluster_config.h"

class FileBackedKeySet;

class KeyRouteAskChain : public JobAskInterface<DTCJobOperation> {
    public:
	KeyRouteAskChain(PollerBase *owner, int keyFormat);

	void bind_remote_helper(JobAskInterface<DTCJobOperation> *r)
	{
		remote_chain.register_next_chain(r);
	}
	void bind_cache(JobAskInterface<DTCJobOperation> *c)
	{
		main_chain.register_next_chain(c);
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}
	ChainJoint<DTCJobOperation> *get_remote_chain()
	{
		return &remote_chain;
	}
	int key_migrated(const char *key);
	int key_migrating(const char *key);

	void init(const std::vector<ClusterConfig::ClusterNode> &nodes);

	void job_ask_procedure(DTCJobOperation *t);

	int load_node_state_if_any();

    private:
	ChainJoint<DTCJobOperation> main_chain;
	ChainJoint<DTCJobOperation> remote_chain;
	int m_keyFormat;

	void process_reload(DTCJobOperation *t);
	void process_node_state_change(DTCJobOperation *t);
	void process_change_node_address(DTCJobOperation *t);
	void process_migrate(DTCJobOperation *t);
	void process_get_cluster_state(DTCJobOperation *t);
	bool accept_key(const std::string &node, const char *key, int len);
	bool is_migrating(const std::string &node, const char *key, int len);
	bool is_same_cluster_config(
		std::map<std::string, std::string> &dtcClusterMap,
		const std::string &strDtcName);
	const char *name_to_addr(const std::string &node)
	{
		return m_serverMigrationState[node].addr.c_str();
	}

	std::string key_list_file_name(const std::string &name)
	{
		return "../data/" + name + ".migrated";
	}

	std::string select_node(const char *key);

	bool migration_inprogress();
	void save_state_to_file();
	void process_cascade(DTCJobOperation *t);

	enum MigrationState {
		MS_NOT_STARTED,
		MS_MIGRATING,
		MS_MIGRATE_READONLY,
		MS_COMPLETED,
	};

	struct ServerMigrationState {
		std::string addr;
		int state;
		FileBackedKeySet *migrated;

		ServerMigrationState() : state(MS_NOT_STARTED), migrated(NULL)
		{
		}
	};

	typedef std::map<std::string, ServerMigrationState> MigrationStateMap;
	MigrationStateMap m_serverMigrationState;
	std::string m_selfName;
	ConsistentHashSelector m_selector;

	//Cascade state enum values, cannot duplicate with migration state enum values above
	enum CascadeState {
		CS_NOT_STARTED = 100,
		CS_CASCADING,
		CS_MAX,
	};
	//This DTC cascade state
	int m_iCSState;
	//Cascade remote DTC address, e.g.: 10.191.147.188:12000/tcp
	std::string m_strCSAddr;
};

#endif
