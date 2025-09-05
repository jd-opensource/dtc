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
#include <alloca.h>
#include <cstring>
#include <cstdio>
#include <cassert>

#include <algorithm>

#include "consistent_hash_selector.h"
#include "log/log.h"

const std::string &ConsistentHashSelector::Select(uint32_t hash)
{
	static std::string empty;
	if (m_nodes.empty())
		return empty;

	std::map<uint32_t, int>::iterator iter = m_nodes.upper_bound(hash);
	if (iter != m_nodes.end())
		return m_nodeNames[iter->second];
	return m_nodeNames[m_nodes.begin()->second];
}

void ConsistentHashSelector::add_node(const char *name)
{
	if (find(m_nodeNames.begin(), m_nodeNames.end(), name) !=
	    m_nodeNames.end()) {
		log4cplus_error("duplicate node name: %s in ClusterConfig",
				name);
		abort();
	}
	m_nodeNames.push_back(name);
	int index = m_nodeNames.size() - 1;
	char *buf = (char *)alloca(strlen(name) + 16);
	for (int i = 0; i < VIRTUAL_NODE_COUNT; ++i) {
		snprintf(buf, strlen(name) + 16, "%s#%d", name, i);
		uint32_t value = Hash(buf, strlen(buf));
		std::map<uint32_t, int>::iterator iter = m_nodes.find(value);
		if (iter != m_nodes.end()) {
			// Hash value conflict, select the smaller string
			if (m_nodeNames[iter->second] < name)
				continue;
		}
		m_nodes[value] = index;
	}
}
