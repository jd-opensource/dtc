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
#ifndef CONSISTENT_HASH_SELECTOR_H__
#define CONSISTENT_HASH_SELECTOR_H__

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include "algorithm/chash.h"

class ConsistentHashSelector {
    public:
	uint32_t Hash(const char *key, int len)
	{
		return chash(key, len);
	}
	const std::string &Select(uint32_t hash);
	const std::string &Select(const char *key, int len)
	{
		return Select(Hash(key, len));
	}

	static const int VIRTUAL_NODE_COUNT = 100;
	void add_node(const char *name);
	void Clear()
	{
		m_nodes.clear();
		m_nodeNames.clear();
	}

    private:
	std::map<uint32_t, int> m_nodes;
	std::vector<std::string> m_nodeNames;
};

#endif
