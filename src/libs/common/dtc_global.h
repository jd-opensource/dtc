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
#ifndef _DTC_GLOBAL_H_
#define _DTC_GLOBAL_H_
#include "algorithm/non_copyable.h"

#define TABLE_CONF_NAME "../conf/dtc.yaml"
#define CACHE_CONF_NAME "../conf/dtc.yaml"
#define ALARM_CONF_FILE "../conf/dtcalarm.conf"

class DTCGlobal : private noncopyable {
    public:
	DTCGlobal(void);
	~DTCGlobal(void);

    public:
	static int pre_alloc_nodegroup_count;
	static int min_chunk_size_;
	static int pre_purge_nodes_;
};
#endif
