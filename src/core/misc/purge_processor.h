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

#ifndef __DTC_PURGE_NODE_PROCESSOR_H
#define __DTC_PURGE_NODE_PROCESSOR_H

#include <stddef.h>

#include "node/node.h"

DTC_BEGIN_NAMESPACE

class PurgeNodeProcessor {
    public:
	PurgeNodeProcessor(){};
	virtual ~PurgeNodeProcessor(){};
	virtual void purge_node_processor(const char *key, Node node) = 0;
};

DTC_END_NAMESPACE

#endif
