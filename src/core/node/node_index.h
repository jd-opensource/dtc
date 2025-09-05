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

#ifndef __DTC_NODE_INDEX_H
#define __DTC_NODE_INDEX_H

#include "namespace.h"
#include "global.h"

DTC_BEGIN_NAMESPACE

#define INDEX_1_SIZE                                                           \
	(((1UL << 8) * sizeof(MEM_HANDLE_T)) +                                 \
	 sizeof(FIRST_INDEX_T)) // first-index size
#define INDEX_2_SIZE                                                           \
	(((1UL << 16) * sizeof(MEM_HANDLE_T)) +                                \
	 sizeof(SECOND_INDEX_T)) // second-index size

#define OFFSET1(id) ((id) >> 24) // High 8 bits, first-level index
#define OFFSET2(id) (((id)&0xFFFF00) >> 8) // Middle 16 bits, second-level index
#define OFFSET3(id) ((id)&0xFF) // Low 8 bits

struct first_index {
	uint32_t fi_used; // Number of first-level index entries used
	MEM_HANDLE_T fi_h[0]; // Handles for storing second-level index
};
typedef struct first_index FIRST_INDEX_T;

struct second_index {
	uint32_t si_used;
	MEM_HANDLE_T si_h[0];
};
typedef struct second_index SECOND_INDEX_T;

class Node;
class NodeIndex {
    public:
	NodeIndex();
	~NodeIndex();

	static NodeIndex *instance();
	static void destroy();

	int do_insert(Node);
	Node do_search(NODE_ID_T id);

	int pre_allocate_index(size_t size);

	const MEM_HANDLE_T get_handle() const
	{
		return M_HANDLE(_firstIndex);
	}
	const char *error() const
	{
		return errmsg_;
	}
	///* Memory block operation functions */
	int do_init(size_t mem_size);
	int do_attach(MEM_HANDLE_T handle);
	int do_detach(void);

    private:
	FIRST_INDEX_T *_firstIndex;
	char errmsg_[256];
};

DTC_END_NAMESPACE

#endif