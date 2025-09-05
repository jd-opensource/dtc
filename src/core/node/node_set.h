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

#ifndef __DTC_NODE_SET_H
#define __DTC_NODE_SET_H

#include <stdint.h>
#include "namespace.h"
#include "global.h"
#include "nodegroup/ng_list.h"

DTC_BEGIN_NAMESPACE

enum attr_type {
	NEXT_NODE = 0,
	TIME_LIST = 1,
	VD_HANDLE = 2,
	DIRTY_BMP = 3,
};
typedef enum attr_type ATTR_TYPE_T;

// Node list freed by nodeset
struct ng_delete {
	uint16_t top;
	uint16_t count;
};
typedef struct ng_delete NG_DELE_T;

// Nodeset attributes
struct ng_attr {
	uint32_t count;
	uint32_t offset[0];
};
typedef struct ng_attr NG_ATTR_T;

class Node;
struct node_set {
    public:
	NG_LIST_T ng_list;
	NG_DELE_T ng_dele;
	uint16_t ng_free;
	uint8_t ng_rsv[2]; // Reserved space
	NODE_ID_T ng_nid;
	NG_ATTR_T ng_attr;

    private:
	Node allocate_node(void); // Allocate a Node
	int release_node(Node); // Release a Node
	bool is_full(void); // Whether NodeGroup is fully allocated
	int do_init(NODE_ID_T id); // NodeGroup initialization
	int system_reserved_init(); // System reserved NG initialization
	// this routine return:
	//    0,  passed, empty lru present
	//    1,  passed, empty lru created
	//    <0, integrity error
	int system_reserved_check(); // System reserved NG consistency check
	static uint32_t Size(void); // Return total size of nodegroup

    private:
	// Attribute operation interface for CNode access
	NODE_ID_T node_id(int idx) const;
	NODE_ID_T &next_node_id(int idx); // attr1]   -> NodeID of next Node
	NODE_ID_T *node_lru(int idx); // attr[2]   -> LRU list
	MEM_HANDLE_T &vd_handle(int idx); // attr[3]   -> Data handle
	bool is_dirty(int idx); // attr[4]   -> Dirty bitmap
	void set_dirty(int idx);
	void clr_dirty(int idx);

	// Return start address of each attribute block
	template <class T> T *__CAST__(ATTR_TYPE_T t)
	{
		return (T *)((char *)this + ng_attr.offset[t]);
	}

    private:
	static uint32_t attr_count(void); // Number of supported attributes
	static uint32_t attr_size(void); // Memory size of all attributes
	static uint32_t base_header_size(void); // Size of Nodegroup excluding attributes
	static const uint32_t NG_ATTR_SIZE[];

	friend class Node;
	friend class NGInfo;
};
typedef struct node_set NODE_SET;

DTC_END_NAMESPACE

#endif
