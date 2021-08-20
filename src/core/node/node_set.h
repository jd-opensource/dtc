/*
* Copyright [2021] JD.com, Inc.
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

//nodeset释放掉的node链表
struct ng_delete {
	uint16_t top;
	uint16_t count;
};
typedef struct ng_delete NG_DELE_T;

//nodeset属性
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
	uint8_t ng_rsv[2]; //保留空间
	NODE_ID_T ng_nid;
	NG_ATTR_T ng_attr;

    private:
	Node allocate_node(void); // 分配一个Node
	int release_node(Node); // 释放一个Node
	bool is_full(void); // NodeGroup是否已经分配完
	int do_init(NODE_ID_T id); // NodeGroup初始化
	int system_reserved_init(); // 系统保留的NG初始化
	// this routine return:
	//    0,  passed, empty lru present
	//    1,  passed, empty lru created
	//    <0, integrity error
	int system_reserved_check(); // 系统保留的NG一致性检查
	static uint32_t Size(void); // 返回nodegroup的总大小

    private:
	//属性操作接口，供CNode访问
	NODE_ID_T node_id(int idx) const;
	NODE_ID_T &next_node_id(int idx); // attr1]   -> 下一个Node的NodeID
	NODE_ID_T *node_lru(int idx); // attr[2]   -> LRU链表
	MEM_HANDLE_T &vd_handle(int idx); // attr[3]   -> 数据handle
	bool is_dirty(int idx); // attr[4]   -> 脏位图
	void set_dirty(int idx);
	void clr_dirty(int idx);

	//返回每种属性块的起始地址
	template <class T> T *__CAST__(ATTR_TYPE_T t)
	{
		return (T *)((char *)this + ng_attr.offset[t]);
	}

    private:
	static uint32_t attr_count(void); // 支持的属性个数
	static uint32_t attr_size(void); // 所有属性的内存大小
	static uint32_t base_header_size(void); // 除开属性外，Nodegroup的大小
	static const uint32_t NG_ATTR_SIZE[];

	friend class Node;
	friend class NGInfo;
};
typedef struct node_set NODE_SET;

DTC_END_NAMESPACE

#endif
