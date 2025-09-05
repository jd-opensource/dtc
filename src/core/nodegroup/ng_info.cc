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

#include <string.h>
#include <stdio.h>
#include "node_set.h"
#include "node_list.h"
#include "node_index.h"
#include "ng_info.h"
#include "node.h"
#include "dtc_global.h"

DTC_USING_NAMESPACE

NGInfo::NGInfo() : nodegroup_info_(NULL)
{
	memset(errmsg_, 0, sizeof(errmsg_));
	empty_node_count = 0;
	empty_startup_mode = CREATED;

	stat_used_nodegroup = g_stat_mgr.get_stat_int_counter(DTC_USED_NGS);
	stat_used_node = g_stat_mgr.get_stat_int_counter(DTC_USED_NODES);
	stat_dirty_node = g_stat_mgr.get_stat_int_counter(DTC_DIRTY_NODES);
	stat_empty_node = g_stat_mgr.get_stat_int_counter(DTC_EMPTY_NODES);
	stat_empty_node = 0;
	stat_used_row = g_stat_mgr.get_stat_int_counter(DTC_USED_ROWS);
	stat_dirty_row = g_stat_mgr.get_stat_int_counter(DTC_DIRTY_ROWS);
}

NGInfo::~NGInfo()
{
}

Node NGInfo::allocate_node(void)
{
	// Prioritize allocation in free list
	NODE_SET *NS = find_free_ng();
	if (!NS) {
		/* Prevent NodeGroup from fragmenting memory, using pre-allocation */
		static int step = DTCGlobal::pre_alloc_nodegroup_count;
		static int fail = 0;
		for (int i = 0; i < step; i++) {
			NS = allocate_ng();
			if (!NS) {
				if (i == 0)
					return Node();
				else {
					fail = 1;
					step = 1;
					break;
				}
			}

			free_list_add(NS);
		}

		/* find again */
		NS = find_free_ng();

		if (step < 256 && !fail)
			step *= 2;
	}

	Node node = NS->allocate_node();
	// There are no allocatable Nodes in NG
	if (NS->is_full()) {
		list_del(NS);
		full_list_add(NS);
	}

	if (!node) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "PANIC: allocate node failed");
		return Node();
	}

	//statistic
	nodegroup_info_->ni_used_node++;
	stat_used_node = nodegroup_info_->ni_used_node;

	//insert to node_index
	I_INSERT(node);
	return node;
}

int NGInfo::release_node(Node &node)
{
	NODE_SET *NS = node.Owner();
	if (NS->is_full()) {
		// NG hangs into free list
		list_del(NS);
		free_list_add(NS);
	}

	nodegroup_info_->ni_used_node--;
	stat_used_node = nodegroup_info_->ni_used_node;
	return node.Release();
}

Node NGInfo::dirty_node_head()
{
	NODE_SET *sysNG = M_POINTER(NODE_SET, nodegroup_info_->ni_sys_zone);
	if (!sysNG)
		return Node();
	return Node(sysNG, SYS_DIRTY_NODE_INDEX);
}

Node NGInfo::clean_node_head()
{
	NODE_SET *sysNG = M_POINTER(NODE_SET, nodegroup_info_->ni_sys_zone);
	if (!sysNG)
		return Node();
	return Node(sysNG, SYS_CLEAN_NODE_INDEX);
}

Node NGInfo::empty_node_head()
{
	NODE_SET *sysNG = M_POINTER(NODE_SET, nodegroup_info_->ni_sys_zone);
	if (!sysNG)
		return Node();
	return Node(sysNG, SYS_EMPTY_NODE_INDEX);
}

int NGInfo::insert_to_dirty_lru(Node node)
{
	NODE_SET *sysNG = M_POINTER(NODE_SET, nodegroup_info_->ni_sys_zone);
	Node dirtyNode(sysNG, SYS_DIRTY_NODE_INDEX);

	NODE_LIST_ADD(node, dirtyNode);

	return 0;
}

int NGInfo::insert_to_clean_lru(Node node)
{
	NODE_SET *sysNG = M_POINTER(NODE_SET, nodegroup_info_->ni_sys_zone);
	Node cleanNode(sysNG, SYS_CLEAN_NODE_INDEX);

	NODE_LIST_ADD(node, cleanNode);

	return 0;
}

int NGInfo::insert_to_empty_lru(Node node)
{
	NODE_SET *sysNG = M_POINTER(NODE_SET, nodegroup_info_->ni_sys_zone);
	Node emptyNode(sysNG, SYS_EMPTY_NODE_INDEX);

	NODE_LIST_ADD(node, emptyNode);

	return 0;
}

int NGInfo::remove_from_lru(Node node)
{
	NODE_LIST_DEL(node);
	return 0;
}

NODE_SET *NGInfo::allocate_ng(void)
{
	MEM_HANDLE_T v = M_CALLOC(NODE_SET::Size());
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "allocate nodegroup failed, %s", M_ERROR());
		return (NODE_SET *)0;
	}

	NODE_SET *NS = M_POINTER(NODE_SET, v);
	NS->do_init(nodegroup_info_->ni_min_id);
	nodegroup_info_->ni_min_id += NODE_GROUP_INCLUDE_NODES;
	nodegroup_info_->ni_used_ng++;
	stat_used_nodegroup = nodegroup_info_->ni_used_ng;

	return NS;
}

NODE_SET *NGInfo::find_free_ng(void)
{
	// List is empty
	if (NG_LIST_EMPTY(&(nodegroup_info_->ni_free_head))) {
		return (NODE_SET *)0;
	}

	return NG_LIST_ENTRY(nodegroup_info_->ni_free_head.Next(), NODE_SET,
			     ng_list);
}

void NGInfo::list_del(NODE_SET *NS)
{
	NG_LIST_T *p = &(NS->ng_list);
	return NG_LIST_DEL(p);
}

#define EXPORT_NG_LIST_FUNCTION(name, member, function)                        \
	void NGInfo::name(NODE_SET *NS)                                        \
	{                                                                      \
		NG_LIST_T *p = &(NS->ng_list);                                 \
		NG_LIST_T *head = &(nodegroup_info_->member);                  \
		return function(p, head);                                      \
	}

EXPORT_NG_LIST_FUNCTION(free_list_add, ni_free_head, NG_LIST_ADD)
EXPORT_NG_LIST_FUNCTION(full_list_add, ni_full_head, NG_LIST_ADD)
EXPORT_NG_LIST_FUNCTION(free_list_add_tail, ni_free_head, NG_LIST_ADD_TAIL)
EXPORT_NG_LIST_FUNCTION(full_list_add_tail, ni_full_head, NG_LIST_ADD_TAIL)

int NGInfo::init_header(NG_INFO_T *ni)
{
	INIT_NG_LIST_HEAD(&(ni->ni_free_head));
	INIT_NG_LIST_HEAD(&(ni->ni_full_head));

	ni->ni_min_id = SYS_MIN_NODE_ID;

	/* init system reserved zone*/
	{
		NODE_SET *sysNG = allocate_ng();
		if (!sysNG)
			return -1;

		sysNG->system_reserved_init();
		ni->ni_sys_zone = M_HANDLE(sysNG);
	}

	ni->ni_used_ng = 1;
	ni->ni_used_node = 0;
	ni->ni_dirty_node = 0;
	ni->ni_used_row = 0;
	ni->ni_dirty_row = 0;

	stat_used_nodegroup = ni->ni_used_ng;
	stat_used_node = ni->ni_used_node;
	stat_dirty_node = ni->ni_dirty_node;
	stat_dirty_row = ni->ni_dirty_row;
	stat_used_row = ni->ni_used_row;
	stat_empty_node = 0;

	return 0;
}

int NGInfo::do_init(void)
{
	//1. malloc ng_info mem.
	MEM_HANDLE_T v = M_CALLOC(sizeof(NG_INFO_T));
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_), "init nginfo failed, %s",
			 M_ERROR());
		return -1;
	}

	//2. mapping
	nodegroup_info_ = M_POINTER(NG_INFO_T, v);

	//3. init header
	return init_header(nodegroup_info_);
}

int NGInfo::do_attach(MEM_HANDLE_T v)
{
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "attach nginfo failed, memory handle = 0");
		return -1;
	}

	nodegroup_info_ = M_POINTER(NG_INFO_T, v);

	/* check system reserved zone:
     *   1. the present of empty lru list
     */
	{
		NODE_SET *sysNG =
			M_POINTER(NODE_SET, nodegroup_info_->ni_sys_zone);
		if (!sysNG)
			return -1;

		int ret = sysNG->system_reserved_check();
		if (ret < 0)
			return ret;
		if (ret > 0) {
			empty_startup_mode = UPGRADED;
		} else {
			empty_startup_mode = ATTACHED;
		}
	}

	return 0;
}

int NGInfo::do_detach(void)
{
	nodegroup_info_ = NULL;
	return 0;
}
