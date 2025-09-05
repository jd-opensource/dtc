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

#ifndef __DTC_NG_INFO_H
#define __DTC_NG_INFO_H

#include <stdint.h>
#include "stat_dtc.h"
#include "algorithm/singleton.h"
#include "namespace.h"
#include "global.h"
#include "ng_list.h"

DTC_BEGIN_NAMESPACE

/* Cache types supported by high-level layer */
enum MEM_CACHE_TYPE_T {
	MEM_DTC_TYPE = 0x1UL,
	MEM_BMP_TYPE = 0x2UL,
};

/* Signature, version, type, etc. of high-level cache */
#define MEM_CACHE_SIGN 0xFF00FF00FF00FF00ULL
#define MEM_CACHE_VERSION 0x1ULL
#define MEM_CACHE_TYPE MEM_DTC_TYPE

struct cache_info {
	uint64_t ci_sign;
	uint64_t ci_version;
	uint64_t ci_type;
};
typedef struct cache_info CACHE_INFO_T;

/* Low-Level reserves 4k space for future expansion */
/* TODO: Add more detailed logic judgment */
struct app_storage {
	CACHE_INFO_T as_cache_info;
	MEM_HANDLE_T as_extend_info;

	int need_format()
	{
		return (as_cache_info.ci_sign != MEM_CACHE_SIGN) ||
		       (INVALID_HANDLE == as_extend_info);
	}

	int do_format(MEM_HANDLE_T v)
	{
		as_cache_info.ci_sign = MEM_CACHE_SIGN;
		as_cache_info.ci_version = MEM_CACHE_VERSION;
		as_cache_info.ci_type = MEM_DTC_TYPE;

		as_extend_info = v;
		return 0;
	}
};
typedef struct app_storage APP_STORAGE_T;

struct ng_info {
	NG_LIST_T ni_free_head; // NG linked list with free Nodes
	NG_LIST_T ni_full_head; // NG linked list with all Nodes allocated
	NODE_ID_T ni_min_id; // Starting NodeId of the next allocated NG
	MEM_HANDLE_T ni_sys_zone; // First NG is reserved for system

	/* The following are statistics used to control asynchronous flush start/stop, speed, etc. */
	uint32_t ni_used_ng;
	uint32_t ni_used_node;
	uint32_t ni_dirty_node;
	uint64_t ni_used_row;
	uint64_t ni_dirty_row;
};
typedef struct ng_info NG_INFO_T;

class NGInfo {
    public:
	NGInfo();
	~NGInfo();

	static NGInfo *instance()
	{
		return Singleton<NGInfo>::instance();
	}
	static void destroy()
	{
		Singleton<NGInfo>::destory();
	}

	Node allocate_node(void); // Allocate a new Node
	int release_node(Node &); // Return CNode to its owning NG and destroy itself

	/*statistic, for async flush */
	void inc_dirty_node(int v)
	{
		nodegroup_info_->ni_dirty_node += v;
		stat_dirty_node = nodegroup_info_->ni_dirty_node;
	}
	void inc_dirty_row(int v)
	{
		nodegroup_info_->ni_dirty_row += v;
		stat_dirty_row = nodegroup_info_->ni_dirty_row;
	}
	void inc_total_row(int v)
	{
		nodegroup_info_->ni_used_row += v;
		stat_used_row = nodegroup_info_->ni_used_row;
	}
	void inc_empty_node(int v)
	{
		empty_node_count += v;
		stat_empty_node = empty_node_count;
	}

	const unsigned int total_dirty_node() const
	{
		return nodegroup_info_->ni_dirty_node;
	}
	const unsigned int get_total_used_node() const
	{
		return nodegroup_info_->ni_used_node;
	}

	const uint64_t total_dirty_row() const
	{
		return nodegroup_info_->ni_dirty_row;
	}
	const uint64_t total_used_row() const
	{
		return nodegroup_info_->ni_used_row;
	}

	Node dirty_node_head();
	Node clean_node_head();
	Node empty_node_head();

	/* Get the minimum available NodeID */
	NODE_ID_T get_min_valid_node_id() const
	{
		return (NODE_ID_T)256;
	}

	/* Get the currently allocated maximum NodeID */
	/* Since the current node-group size is fixed and will not be released after allocation, it can be calculated directly through the used node-group */
	NODE_ID_T max_node_id() const
	{
		return nodegroup_info_->ni_used_ng * 256 - 1;
	}

	//time-list op
	int insert_to_dirty_lru(Node);
	int insert_to_clean_lru(Node);
	int insert_to_empty_lru(Node);
	int remove_from_lru(Node);
	int empty_count(void) const
	{
		return empty_node_count;
	}
	enum { CREATED, // this memory is fresh
	       ATTACHED, // this is an old memory, and empty lru present
	       UPGRADED // this is an old memory, and empty lru is missing
	};
	int get_empty_startup_mode(void) const
	{
		return empty_startup_mode;
	}

	const MEM_HANDLE_T get_handle() const
	{
		return M_HANDLE(nodegroup_info_);
	}
	const char *error() const
	{
		return errmsg_;
	}

	// Create physical memory and format
	int do_init(void);
	// Bind to physical memory
	int do_attach(MEM_HANDLE_T handle);
	// Detach from physical memory
	int do_detach(void);

    protected:
	int init_header(NG_INFO_T *);

	NODE_SET *allocate_ng(void);
	NODE_SET *find_free_ng(void);

	void list_del(NODE_SET *);
	void free_list_add(NODE_SET *);
	void full_list_add(NODE_SET *);
	void full_list_add_tail(NODE_SET *);
	void free_list_add_tail(NODE_SET *);

    private:
	NG_INFO_T *nodegroup_info_;
	char errmsg_[256];
	// the total empty node present
	int empty_node_count;
	int empty_startup_mode;

    private:
	StatCounter stat_used_nodegroup;
	StatCounter stat_used_node;
	StatCounter stat_dirty_node;
	StatCounter stat_empty_node;
	StatCounter stat_used_row;
	StatCounter stat_dirty_row;
};

DTC_END_NAMESPACE

#endif
