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

#ifndef __DTC_BUFFER_POOL_H
#define __DTC_BUFFER_POOL_H

#include <stddef.h>

#include "stat_dtc.h"
#include "namespace.h"
#include "mem/pt_malloc.h"
#include "shmem.h"
#include "global.h"
#include "node/node_list.h"
#include "node/node_index.h"
#include "node/node_set.h"
#include "mem/feature.h"
#include "nodegroup/ng_info.h"
#include "algorithm/hash.h"
#include "data/col_expand.h"
#include "node/node.h"
#include "timer/timer_list.h"
#include "misc/purge_processor.h"
#include "data/data_chunk.h"

DTC_BEGIN_NAMESPACE

//time-marker node in dirty lru list
#define TIME_MARKER_NEXT_NODE_ID (INVALID_NODE_ID - 1)

//cache basic information
typedef struct _BlockProperties {
	// shared memory key
	int ipc_mem_key;
	// shared memory size
	uint64_t ipc_mem_size;
	// key size
	unsigned short key_size;
	// memory version number
	unsigned char version;
	// synchronous/asynchronous mode
	unsigned char sync_update : 1;
	// open in read-only mode
	unsigned char read_only : 1;
	// for mem_tool usage
	unsigned char create_only : 1;
	// whether to enable empty node filtering function
	unsigned char empty_filter : 1;
	// whether to automatically delete and rebuild shared memory when incomplete memory is detected
	unsigned char auto_delete_dirty_shm : 1;
	// whether to force update shared memory configuration using table.conf
	unsigned char force_update_table_conf : 1;

	inline void init(int key_format, unsigned long cache_size,
			 unsigned int create_version)
	{
		// calculate buckettotal
		key_size = key_format;
		ipc_mem_size = cache_size;
		version = create_version;
	}

} BlockProperties;

class BufferPond : private TimerObject {
    protected:
	PurgeNodeProcessor *_purge_processor;
	//shared memory manager
	SharedMemory _shm;
	//cache basic information
	BlockProperties _cache_info;
	//hash bucket
	DTCHash *_hash;
	//node management
	NGInfo *_ng_info;
	//feature abstraction
	Feature *_feature;
	//NodeID conversion
	NodeIndex *_node_index;
	//column expansion
	DTCColExpand *_col_expand;

	char _err_msg[512];
	int _need_set_integrity;
	//number of nodes to be eliminated
	unsigned _need_purge_node_count;

	TimerList *_delay_purge_timerlist;
	unsigned first_marker_time;
	unsigned last_marker_time;
	int empty_limit;
	//for purge alert
	int _disable_try_purge;
	//alert if the last update time of automatically eliminated data is less than current time minus DataExpireAlertTime
	int date_expire_alert_time;

    protected:
	//statistics
	StatCounter stat_cache_size;
	StatCounter stat_cache_key;
	StatCounter stat_cache_version;
	StatCounter stat_update_mode;
	StatCounter stat_empty_filter;
	StatCounter stat_hash_size;
	StatCounter stat_free_bucket;
	StatCounter stat_dirty_eldest;
	StatCounter stat_dirty_age;
	StatSample stat_try_purge_count;
	StatCounter stat_try_purge_nodes;
	//maximum value of lastcmod of the last eliminated node (if multiple rows)
	StatCounter stat_last_purge_node_mod_time;
	//current time minus statLastPurgeNodeModTime
	StatCounter stat_data_exist_time;
	StatSample survival_hour;
	StatSample stat_purge_for_create_update_count;

    private:
	int app_storage_open();
	int dtc_mem_open(APP_STORAGE_T *);
	int dtc_mem_attach(APP_STORAGE_T *);
	int dtc_mem_init(APP_STORAGE_T *);
	int verify_cache_info(BlockProperties *);
	unsigned int hash_bucket_num(uint64_t);

	int remove_from_hash_base(const char *key, Node node, int new_hash);
	int remove_from_hash(const char *key, Node node);
	int move_to_new_hash(const char *key, Node node);
	int insert_to_hash(const char *key, Node node);

	int purge_node(const char *key, Node purge_node);
	int purge_node_and_data(const char *key, Node purge_node);

	//purge alert
	int purge_node_with_alert(Node purge_node);
	uint32_t get_cmodtime(Node *purge_node);

	uint32_t get_expire_time(Node *node, uint32_t &expire);

	//lru list op
	int insert_to_dirty_lru(Node node)
	{
		return _ng_info->insert_to_dirty_lru(node);
	}
	int insert_to_clean_lru(Node node)
	{
		return _ng_info->insert_to_clean_lru(node);
	}
	int insert_to_empty_lru(Node node)
	{
		return empty_limit ? _ng_info->insert_to_empty_lru(node) :
				     _ng_info->insert_to_clean_lru(node);
	}
	int remove_from_lru(Node node)
	{
		return _ng_info->remove_from_lru(node);
	}
	int key_cmp(const char *key, const char *other);

	//node|row count statistic for async flush.
	void inc_dirty_node(int v)
	{
		_ng_info->inc_dirty_node(v);
	}
	void inc_dirty_row(int v)
	{
		_ng_info->inc_dirty_row(v);
	}
	void dec_empty_node(void)
	{
		if (empty_limit)
			_ng_info->inc_empty_node(-1);
	}
	void inc_empty_node(void)
	{
		if (empty_limit) {
			_ng_info->inc_empty_node(1);
			if (_ng_info->empty_count() > empty_limit) {
				purge_single_empty_node();
			}
		}
	}

	const unsigned int total_dirty_node() const
	{
		return _ng_info->total_dirty_node();
	}

	const uint64_t total_dirty_row() const
	{
		return _ng_info->total_dirty_row();
	}
	const uint64_t total_used_row() const
	{
		return _ng_info->total_used_row();
	}

	//schedule delay purge task periodically
	virtual void job_timer_procedure(void);

    public:
	BufferPond(PurgeNodeProcessor *o = NULL);
	~BufferPond();

	int check_expand_status();
	unsigned char shm_table_idx();
	bool col_expand(const char *table, int len);
	int try_col_expand(const char *table, int len);
	bool reload_table();

	int cache_open(BlockProperties *);
	void set_empty_node_limit(int v)
	{
		empty_limit = v < 0 ? 0 : v;
	}
	int init_empty_node_list(void);
	int upgrade_empty_node_list(void);
	int merge_empty_node_list(void);
	int prune_empty_node_list(void);
	int shrink_empty_node_list(void);
	int purge_single_empty_node(void);

	Node cache_find(const char *key, int new_hash);
	Node cache_find_auto_chose_hash(const char *key);
	int cache_purge(const char *key);
	int purge_node_and_data(Node purge_node);
	Node cache_allocation(const char *key);
	int try_purge_size(size_t size, Node purge_node, unsigned count = 2500);
	void disable_try_purge(void)
	{
		_disable_try_purge = 1;
	}
	void set_date_expire_alert_time(int time)
	{
		date_expire_alert_time = time < 0 ? 0 : time;
	};

	//eliminate a fixed number of nodes
	void delay_purge_notify(const unsigned count = 50);
	int pre_purge_nodes(int purge_cnt, Node reserve);
	int purge_by_time(unsigned int oldest_time);
	void start_delay_purge_task(TimerList *);

	int insert_time_marker(unsigned int);
	int remove_time_marker(Node node);
	int is_time_marker(Node node) const;
	Node first_time_marker() const;
	Node last_time_marker() const;
	unsigned int first_time_marker_time();
	unsigned int last_time_marker_time();

	Node dirty_lru_head() const;
	Node clean_lru_head() const;
	Node empty_lru_head() const;
	int dirty_lru_empty() const
	{
		return NODE_LIST_EMPTY(dirty_lru_head());
	}

	const BlockProperties *get_cache_info() const
	{
		return &_cache_info;
	}
	const char *error(void) const
	{
		return _err_msg;
	}

	FEATURE_INFO_T *query_feature_by_id(const uint32_t id)
	{
		return _feature ? _feature->get_feature_by_id(id) :
				  (FEATURE_INFO_T *)(0);
	}

	int add_feature(const uint32_t id, const MEM_HANDLE_T v)
	{
		if (_feature == NULL)
			return -1;
		return _feature->add_feature(id, v);
	}

	int clear_create();

	uint32_t max_node_id(void) const
	{
		return _ng_info->max_node_id();
	}

	NODE_ID_T get_min_valid_node_id(void) const
	{
		return _ng_info->get_min_valid_node_id();
	}

	const unsigned int get_total_used_node() const
	{
		return _ng_info->get_total_used_node();
	}
	void inc_total_row(int v)
	{
		_ng_info->inc_total_row(v);
	}

	static int32_t node_rows_count(Node node)
	{
		if (!node || node.vd_handle() == INVALID_HANDLE)
			return 0;

		DataChunk *chunk =
			((DataChunk *)(PtMalloc::instance()->handle_to_ptr(
				node.vd_handle())));
		if (!chunk)
			return 0;

		return chunk->total_rows();
	}

	friend class BufferProcessAskChain;
	friend class RawDataProcess;
	friend class TreeDataProcess;
};

DTC_END_NAMESPACE

#endif
