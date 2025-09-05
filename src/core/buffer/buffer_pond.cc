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

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>

#include "pt_malloc.h"
#include "namespace.h"
#include "buffer_pond.h"
#include "data_chunk.h"
#include "empty_filter.h"
#include "task/task_request.h"
#include "dtc_global.h"
#include "algorithm/relative_hour_calculator.h"
#include "table/table_def_manager.h"

extern DTCTableDefinition *g_table_def[];
extern int g_hash_changing;
extern int g_target_new_hash;

DTC_USING_NAMESPACE

BufferPond::BufferPond(PurgeNodeProcessor *pn) : _purge_processor(pn)
{
	memset(&_cache_info, 0x00, sizeof(BlockProperties));

	_hash = 0;
	_ng_info = 0;
	_feature = 0;
	_node_index = 0;
	_col_expand = 0;

	memset(_err_msg, 0, sizeof(_err_msg));
	_need_set_integrity = 0;
	_need_purge_node_count = 0;

	_delay_purge_timerlist = NULL;
	first_marker_time = last_marker_time = 0;
	empty_limit = 0;
	_disable_try_purge = 0;
	survival_hour = g_stat_mgr.get_sample(DATA_SURVIVAL_HOUR_STAT);
}

BufferPond::~BufferPond()
{
	_hash->destroy();
	_ng_info->destroy();
	_feature->destroy();
	_node_index->destroy();

	/* If we reach here, the program stopped normally, set shared memory integrity flag */
	if (_need_set_integrity) {
		log4cplus_info("Share Memory Integrity... ok");
		PtMalloc::instance()->set_share_memory_integrity(1);
	}
}

/* Check if LRU list is cross-linked, once this happens, there's no way to handle it :( */
static inline int check_cross_linked_lru(Node node)
{
	Node v = node.Prev();

	if (v == node) {
		log4cplus_error("BUG: cross-linked lru list");
		return -1;
	}
	return 0;
}

/* Verify cache info validity to avoid unexpected issues */
int BufferPond::verify_cache_info(BlockProperties *info)
{
	if (INVALID_HANDLE != 0UL) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "PANIC: invalid handle must be 0UL");
		return -1;
	}

	if (INVALID_NODE_ID != (NODE_ID_T)(-1)) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "PANIC: invalid node id must be %u, but it is %u now",
			 (NODE_ID_T)(-1), INVALID_NODE_ID);
		return -1;
	}

	if (info->version != 4) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "only support cache version >= 4");
		return -1;
	}

	/* Minimum memory required for system operation */
	/* 1. empty_filter = 0  Min=64M */
	/* 2. empty_filter = 1  Min=256M, initially calculated based on 1.5G users */

	if (info->empty_filter) {
		if (info->ipc_mem_size < (256UL << 20)) {
			snprintf(
				_err_msg, sizeof(_err_msg),
				"Empty-Node Filter function need min 256M mem");
			return -1;
		}
	}

	if (info->ipc_mem_size < (64UL << 20)) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "too small mem size, need min 64M mem");
		return -1;
	}

	/* size check on 32bits platform*/
	if (sizeof(long) == 4) {
		if (info->ipc_mem_size >= UINT_MAX) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "cache size " UINT64FMT
				 "exceed 4G, Please upgrade to 64 bit version",
				 info->ipc_mem_size);
			return -1;
		}
	}

	/* support max 64G memory size*/
	if (info->ipc_mem_size > (64ULL << 30)) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "cache size exceed 64G, unsupported");
		return -1;
	}

	return 0;
}

int BufferPond::check_expand_status()
{
	if (!_col_expand) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "column expand not support");
		return -2;
	}
	if (_col_expand->is_expanding()) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "column expanding, please later");
		return -1;
	}
	return 0;
}

unsigned char BufferPond::shm_table_idx()
{
	if (!_col_expand) {
		// column not supported, always return 0
		return 0;
	}

	return _col_expand->cur_table_idx();
}

bool BufferPond::col_expand(const char *table, int len)
{
	if (!_col_expand->expand(table, len)) {
		snprintf(_err_msg, sizeof(_err_msg), "column expand error");
		return false;
	}

	return true;
}

int BufferPond::try_col_expand(const char *table, int len)
{
	return _col_expand->try_expand(table, len);
}

bool BufferPond::reload_table()
{
	if (!_col_expand) {
		return true;
	}
	if (!_col_expand->reload_table()) {
		snprintf(_err_msg, sizeof(_err_msg), "reload table error");
		return false;
	}
	return true;
}

int BufferPond::cache_open(BlockProperties *info)
{
TRY_CACHE_INIT_AGAIN:
	if (info->read_only == 0) {
		if (verify_cache_info(info) != 0)
			return -1;
		memcpy((char *)&_cache_info, info, sizeof(BlockProperties));
	} else {
		memset((char *)&_cache_info, 0, sizeof(BlockProperties));
		_cache_info.read_only = 1;
		_cache_info.key_size = info->key_size;
		_cache_info.ipc_mem_key = info->ipc_mem_key;
	}

	//Initialize statistics objects
	stat_cache_size = g_stat_mgr.get_stat_int_counter(DTC_CACHE_SIZE);
	stat_cache_key = g_stat_mgr.get_stat_int_counter(DTC_CACHE_KEY);
	stat_cache_version = g_stat_mgr.get_stat_iterm(DTC_CACHE_VERSION);
	stat_update_mode = g_stat_mgr.get_stat_int_counter(DTC_UPDATE_MODE);
	stat_empty_filter = g_stat_mgr.get_stat_int_counter(DTC_EMPTY_FILTER);
	stat_hash_size = g_stat_mgr.get_stat_int_counter(DTC_BUCKET_TOTAL);
	stat_free_bucket = g_stat_mgr.get_stat_int_counter(DTC_FREE_BUCKET);
	stat_dirty_eldest = g_stat_mgr.get_stat_int_counter(DTC_DIRTY_ELDEST);
	stat_dirty_age = g_stat_mgr.get_stat_int_counter(DTC_DIRTY_AGE);
	stat_try_purge_count = g_stat_mgr.get_sample(TRY_PURGE_COUNT);
	stat_purge_for_create_update_count =
		g_stat_mgr.get_sample(PURGE_CREATE_UPDATE_STAT);
	stat_try_purge_nodes = g_stat_mgr.get_stat_int_counter(TRY_PURGE_NODES);
	stat_last_purge_node_mod_time =
		g_stat_mgr.get_stat_int_counter(LAST_PURGE_NODE_MOD_TIME);
	stat_data_exist_time = g_stat_mgr.get_stat_int_counter(DATA_EXIST_TIME);

	//Open shared memory
	if (_shm.mem_open(_cache_info.ipc_mem_key) > 0) {
		//Shared memory already exists

		if (_cache_info.create_only) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "m_shm already exist");
			return -1;
		}

		if (_cache_info.read_only == 0 && _shm.mem_lock() != 0) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "m_shm.Lock() failed");
			return -1;
		}

		if (_shm.mem_attach(_cache_info.read_only) == NULL) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "m_shm.do_attach() failed");
			return -1;
		}

		//Underlying allocator
		if (PtMalloc::instance()->do_attach(_shm.mem_ptr(),
						    _shm.mem_size()) != 0) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "binmalloc attach failed: %s", M_ERROR());
			return -1;
		}

		//Memory version detection, currently due to the underlying allocator, only supports version >= 4
		_cache_info.version = PtMalloc::instance()->detect_version();
		if (_cache_info.version != 4) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "unsupport version, %d", _cache_info.version);
			return -1;
		}

		/* Check shared memory integrity, passed */
		if (PtMalloc::instance()->share_memory_integrity()) {
			log4cplus_info("Share Memory Integrity Check.... ok");
			/* 
             * Set shared memory incomplete flag
             *
             * This allows DTC to detect corrupted memory when restarted after a program coredump.
             */
			if (_cache_info.read_only == 0) {
				_need_set_integrity = 1;
				PtMalloc::instance()->set_share_memory_integrity(
					0);
			}
		}
		/* Failed */
		else {
			log4cplus_warning(
				"Share Memory Integrity Check... failed");

			if (_cache_info.auto_delete_dirty_shm) {
				if (_cache_info.read_only == 1) {
					log4cplus_error(
						"ReadOnly Share Memory is Confuse");
					return -1;
				}

				/* Delete shared memory, restart cache initialization process */
				if (_shm.mem_delete() < 0) {
					log4cplus_error(
						"Auto Delete Share Memory failed: %m");
					return -1;
				}

				log4cplus_info(
					"Auto Delete Share Memory Success, Try Rebuild");

				_shm.mem_unlock();

				PtMalloc::destroy();

				/* Reinitialize */
				goto TRY_CACHE_INIT_AGAIN;
			}
		}
	}

	//Shared memory does not exist, need to create
	else {
		//Read-only, failed
		if (_cache_info.read_only) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "readonly m_shm non-exists");
			return -1;
		}

		//Create
		if (_shm.mem_create(_cache_info.ipc_mem_key,
				    _cache_info.ipc_mem_size) <= 0) {
			if (errno == EACCES || errno == EEXIST)
				snprintf(_err_msg, sizeof(_err_msg),
					 "m_shm exists but unwritable");
			else
				snprintf(_err_msg, sizeof(_err_msg),
					 "create m_shm failed: %m");
			return -1;
		}

		if (_shm.mem_lock() != 0) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "m_shm.Lock() failed");
			return -1;
		}

		if (_shm.mem_attach() == NULL) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "m_shm.do_attach() failed");
			return -1;
		}

		//Initialize underlying allocator
		if (PtMalloc::instance()->do_init(_shm.mem_ptr(),
						  _shm.mem_size()) != 0) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "binmalloc init failed: %s", M_ERROR());
			return -1;
		}

		/* 
         * Set shared memory incomplete flag
         */
		_need_set_integrity = 1;
		PtMalloc::instance()->set_share_memory_integrity(0);
	}

	/* statistic */
	stat_cache_size = _cache_info.ipc_mem_size;
	stat_cache_key = _cache_info.ipc_mem_key;
	stat_cache_version = _cache_info.version;
	stat_update_mode = _cache_info.sync_update;
	stat_empty_filter = _cache_info.empty_filter;
	/*set minchunksize*/
	PtMalloc::instance()->set_min_chunk_size(DTCGlobal::min_chunk_size_);

	//attention: invoke app_storage_open() must after PtMalloc init() or attach().
	return app_storage_open();
}

int BufferPond::app_storage_open()
{
	APP_STORAGE_T *storage = M_POINTER(
		APP_STORAGE_T, PtMalloc::instance()->get_reserve_zone());
	if (!storage) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "get reserve zone from binmalloc failed: %s",
			 M_ERROR());

		return -1;
	}

	return dtc_mem_open(storage);
}

int BufferPond::dtc_mem_open(APP_STORAGE_T *storage)
{
	if (storage->need_format()) {
		log4cplus_debug("starting init dtc mem");
		return dtc_mem_init(storage);
	}

	return dtc_mem_attach(storage);
}

/* hash size = 1% total memory size */
/* return hash bucket num*/

uint32_t BufferPond::hash_bucket_num(uint64_t size)
{
	int h = (uint32_t)(size / 100 - 16) / sizeof(NODE_ID_T);
	h = (h / 9) * 9;
	return h;
}

int BufferPond::dtc_mem_init(APP_STORAGE_T *storage)
{
	_feature = Feature::instance();
	if (!_feature || _feature->do_init(MIN_FEATURES)) {
		snprintf(_err_msg, sizeof(_err_msg), "init feature failed, %s",
			 _feature->error());
		return -1;
	}

	if (storage->do_format(_feature->get_handle())) {
		snprintf(_err_msg, sizeof(_err_msg), "format storage failed");
		return -1;
	}

	/* Node-get_index*/
	_node_index = NodeIndex::instance();
	if (!_node_index || _node_index->do_init(_cache_info.ipc_mem_size)) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "init node-index failed, %s", _node_index->error());
		return -1;
	}

	/* Hash-Bucket */
	_hash = DTCHash::instance();
	if (!_hash || _hash->do_init(hash_bucket_num(_cache_info.ipc_mem_size),
				     _cache_info.key_size)) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "init hash-bucket failed, %s", _hash->error());
		return -1;
	}
	stat_hash_size = _hash->hash_size();
	stat_free_bucket = _hash->free_bucket();

	/* NS-Info */
	_ng_info = NGInfo::instance();
	if (!_ng_info || _ng_info->do_init()) {
		snprintf(_err_msg, sizeof(_err_msg), "init ns-info failed, %s",
			 _ng_info->error());
		return -1;
	}

	/* insert features*/
	if (_feature->add_feature(NODE_INDEX, _node_index->get_handle())) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "add node-index feature failed, %s",
			 _feature->error());
		return -1;
	}

	if (_feature->add_feature(HASH_BUCKET, _hash->get_handle())) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "add hash-bucket feature failed, %s",
			 _feature->error());
		return -1;
	}

	if (_feature->add_feature(NODE_GROUP, _ng_info->get_handle())) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "add node-group feature failed, %s",
			 _feature->error());
		return -1;
	}

	/* Empty-Node Filter*/
	if (_cache_info.empty_filter) {
		EmptyNodeFilter *p = EmptyNodeFilter::instance();
		if (!p || p->do_init()) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "start Empty-Node Filter failed, %s",
				 p->error());
			return -1;
		}

		if (_feature->add_feature(EMPTY_FILTER, p->get_handle())) {
			snprintf(_err_msg, sizeof(_err_msg),
				 "add empty-filter feature failed, %s",
				 _feature->error());
			return -1;
		}
	}

	// column expand
	_col_expand = DTCColExpand::instance();
	if (!_col_expand || _col_expand->initialization()) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "init column expand failed, %s", _col_expand->error());
		return -1;
	}
	if (_feature->add_feature(COL_EXPAND, _col_expand->get_handle())) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "add column expand feature failed, %s",
			 _feature->error());
		return -1;
	}

	stat_dirty_eldest = 0;
	stat_dirty_age = 0;

	return 0;
}

int BufferPond::dtc_mem_attach(APP_STORAGE_T *storage)
{
	_feature = Feature::instance();
	if (!_feature || _feature->do_attach(storage->as_extend_info)) {
		snprintf(_err_msg, sizeof(_err_msg), "%s", _feature->error());
		return -1;
	}

	/*hash-bucket*/
	FEATURE_INFO_T *p = _feature->get_feature_by_id(HASH_BUCKET);
	if (!p) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "not found hash-bucket feature");
		return -1;
	}
	_hash = DTCHash::instance();
	if (!_hash || _hash->do_attach(p->fi_handle)) {
		snprintf(_err_msg, sizeof(_err_msg), "%s", _hash->error());
		return -1;
	}
	stat_hash_size = _hash->hash_size();
	stat_free_bucket = _hash->free_bucket();

	/*node-index*/
	p = _feature->get_feature_by_id(NODE_INDEX);
	if (!p) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "not found node-index feature");
		return -1;
	}
	_node_index = NodeIndex::instance();
	if (!_node_index || _node_index->do_attach(p->fi_handle)) {
		snprintf(_err_msg, sizeof(_err_msg), "%s",
			 _node_index->error());
		return -1;
	}

	/*ns-info*/
	p = _feature->get_feature_by_id(NODE_GROUP);
	if (!p) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "not found ns-info feature");
		return -1;
	}
	_ng_info = NGInfo::instance();
	if (!_ng_info || _ng_info->do_attach(p->fi_handle)) {
		snprintf(_err_msg, sizeof(_err_msg), "%s", _ng_info->error());
		return -1;
	}

	Node stLastTime = last_time_marker();
	Node stFirstTime = first_time_marker();
	if (!(!stLastTime) && !(!stFirstTime)) {
		stat_dirty_eldest = stLastTime.Time();
		stat_dirty_age = stFirstTime.Time() - stLastTime.Time();
	}

	//TODO tableinfo
	// column expand
	p = _feature->get_feature_by_id(COL_EXPAND);
	if (p) {
		_col_expand = DTCColExpand::instance();
		if (!_col_expand ||
		    _col_expand->attach(p->fi_handle,
					_cache_info.force_update_table_conf)) {
			// if _col_expand if null
			snprintf(_err_msg, sizeof(_err_msg), "%s",
				 _col_expand->error());
			return -1;
		}
	} else {
		log4cplus_error(
			"column expand feature not enable, do not support column expand");
		_col_expand = NULL;
	}
	return 0;
}

// Sync the empty node statstics
int BufferPond::init_empty_node_list(void)
{
	if (_ng_info->get_empty_startup_mode() == NGInfo::ATTACHED) {
		// iterate through empty lru list
		// re-counting the total empty lru statstics

		// empty lru header
		int count = 0;
		Node header = _ng_info->empty_node_head();
		Node pos;

		for (pos = header.Prev(); pos != header; pos = pos.Prev()) {
			/* check whether cross-linked */
			if (check_cross_linked_lru(pos) < 0)
				break;
			count++;
		}
		_ng_info->inc_empty_node(count);
		log4cplus_info("found %u empty nodes inside empty lru list",
			       count);
	}
	return 0;
}

// migrate empty node from clean list to empty list
int BufferPond::upgrade_empty_node_list(void)
{
	if (_ng_info->get_empty_startup_mode() != NGInfo::CREATED) {
		int count = 0;
		Node header = _ng_info->clean_node_head();
		Node next;

		for (Node pos = header.Prev(); pos != header; pos = next) {
			/* check whether cross-linked */
			if (check_cross_linked_lru(pos) < 0)
				break;
			next = pos.Prev();

			if (node_rows_count(pos) == 0) {
				_ng_info->remove_from_lru(pos);
				_ng_info->insert_to_empty_lru(pos);
				count++;
			}
		}
		_ng_info->inc_empty_node(count);
		log4cplus_info(
			"found %u empty nodes inside clean lru list, move to empty lru",
			count);
	}

	return 0;
}

// migrate empty node from empty list to clean list
int BufferPond::merge_empty_node_list(void)
{
	if (_ng_info->get_empty_startup_mode() != NGInfo::CREATED) {
		int count = 0;
		Node header = _ng_info->empty_node_head();
		Node next;

		for (Node pos = header.Prev(); pos != header; pos = next) {
			/* check whether cross-linked */
			if (check_cross_linked_lru(pos) < 0)
				break;
			next = pos.Prev();

			_ng_info->remove_from_lru(pos);
			_ng_info->insert_to_clean_lru(pos);
			count++;
		}
		log4cplus_info("found %u empty nodes, move to clean lru",
			       count);
	}

	return 0;
}

// prune all empty nodes
int BufferPond::prune_empty_node_list(void)
{
	if (_ng_info->get_empty_startup_mode() == NGInfo::ATTACHED) {
		int count = 0;
		Node header = _ng_info->empty_node_head();
		Node next;

		for (Node pos = header.Prev(); pos != header; pos = next) {
			/* check whether cross-linked */
			if (check_cross_linked_lru(pos) < 0)
				break;
			next = pos.Prev();

			count++;
			purge_node_and_data(pos);
		}

		log4cplus_info("fullmode: total %u empty nodes purged", count);
	}

	return 0;
}

int BufferPond::shrink_empty_node_list(void)
{
	if (empty_limit && _ng_info->empty_count() > empty_limit) {
		//bug fix recalc empty
		int togo = _ng_info->empty_count() - empty_limit;
		int count = 0;
		Node header = _ng_info->empty_node_head();
		Node next;

		for (Node pos = header.Prev(); count < togo && pos != header;
		     pos = next) {
			/* check whether cross-linked */
			if (check_cross_linked_lru(pos) < 0)
				break;

			next = pos.Prev();

			purge_node_and_data(pos);
			_ng_info->inc_empty_node(-1);
			count++;
		}
		log4cplus_info("shrink empty lru, %u empty nodes purged",
			       count);
	}

	return 0;
}

int BufferPond::purge_single_empty_node(void)
{
	Node header = _ng_info->empty_node_head();
	Node pos = header.Prev();

	if (pos != header) {
		/* check whether cross-linked */
		if (check_cross_linked_lru(pos) < 0)
			return -1;

		log4cplus_debug("empty node execeed limit, purge node %u",
				pos.node_id());
		purge_node_and_data(pos);
		_ng_info->inc_empty_node(-1);
	}

	return 0;
}

/* insert node to hash bucket*/
int BufferPond::insert_to_hash(const char *key, Node node)
{
	HASH_ID_T hashslot;

	if (g_target_new_hash) {
		hashslot = _hash->new_hash_slot(key);
	} else {
		hashslot = _hash->hash_slot(key);
	}

	if (_hash->hash_to_node(hashslot) == INVALID_NODE_ID) {
		_hash->inc_free_bucket(-1);
		--stat_free_bucket;
	}

	_hash->inc_node_cnt(1);

	node.next_node_id() = _hash->hash_to_node(hashslot);
	_hash->hash_to_node(hashslot) = node.node_id();

	return 0;
}

int BufferPond::remove_from_hash_base(const char *key, Node remove_node,
				      int newhash)
{
	HASH_ID_T hash_slot;

	if (newhash) {
		hash_slot = _hash->new_hash_slot(key);
	} else {
		hash_slot = _hash->hash_slot(key);
	}

	NODE_ID_T node_id = _hash->hash_to_node(hash_slot);

	/* hash miss */
	if (node_id == INVALID_NODE_ID)
		return 0;

	/* found in hash head */
	if (node_id == remove_node.node_id()) {
		_hash->hash_to_node(hash_slot) = remove_node.next_node_id();

		// stat
		if (_hash->hash_to_node(hash_slot) == INVALID_NODE_ID) {
			_hash->inc_free_bucket(1);
			++stat_free_bucket;
		}

		_hash->inc_node_cnt(-1);
		return 0;
	}

	Node prev = I_SEARCH(node_id);
	Node next = I_SEARCH(prev.next_node_id());

	while (!(!next) && next.node_id() != remove_node.node_id()) {
		prev = next;
		next = I_SEARCH(next.next_node_id());
	}

	/* found */
	if (!(!next)) {
		prev.next_node_id() = next.next_node_id();
		_hash->inc_node_cnt(-1);
	} else {
		log4cplus_error(
			"remove_from_hash failed, node-id [%d] not found in slot %u ",
			remove_node.node_id(), hash_slot);
		return -1;
	}

	return 0;
}

int BufferPond::remove_from_hash(const char *key, Node remove_node)
{
	if (g_hash_changing) {
		remove_from_hash_base(key, remove_node, 1);
		remove_from_hash_base(key, remove_node, 0);
	} else {
		if (g_target_new_hash)
			remove_from_hash_base(key, remove_node, 1);
		else
			remove_from_hash_base(key, remove_node, 0);
	}

	return 0;
}

int BufferPond::move_to_new_hash(const char *key, Node node)
{
	remove_from_hash(key, node);
	insert_to_hash(key, node);
	return 0;
}

inline int BufferPond::key_cmp(const char *key, const char *other)
{
	int len = _cache_info.key_size == 0 ? (*(unsigned char *)key + 1) :
					      _cache_info.key_size;

	return memcmp(key, other, len);
}

Node BufferPond::cache_find_auto_chose_hash(const char *key)
{
	int oldhash = 0;
	int newhash = 1;
	Node stNode;

	if (g_hash_changing) {
		if (g_target_new_hash) {
			oldhash = 0;
			newhash = 1;
		} else {
			oldhash = 1;
			newhash = 0;
		}

		stNode = cache_find(key, oldhash);
		if (!stNode) {
			stNode = cache_find(key, newhash);
		} else {
			move_to_new_hash(key, stNode);
		}
	} else {
		if (g_target_new_hash) {
			stNode = cache_find(key, 1);
		} else {
			stNode = cache_find(key, 0);
		}
	}
	return stNode;
}

Node BufferPond::cache_find(const char *key, int newhash)
{
	HASH_ID_T hash_slot;

	if (newhash) {
		hash_slot = _hash->new_hash_slot(key);
	} else {
		hash_slot = _hash->hash_slot(key);
	}

	NODE_ID_T node_id = _hash->hash_to_node(hash_slot);

	/* not found */
	if (node_id == INVALID_NODE_ID)
		return Node();

	Node iter = I_SEARCH(node_id);
	while (!(!iter)) {
		if (iter.vd_handle() == INVALID_HANDLE) {
			log4cplus_warning("node[%u]'s handle is invalid",
					  iter.node_id());
			Node node = iter;
			iter = I_SEARCH(iter.next_node_id());
			purge_node(key, node);
			continue;
		}

		DataChunk *data_chunk = M_POINTER(DataChunk, iter.vd_handle());
		if (NULL == data_chunk) {
			log4cplus_warning("node[%u]'s handle is invalid",
					  iter.node_id());
			Node node = iter;
			iter = I_SEARCH(iter.next_node_id());
			purge_node(key, node);
			continue;
		}

		if (NULL == data_chunk->key()) {
			log4cplus_warning(
				"node[%u]'s handle is invalid, decode key failed",
				iter.node_id());
			Node node = iter;
			iter = I_SEARCH(iter.next_node_id());
			purge_node(key, node);
			continue;
		}

		/* EQ */
		if (key_cmp(key, data_chunk->key()) == 0) {
			log4cplus_debug("found node[%u]", iter.node_id());
			return iter;
		}

		iter = I_SEARCH(iter.next_node_id());
	}

	/* not found*/
	return Node();
}

unsigned int BufferPond::first_time_marker_time(void)
{
	if (first_marker_time == 0) {
		Node marker = first_time_marker();
		first_marker_time = !marker ? 0 : marker.Time();
	}
	return first_marker_time;
}

unsigned int BufferPond::last_time_marker_time(void)
{
	if (last_marker_time == 0) {
		Node marker = last_time_marker();
		last_marker_time = !marker ? 0 : marker.Time();
	}
	return last_marker_time;
}

/* insert a time-marker to dirty lru list*/
int BufferPond::insert_time_marker(unsigned int t)
{
	Node tm_node = _ng_info->allocate_node();
	if (!tm_node) {
		log4cplus_debug(
			"no mem allocate timemarker, purge 10 clean node");
		/* prepurge clean node for cache is full */
		pre_purge_nodes(10, Node());
		tm_node = _ng_info->allocate_node();
		if (!tm_node) {
			log4cplus_error(
				"can not allocate time marker for dirty lru");
			return -1;
		}
	}

	log4cplus_debug("insert time marker in dirty lru, time %u", t);
	tm_node.next_node_id() = TIME_MARKER_NEXT_NODE_ID;
	tm_node.vd_handle() = t;

	_ng_info->insert_to_dirty_lru(tm_node);

	//stat
	first_marker_time = t;

	/*in case last_marker_time not set*/
	if (last_marker_time == 0)
		last_time_marker_time();

	stat_dirty_age = first_marker_time - last_marker_time;
	stat_dirty_eldest = last_marker_time;

	return 0;
}

/* -1: not a time marker
 * -2: this the only time marker

 */
int BufferPond::remove_time_marker(Node node)
{
	/* is not timermarker node */
	if (!is_time_marker(node))
		return -1;

	_ng_info->remove_from_lru(node);
	_ng_info->release_node(node);

	//stat
	Node stLastTime = last_time_marker();
	if (!stLastTime) {
		last_marker_time = first_marker_time;
	} else {
		last_marker_time = stLastTime.Time();
	}

	stat_dirty_age = first_marker_time - last_marker_time;
	stat_dirty_eldest = last_marker_time;
	return 0;
}

/* prev <- dirtyhead */
Node BufferPond::last_time_marker() const
{
	Node pos, dirtyHeader = _ng_info->dirty_node_head();
	NODE_LIST_FOR_EACH_RVS(pos, dirtyHeader)
	{
		if (pos.next_node_id() == TIME_MARKER_NEXT_NODE_ID)
			return pos;
	}

	return Node();
}

/* dirtyhead -> next */
Node BufferPond::first_time_marker() const
{
	Node pos, dirtyHeader = _ng_info->dirty_node_head();

	NODE_LIST_FOR_EACH(pos, dirtyHeader)
	{
		if (pos.next_node_id() == TIME_MARKER_NEXT_NODE_ID)
			return pos;
	}

	return Node();
}

/* dirty lru list head */
Node BufferPond::dirty_lru_head() const
{
	return _ng_info->dirty_node_head();
}

/* clean lru list head */
Node BufferPond::clean_lru_head() const
{
	return _ng_info->clean_node_head();
}

/* empty lru list head */
Node BufferPond::empty_lru_head() const
{
	return _ng_info->empty_node_head();
}

int BufferPond::is_time_marker(Node node) const
{
	return node.next_node_id() == TIME_MARKER_NEXT_NODE_ID;
}

int BufferPond::try_purge_size(size_t size, Node reserve,
			       unsigned max_purge_count)
{
	log4cplus_debug("start try_purge_size");

	if (_disable_try_purge) {
		static int alert_count = 0;
		if (!alert_count++) {
			log4cplus_fatal("memory overflow, auto purge disabled");
		}
		return -1;
	}
	/*if have pre purge, purge node and continue*/
	/* prepurge should not purge reserved node in try_purge_size */
	pre_purge_nodes(DTCGlobal::pre_purge_nodes_, reserve);

	unsigned real_try_purge_count = 0;

	/* clean lru header */
	Node clean_header = clean_lru_head();

	Node pos = clean_header.Prev();

	for (unsigned iter = 0;
	     iter < max_purge_count && !(!pos) && pos != clean_header; ++iter) {
		Node purge_node = pos;

		if (get_total_used_node() < 10)
			break;

		/* check whether cross-linked */
		if (check_cross_linked_lru(pos) < 0)
			break;

		pos = pos.Prev();

		if (purge_node == reserve) {
			continue;
		}

		if (purge_node.vd_handle() == INVALID_HANDLE) {
			log4cplus_warning("node[%u]'s handle is invalid",
					  purge_node.node_id());
			continue;
		}

		/* ask for data-chunk's size */
		DataChunk *data_chunk =
			M_POINTER(DataChunk, purge_node.vd_handle());
		if (NULL == data_chunk) {
			log4cplus_warning(
				"node[%u] handle is invalid, attach DataChunk failed",
				purge_node.node_id());
			continue;
		}

		unsigned combine_size =
			data_chunk->ask_for_destroy_size(PtMalloc::instance());
		log4cplus_debug("need_size=%u, combine-size=%u, node-size=%u",
				(unsigned)size, combine_size,
				data_chunk->node_size());

		if (combine_size >= size) {
			/* stat total rows */
			inc_total_row(0LL - node_rows_count(purge_node));
			purge_node_with_alert(purge_node);
			_need_purge_node_count = iter;
			log4cplus_debug(
				"try purge size for create or update: %d",
				iter + 1);
			stat_purge_for_create_update_count.push(iter + 1);
			++stat_try_purge_nodes;
			return 0;
		}

		++real_try_purge_count;
	}

	_need_purge_node_count = real_try_purge_count;
	return -1;
}

int BufferPond::purge_node(const char *key, Node purge_node)
{
	/* HB */
	if (_purge_processor)
		_purge_processor->purge_node_processor(key, purge_node);

	/*1. Remove from hash */
	remove_from_hash(key, purge_node);

	/*2. Remove from LRU */
	_ng_info->remove_from_lru(purge_node);

	/*3. Release node, it can auto remove from nodeIndex */
	_ng_info->release_node(purge_node);

	return 0;
}

int BufferPond::purge_node_and_data(Node node)
{
	/* invalid node attribute */
	if (!(!node) && node.vd_handle() != INVALID_HANDLE) {
		DataChunk *data_chunk = M_POINTER(DataChunk, node.vd_handle());
		if (NULL == data_chunk || NULL == data_chunk->key()) {
			log4cplus_error(
				"node[%u]'s handle is invalid, can't attach and decode key",
				node.node_id());
			//TODO
			return -1;
		}
		uint32_t dwCreatetime = data_chunk->create_time();
		uint32_t dwPurgeHour =
			RELATIVE_HOUR_CALCULATOR->get_relative_hour();
		log4cplus_debug(
			"lru purge node,node[%u]'s createhour is %u, purgeHour is %u",
			node.node_id(), dwCreatetime, dwPurgeHour);
		survival_hour.push((dwPurgeHour - dwCreatetime));

		char key[256] = { 0 };
		/* decode key */
		memcpy(key, data_chunk->key(),
		       _cache_info.key_size > 0 ?
			       _cache_info.key_size :
			       *(unsigned char *)(data_chunk->key()) + 1);

		/* destroy data-chunk */
		data_chunk->destory(PtMalloc::instance());

		return purge_node(key, node);
	}

	return 0;
}

uint32_t BufferPond::get_cmodtime(Node *node)
{
	// how init
	RawData *_raw_data = new RawData(PtMalloc::instance(), 1);
	uint32_t lastcmod = 0;
	uint32_t lastcmod_thisrow = 0;
	int iRet = _raw_data->do_attach(node->vd_handle());
	if (iRet != 0) {
		log4cplus_error("raw-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				node->vd_handle(), iRet,
				_raw_data->get_err_msg());
		return (0);
	}

	unsigned int uiTotalRows = _raw_data->total_rows();
	for (unsigned int i = 0; i < uiTotalRows; i++) //Search
	{
		if ((iRet = _raw_data->get_lastcmod(lastcmod_thisrow)) != 0) {
			log4cplus_error("raw-data decode row error: %d,%s",
					iRet, _raw_data->get_err_msg());
			return (0);
		}
		if (lastcmod_thisrow > lastcmod)
			lastcmod = lastcmod_thisrow;
	}
	return lastcmod;
}

//check if node's timestamp max than setting
//and purge_node_and_data
int BufferPond::purge_node_with_alert(Node node)
{
	int dataExistTime = stat_data_exist_time;
	if (date_expire_alert_time) {
		struct timeval tm;
		gettimeofday(&tm, NULL);
		unsigned int lastnodecmodtime = get_cmodtime(&node);
		if (lastnodecmodtime > stat_last_purge_node_mod_time) {
			stat_last_purge_node_mod_time = lastnodecmodtime;
			dataExistTime = (unsigned int)tm.tv_sec -
					stat_last_purge_node_mod_time;
			stat_data_exist_time = dataExistTime;
		}
		if (stat_data_exist_time < date_expire_alert_time) {
			static int alert_count = 0;
			if (!alert_count++) {
				log4cplus_fatal(
					"DataExistTime:%u is little than setting:%u",
					dataExistTime, date_expire_alert_time);
			}
		}
		log4cplus_debug(
			"dateExpireAlertTime:%d ,lastnodecmodtime:%d,timenow:%u",
			date_expire_alert_time, lastnodecmodtime,
			(uint32_t)tm.tv_sec);
	}

	return purge_node_and_data(node);
}
int BufferPond::purge_node_and_data(const char *key, Node node)
{
	DataChunk *data_chunk = NULL;
	if (!(!node) && node.vd_handle() != INVALID_HANDLE) {
		data_chunk = M_POINTER(DataChunk, node.vd_handle());
		if (NULL == data_chunk) {
			log4cplus_error(
				"node[%u]'s handle is invalid, can't attach data-chunk",
				node.node_id());
			return -1;
		}
		uint32_t dwCreatetime = data_chunk->create_time();
		uint32_t dwPurgeHour =
			RELATIVE_HOUR_CALCULATOR->get_relative_hour();
		log4cplus_debug(
			" purge node, node[%u]'s createhour is %u, purgeHour is %u",
			node.node_id(), dwCreatetime, dwPurgeHour);
		survival_hour.push((dwPurgeHour - dwCreatetime));
		/* destroy data-chunk */
		data_chunk->destory(PtMalloc::instance());
	}

	if (!(!node))
		return purge_node(key, node);
	return 0;
}

/* allocate a new node by key */
Node BufferPond::cache_allocation(const char *key)
{
	Node allocate_node = _ng_info->allocate_node();

	/* allocate failed */
	if (!allocate_node)
		return allocate_node;

	/*1. Insert to hash bucket */
	insert_to_hash(key, allocate_node);

	/*2. Insert to clean Lru list*/
	_ng_info->insert_to_clean_lru(allocate_node);

	return allocate_node;
}

extern int useNewHash;

/* purge key{data-chunk, hash, lru, node...} */
int BufferPond::cache_purge(const char *key)
{
	Node purge_node;

	if (g_hash_changing) {
		purge_node = cache_find(key, 0);
		if (!purge_node) {
			purge_node = cache_find(key, 1);
			if (!purge_node) {
				return 0;
			} else {
				if (purge_node_and_data(key, purge_node) < 0)
					return -1;
			}
		} else {
			if (purge_node_and_data(key, purge_node) < 0)
				return -1;
		}
	} else {
		if (g_target_new_hash) {
			purge_node = cache_find(key, 1);
			if (!purge_node)
				return 0;
			else {
				if (purge_node_and_data(key, purge_node) < 0)
					return -1;
			}
		} else {
			purge_node = cache_find(key, 0);
			if (!purge_node)
				return 0;
			else {
				if (purge_node_and_data(key, purge_node) < 0)
					return -1;
			}
		}
	}

	return 0;
}

void BufferPond::delay_purge_notify(const unsigned count)
{
	if (_need_purge_node_count == 0)
		return;
	else
		stat_try_purge_count.push(_need_purge_node_count);

	unsigned purge_count =
		count < _need_purge_node_count ? count : _need_purge_node_count;
	unsigned real_purge_count = 0;

	log4cplus_debug("delay_purge_notify: total=%u, now=%u",
			_need_purge_node_count, purge_count);

	/* clean lru header */
	Node clean_header = clean_lru_head();
	Node pos = clean_header.Prev();

	while (purge_count-- > 0 && !(!pos) && pos != clean_header) {
		Node purge_node = pos;
		check_cross_linked_lru(pos);
		pos = pos.Prev();

		/* stat total rows */
		inc_total_row(0LL - node_rows_count(purge_node));

		purge_node_with_alert(purge_node);

		++stat_try_purge_nodes;
		++real_purge_count;
	}

	_need_purge_node_count -= real_purge_count;

	/* If there are no requests, reschedule delay purge task */
	if (_need_purge_node_count > 0)
		attach_timer(_delay_purge_timerlist);

	return;
}

int BufferPond::pre_purge_nodes(int purge_count, Node reserve)
{
	int realpurged = 0;

	if (purge_count <= 0)
		return 0;
	else
		stat_try_purge_count.push(purge_count);

	/* clean lru header */
	Node clean_header = clean_lru_head();
	Node pos = clean_header.Prev();

	while (purge_count-- > 0 && !(!pos) && pos != clean_header) {
		Node purge_node = pos;
		check_cross_linked_lru(pos);
		pos = pos.Prev();

		if (reserve == purge_node)
			continue;

		/* stat total rows */
		inc_total_row(0LL - node_rows_count(purge_node));
		purge_node_with_alert(purge_node);
		++stat_try_purge_nodes;
		realpurged++;
	}
	return realpurged;
	;
}

int BufferPond::purge_by_time(unsigned int oldest_time)
{
	return 0;
}

int BufferPond::clear_create()
{
	if (_cache_info.read_only == 1) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "cache readonly, can not clear cache");
		return -2;
	}
	_hash->destroy();
	_ng_info->destroy();
	_feature->destroy();
	_node_index->destroy();
	PtMalloc::instance()->destroy();
	if (_shm.mem_delete() < 0) {
		snprintf(_err_msg, sizeof(_err_msg), "delete shm memory error");
		return -1;
	}
	log4cplus_info("delete shm memory ok when clear cache");

	if (_shm.mem_create(_cache_info.ipc_mem_key,
			    _cache_info.ipc_mem_size) <= 0) {
		snprintf(_err_msg, sizeof(_err_msg), "create shm memory error");
		return -1;
	}
	if (_shm.mem_attach() == NULL) {
		snprintf(_err_msg, sizeof(_err_msg), "attach shm memory error");
		return -1;
	}

	if (PtMalloc::instance()->do_init(_shm.mem_ptr(), _shm.mem_size()) !=
	    0) {
		snprintf(_err_msg, sizeof(_err_msg),
			 "binmalloc init failed: %s", M_ERROR());
		return -1;
	}
	PtMalloc::instance()->set_min_chunk_size(DTCGlobal::min_chunk_size_);
	return app_storage_open();
}

void BufferPond::start_delay_purge_task(TimerList *timer)
{
	log4cplus_info("start delay-purge job");
	_delay_purge_timerlist = timer;
	attach_timer(_delay_purge_timerlist);

	return;
}
void BufferPond::job_timer_procedure(void)
{
	log4cplus_debug("enter timer procedure");

	log4cplus_debug("sched delay-purge job");
	delay_purge_notify();

	log4cplus_debug("leave timer procedure");
}
