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

#include <stdlib.h>
#include <stdio.h>
#include <endian.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "packet/packet.h"
#include "log/log.h"
#include "buffer_process_ask_chain.h"
#include "buffer_flush.h"
#include "mysql_error.h"
#include "sys_malloc.h"
#include "data_chunk.h"
#include "raw_data_process.h"
#include "key/key_route_ask_chain.h"
#include "buffer_remoteLog.h"
#include "hotback_task.h"
#include "tree_data_process.h"
DTC_USING_NAMESPACE;

extern DTCTableDefinition *g_table_def[];
extern KeyRouteAskChain *g_key_route_ask_instance;
extern int g_hash_changing;
extern int g_target_new_hash;
extern DTCConfig *g_dtc_config;
extern int collect_load_config(DbConfig *dbconfig);
extern DbConfig *dbConfig;

#if __WORDSIZE == 64
#define UINT64FMT_T "%lu"
#else
#define UINT64FMT_T "%llu"
#endif

inline int BufferProcessAskChain::transaction_find_node(DTCJobOperation &job)
{
	// alreay cleared/zero-ed
	key = job.packed_key();
	if (empty_node_filter_ != NULL &&
	    empty_node_filter_->ISSET(job.int_key())) {
		//Cache.cache_purge(key);
		cache_transaction_node = Node();
		return node_status = DTC_CODE_NODE_EMPTY;
	}

	int newhash, oldhash;
	if (g_hash_changing) {
		if (g_target_new_hash) {
			oldhash = 0;
			newhash = 1;
		} else {
			oldhash = 1;
			newhash = 0;
		}

		cache_transaction_node = cache_.cache_find(key, oldhash);
		if (!cache_transaction_node) {
			cache_transaction_node =
				cache_.cache_find(key, newhash);
			if (!cache_transaction_node)
				return node_status = DTC_CODE_NODE_NOTFOUND;
		} else {
			cache_.move_to_new_hash(key, cache_transaction_node);
		}
	} else {
		if (g_target_new_hash) {
			cache_transaction_node = cache_.cache_find(key, 1);
			if (!cache_transaction_node)
				return node_status = DTC_CODE_NODE_NOTFOUND;
		} else {
			cache_transaction_node = cache_.cache_find(key, 0);
			if (!cache_transaction_node)
				return node_status = DTC_CODE_NODE_NOTFOUND;
		}
	}

	key_dirty = cache_transaction_node.is_dirty();
	old_rows = cache_.node_rows_count(cache_transaction_node);
	// prepare to decrease empty node count
	node_empty = key_dirty == 0 && old_rows == 0;
	return node_status = DTC_CODE_NODE_HIT;
}

inline void BufferProcessAskChain::transaction_update_lru(bool async, int level)
{
	if (!key_dirty) {
		// clear node empty here, because the lru is adjusted
		// it's not a fresh node in EmptyButInCleanList state
		if (async == true) {
			cache_transaction_node.set_dirty();
			cache_.inc_dirty_node(1);
			cache_.remove_from_lru(cache_transaction_node);
			cache_.insert_to_dirty_lru(cache_transaction_node);
			if (node_empty != 0) {
				// empty to non-empty
				cache_.dec_empty_node();
				node_empty = 0;
			}
			lru_update = LRU_NONE;
		} else {
			lru_update = level;
		}
	}
}

void BufferProcessAskChain::transaction_end(void)
{
	int newRows = 0;
	if (!!cache_transaction_node && !key_dirty &&
	    !cache_transaction_node.is_dirty()) {
		newRows = cache_.node_rows_count(cache_transaction_node);
		int nodeEmpty1 = newRows == 0;

		if (lru_update > lru_update_level_ ||
		    nodeEmpty1 != node_empty) {
			if (newRows == 0) {
				cache_.remove_from_lru(cache_transaction_node);
				cache_.insert_to_empty_lru(
					cache_transaction_node);
				if (node_empty == 0) {
					// non-empty to empty
					cache_.inc_empty_node();
					node_empty = 1;
				}
				// Cache.DumpEmptyNodeList();
			} else {
				cache_.remove_from_lru(cache_transaction_node);
				cache_.insert_to_clean_lru(
					cache_transaction_node);
				if (node_empty != 0) {
					// empty to non-empty
					cache_.dec_empty_node();
					node_empty = 0;
				}
			}
		}
	}

	CacheTransaction::Free();
}

int BufferProcessAskChain::write_lru_hotbackup_log(const char *key)
{
	log4cplus_debug("write_lru_hotbackup_log begin");
	if (!log_hotbackup_key_switch_) {
		return 0;
	}
	log4cplus_debug("write_lru_hotbackup_log new job");
	DTCJobOperation *pJob = new DTCJobOperation;
	if (pJob == NULL) {
		log4cplus_error(
			"cannot write_hotbackup_log row, new job error, possible memory exhausted\n");
		return -1;
	}

	pJob->set_request_type(TaskTypeWriteLruHbLog);
	HotBackTask &hotbacktask = pJob->get_hot_back_task();
	hotbacktask.set_type(DTCHotBackup::SYNC_LRU);
	hotbacktask.set_flag(DTCHotBackup::NON_VALUE);
	hotbacktask.set_value(NULL, 0);
	DTCValue packeKey = table_define_infomation_->packed_key(key);
	hotbacktask.set_packed_key(packeKey.bin.ptr, packeKey.bin.len);
	log4cplus_debug(" packed key len:%d, key len:%d,  key :%s",
			packeKey.bin.len, *(unsigned char *)packeKey.bin.ptr,
			packeKey.bin.ptr + 1);
	dispatch_hot_back_task(pJob);
	return 0;
}

int BufferProcessAskChain::write_hotbackup_log(const char *key, char *pstChunk,
					       unsigned int uiNodeSize,
					       int iType)
{
	if (!log_hotbackup_key_switch_) {
		return 0;
	}
	DTCJobOperation *pJob = new DTCJobOperation;
	if (pJob == NULL) {
		log4cplus_error(
			"cannot write_hotbackup_log row, new job error, possible memory exhausted\n");
		return -1;
	}

	pJob->set_request_type(TaskTypeWriteHbLog);

	HotBackTask &hotbacktask = pJob->get_hot_back_task();
	hotbacktask.set_type(iType);
	DTCValue packeKey;
	if (iType == DTCHotBackup::SYNC_COLEXPAND_CMD)
		packeKey.Set(key);
	else
		packeKey = table_define_infomation_->packed_key(key);
	hotbacktask.set_packed_key(packeKey.bin.ptr, packeKey.bin.len);
	log4cplus_debug(" packed key len:%d, key len:%d,  key :%s",
			packeKey.bin.len, *(unsigned char *)packeKey.bin.ptr,
			packeKey.bin.ptr + 1);
	if (uiNodeSize > 0 &&
	    (iType == DTCHotBackup::SYNC_COLEXPAND_CMD || uiNodeSize <= 100)) {
		hotbacktask.set_flag(DTCHotBackup::HAS_VALUE);
		hotbacktask.set_value(pstChunk, uiNodeSize);
		dispatch_hot_back_task(pJob);
	} else {
		hotbacktask.set_flag(DTCHotBackup::NON_VALUE);
		hotbacktask.set_value(NULL, 0);
		dispatch_hot_back_task(pJob);
	}

	return 0;
}

int BufferProcessAskChain::write_hotbackup_log(const char *key, Node &node,
					       int iType)
{
	if (!log_hotbackup_key_switch_) {
		return 0;
	}

	unsigned int uiNodeSize = 0;
	DataChunk *pstChunk = NULL;

	if (!(!node) && node.vd_handle() != INVALID_HANDLE) {
		pstChunk = (DataChunk *)PtMalloc::instance()->handle_to_ptr(
			node.vd_handle());
		uiNodeSize = pstChunk->node_size();
	}
	return write_hotbackup_log(key, (char *)pstChunk, uiNodeSize, iType);
}

inline int BufferProcessAskChain::write_hotbackup_log(DTCJobOperation &job,
						      Node &node, int iType)
{
	return write_hotbackup_log(job.packed_key(), node, iType);
}

void BufferProcessAskChain::purge_node_processor(const char *key, Node node)
{
	if (!node)
		return;

	if (node == cache_transaction_node) {
		if (node_empty) {
			// purge an empty node! decrease empty counter
			cache_.dec_empty_node();
			node_empty = 0;
		}
		cache_transaction_node = Node::Empty();
	}

	if (write_hotbackup_log(key, node, DTCHotBackup::SYNC_PURGE)) {
		log4cplus_error("hb: log purge key failed");
	}
}

BufferProcessAskChain::BufferProcessAskChain(PollerBase *p,
					     DTCTableDefinition *tdef,
					     EUpdateMode um)
	: JobAskInterface<DTCJobOperation>(p), main_chain(p), remote_chain(p),
	  hotbackup_chain(p), cache_reply_(this),
	  table_define_infomation_(tdef), cache_(this),
	  dtc_mode_(DTC_MODE_DATABASE_ADDITION), full_mode_(false),
	  m_bReplaceEmpty(false), lru_update_level_(0), async_server_(um),
	  update_mode_(MODE_SYNC), insert_mode_(MODE_SYNC),
	  memory_dirty_(false), insert_order_(INSERT_ORDER_LAST),
	  node_size_limit_(0), node_rows_limit_(0), node_empty_limit_(0),

	  flush_reply_(this), flush_timer_(NULL),
	  current_pend_flush_request_(0), pend_flush_request_(0),
	  max_flush_request_(1), marker_interval_(300), min_dirty_time_(3600),
	  max_dirty_time_(43200),

	  empty_node_filter_(NULL),
	  // Hot Backup
	  log_hotbackup_key_switch_(false), hotbackup_lru_feature_(NULL),
	  // Hot Backup
	  // BlackList
	  black_list_(0), blacklist_timer_(0)

// BlackList
{
	memset((char *)&cache_info_, 0, sizeof(cache_info_));

	stat_get_count_ = g_stat_mgr.get_stat_int_counter(DTC_GET_COUNT);
	stat_get_hits_ = g_stat_mgr.get_stat_int_counter(DTC_GET_HITS);
	stat_insert_count_ = g_stat_mgr.get_stat_int_counter(DTC_INSERT_COUNT);
	stat_insert_hits_ = g_stat_mgr.get_stat_int_counter(DTC_INSERT_HITS);
	stat_update_count_ = g_stat_mgr.get_stat_int_counter(DTC_UPDATE_COUNT);
	stat_update_hits_ = g_stat_mgr.get_stat_int_counter(DTC_UPDATE_HITS);
	stat_delete_count_ = g_stat_mgr.get_stat_int_counter(DTC_DELETE_COUNT);
	stat_delete_hits_ = g_stat_mgr.get_stat_int_counter(DTC_DELETE_HITS);
	stat_purge_count_ = g_stat_mgr.get_stat_int_counter(DTC_PURGE_COUNT);

	stat_drop_count_ = g_stat_mgr.get_stat_int_counter(DTC_DROP_COUNT);
	stat_drop_rows_ = g_stat_mgr.get_stat_int_counter(DTC_DROP_ROWS);
	stat_flush_count_ = g_stat_mgr.get_stat_int_counter(DTC_FLUSH_COUNT);
	stat_flush_rows_ = g_stat_mgr.get_stat_int_counter(DTC_FLUSH_ROWS);
	// statIncSyncStep = g_stat_mgr.get_sample(HBP_INC_SYNC_STEP);

	stat_maxflush_request_ =
		g_stat_mgr.get_stat_int_counter(DTC_MAX_FLUSH_REQ);
	stat_currentFlush_request_ =
		g_stat_mgr.get_stat_int_counter(DTC_CURR_FLUSH_REQ);

	stat_oldestdirty_time_ =
		g_stat_mgr.get_stat_int_counter(DTC_OLDEST_DIRTY_TIME);
	stat_asyncflush_count_ =
		g_stat_mgr.get_stat_int_counter(DTC_ASYNC_FLUSH_COUNT);

	stat_expire_count_ =
		g_stat_mgr.get_stat_int_counter(DTC_KEY_EXPIRE_USER_COUNT);
	stat_buffer_process_expire_count_ =
		g_stat_mgr.get_stat_int_counter(CACHE_EXPIRE_REQ);

	max_expire_count_ =
		g_dtc_config->get_int_val("cache", "max_expire_count_", 100);
	max_expire_time_ = g_dtc_config->get_int_val(
		"cache", "max_expire_time_", 3600 * 24 * 30);
}

BufferProcessAskChain::~BufferProcessAskChain()
{
	if (empty_node_filter_ != NULL)
		delete empty_node_filter_;
}

int BufferProcessAskChain::set_insert_order(int o)
{
	if (dtc_mode_ == DTC_MODE_CACHE_ONLY && o == INSERT_ORDER_PURGE) {
		log4cplus_error(
			"NoDB server don't support TABLE_CONF.ServerOrderInsert = purge");
		return -1;
	}

	if (cache_info_.sync_update == 0 && o == INSERT_ORDER_PURGE) {
		log4cplus_error(
			"AsyncUpdate server don't support TABLE_CONF.ServerOrderInsert = purge");
		return -1;
	}
	insert_order_ = o;
	if (data_process_)
		data_process_->set_insert_order(o);
	return 0;
}

int BufferProcessAskChain::enable_no_db_mode(void)
{
	if (insert_order_ == INSERT_ORDER_PURGE) {
		log4cplus_error(
			"NoDB server don't support TABLE_CONF.ServerOrderInsert = purge");
		return DTC_CODE_FAILED;
	}
	if (table_define_infomation_->has_auto_increment()) {
		log4cplus_error(
			"NoDB server don't support auto_increment field");
		return DTC_CODE_FAILED;
	}
	dtc_mode_ = DTC_MODE_CACHE_ONLY;
	full_mode_ = true;
	return DTC_CODE_SUCCESS;
}

int BufferProcessAskChain::disable_lru_update(int level)
{
	if (level > LRU_WRITE)
		level = LRU_WRITE;
	if (level < 0)
		level = 0;
	lru_update_level_ = level;
	return 0;
}

int BufferProcessAskChain::disable_async_log(int disable)
{
	async_log_ = !!disable;
	return 0;
}

int BufferProcessAskChain::set_buffer_size_and_version(
	unsigned long cache_size, unsigned int cache_version)
{
	cache_info_.init(table_define_infomation_->key_format(), cache_size,
			 cache_version);
	return DTC_CODE_SUCCESS;
}

/*
 * Function		: cache_open
 * Description	: 打开cache
 * Input			: key_name		共享内存ipc key
 *				  ulNodeTotal_	数据节点总数
 * ulBucketTotal	hash桶总数
 * ulChunkTotal	chunk节点总数
 * ulChunkSize	chunk节点大小(单位:byte)
 * Output		: 
 * Return		: 成功返回0,失败返回-1
 */
int BufferProcessAskChain::open_init_buffer(int key_name,
					    int enable_empty_filter,
					    int enable_auto_clean_dirty_buffer)
{
	cache_info_.key_size = table_define_infomation_->key_format();
	cache_info_.ipc_mem_key = key_name;
	cache_info_.sync_update = !async_server_;
	cache_info_.empty_filter = enable_empty_filter ? 1 : 0;
	cache_info_.auto_delete_dirty_shm =
		enable_auto_clean_dirty_buffer ? 1 : 0;
	cache_info_.force_update_table_conf =
		g_dtc_config->get_int_val("cache", "ForceUpdateTableConf", 0);

	log4cplus_debug(
		"cache_info: \n\tshmkey[%d] \n\tshmsize[" UINT64FMT
		"] \n\tkeysize[%u]"
		"\n\tversion[%u] \n\tsyncUpdate[%u] \n\treadonly[%u]"
		"\n\tcreateonly[%u] \n\tempytfilter[%u] \n\tautodeletedirtysharememory[%u]",
		cache_info_.ipc_mem_key, cache_info_.ipc_mem_size,
		cache_info_.key_size, cache_info_.version,
		cache_info_.sync_update, cache_info_.read_only,
		cache_info_.create_only, cache_info_.empty_filter,
		cache_info_.auto_delete_dirty_shm);

	if (cache_.cache_open(&cache_info_)) {
		log4cplus_error("%s", cache_.error());
		return -1;
	}

	log4cplus_info("Current cache_ memory format is V%d\n",
		       cache_info_.version);

	int iMemSyncUpdate = cache_.dirty_lru_empty() ? 1 : 0;
	/*
	 * 1. sync dtc + dirty mem, SYNC + memory_dirty_
	 * 2. sync dtc + clean mem, SYNC + !memory_dirty_
	 * 3. async dtc + dirty mem/clean mem: ASYNC
	 *    disable ASYNC <--> FLUSH switch, so FLUSH never happen forever
	 *    update_mode_ == async_server_
	 */
	switch (async_server_ * 0x10000 + iMemSyncUpdate) {
	// sync dtcd + async mem
	case 0x00000:
		memory_dirty_ = true;
		update_mode_ = MODE_SYNC;
		break;
	// sync dtcd + sync mem
	case 0x00001:
		update_mode_ = MODE_SYNC;
		break;
	// async dtcd + async mem
	case 0x10000:
		update_mode_ = MODE_ASYNC;
		break;
	// async dtcd + sync mem
	case 0x10001:
		update_mode_ = MODE_ASYNC;
		break;
	default:
		update_mode_ = cache_info_.sync_update ? MODE_SYNC : MODE_ASYNC;
	}
	if (table_define_infomation_->has_auto_increment() == 0 &&
	    update_mode_ == MODE_ASYNC)
		insert_mode_ = MODE_ASYNC;
	log4cplus_info("Cache Update Mode: %s",
		       update_mode_ == MODE_SYNC ?
			       "SYNC" :
			       update_mode_ == MODE_ASYNC ?
			       "ASYNC" :
			       update_mode_ == MODE_FLUSH ? "FLUSH" : "<BAD>");
	// 空结点过滤
	const FEATURE_INFO_T *pstFeature;
	pstFeature = cache_.query_feature_by_id(EMPTY_FILTER);
	if (pstFeature != NULL) {
		NEW(EmptyNodeFilter, empty_node_filter_);
		if (empty_node_filter_ == NULL) {
			log4cplus_error("new %s error: %m", "EmptyNodeFilter");
			return -1;
		}
		if (empty_node_filter_->do_attach(pstFeature->fi_handle) != 0) {
			log4cplus_error("EmptyNodeFilter attach error: %s",
					empty_node_filter_->error());
			return -1;
		}
	}
	MallocBase *pstMalloc = PtMalloc::instance();
	UpdateMode stUpdateMod = { async_server_, update_mode_, insert_mode_,
				   insert_order_ };
	if (table_define_infomation_->index_fields() > 0) {
		log4cplus_debug("tree index enable, index field num[%d]",
				table_define_infomation_->index_fields());
		data_process_ =
			new TreeDataProcess(pstMalloc, table_define_infomation_,
					    &cache_, &stUpdateMod);
		if (data_process_ == NULL) {
			log4cplus_error("create TreeDataProcess error: %m");
			return -1;
		}
	} else {
		log4cplus_debug("%s", "use raw-data mode");
		data_process_ =
			new RawDataProcess(pstMalloc, table_define_infomation_,
					   &cache_, &stUpdateMod);
		if (data_process_ == NULL) {
			log4cplus_error("create RawDataProcess error: %m");
			return -1;
		}
		((RawDataProcess *)data_process_)
			->set_limit_node_size(node_size_limit_);
	}
	if (update_mode_ == MODE_SYNC) {
		async_log_ = 1;
	}
	// 热备特性
	pstFeature = cache_.query_feature_by_id(HOT_BACKUP);
	if (pstFeature != NULL) {
		NEW(HBFeature, hotbackup_lru_feature_);
		if (hotbackup_lru_feature_ == NULL) {
			log4cplus_error("new hot-backup feature error: %m");
			return -1;
		}
		if (hotbackup_lru_feature_->attach(pstFeature->fi_handle) !=
		    0) {
			log4cplus_error("hot-backup feature attach error: %s",
					hotbackup_lru_feature_->error());
			return -1;
		}

		if (hotbackup_lru_feature_->master_uptime() != 0) {
			// 开启变更key日志
			log_hotbackup_key_switch_ = true;
		}
	}
	// Hot Backup
	// DelayPurge
	cache_.start_delay_purge_task(
		owner->get_timer_list_by_m_seconds(10 /*10 ms*/));

	// Blacklist
	// 10 min sched
	blacklist_timer_ = owner->get_timer_list(10 * 60);
	NEW(BlackListUnit(blacklist_timer_), black_list_);
	if (NULL == black_list_ ||
	    black_list_->init_blacklist(
		    100000, table_define_infomation_->key_format())) {
		log4cplus_error("init black_list failed");
		return -1;
	}
	black_list_->start_blacklist_expired_task();
	// Blacklist
	if (table_define_infomation_->expire_time_field_id() != -1) {
		if (dtc_mode_ == DTC_MODE_CACHE_ONLY) {
			key_expire_timer_ = owner->get_timer_list_by_m_seconds(
				1000 /* 1s */);
			NEW(ExpireTime(key_expire_timer_, &cache_,
				       data_process_, table_define_infomation_,
				       max_expire_count_),
			    key_expire);
			if (key_expire == NULL) {
				log4cplus_error("init key expire time failed");
				return -1;
			}
			key_expire->start_key_expired_task();
		} else {
			log4cplus_error("db mode do not support expire time");
			return -1;
		}
	}
	// Empty Node list
	if (full_mode_ == true) {
		// nodb Mode has not empty nodes,
		node_empty_limit_ = 0;
		// prune all present empty nodes
		cache_.prune_empty_node_list();
	} else if (node_empty_limit_) {
		// Enable Empty Node Limitation
		cache_.set_empty_node_limit(node_empty_limit_);
		// re-counting empty node count
		cache_.init_empty_node_list();
		// upgrade from old memory
		cache_.upgrade_empty_node_list();
		// shrinking empty list
		cache_.shrink_empty_node_list();
	} else {
		// move all empty node to clean list
		cache_.merge_empty_node_list();
	}

	// Empty Node list
	return 0;
}

bool BufferProcessAskChain::insert_empty_node(void)
{
	for (int i = 0; i < 2; i++) {
		cache_transaction_node = cache_.cache_allocation(key);
		if (!(!cache_transaction_node))
			break;
		if (cache_.try_purge_size(1, cache_transaction_node) != 0)
			break;
	}
	if (!cache_transaction_node) {
		log4cplus_debug("alloc cache node error");
		return false;
	}
	cache_transaction_node.vd_handle() = INVALID_HANDLE;
	// new node created, it's EmptyButInCleanList
	// means it's not in empty before transaction
	node_empty = 0;
	return true;
}

BufferResult BufferProcessAskChain::insert_default_row(DTCJobOperation &job)
{
	int iRet;
	log4cplus_debug("%s", "insert default start!");
	if (!cache_transaction_node) {
		// 发现空节点
		if (insert_empty_node() == false) {
			log4cplus_warning("alloc cache node error");
			job.set_error(-EIO, CACHE_SVC,
				      "alloc cache node error");
			return DTC_CODE_BUFFER_ERROR;
		}
		if (empty_node_filter_)
			empty_node_filter_->CLR(job.int_key());
	} else {
		uint32_t uiTotalRows =
			((DataChunk *)(PtMalloc::instance()->handle_to_ptr(
				 cache_transaction_node.vd_handle())))
				->total_rows();
		if (uiTotalRows != 0)
			return DTC_CODE_BUFFER_SUCCESS;
	}
	RowValue stRowValue(job.table_definition());
	stRowValue.default_value();
	RawData stDataRows(&g_stSysMalloc, 1);
	iRet = stDataRows.do_init(key);
	if (iRet != 0) {
		log4cplus_warning("raw data init error: %d, %s", iRet,
				  stDataRows.get_err_msg());
		job.set_error(-ENOMEM, CACHE_SVC, "new raw-data error");
		cache_.purge_node_and_data(key, cache_transaction_node);
		return DTC_CODE_BUFFER_ERROR;
	}
	stDataRows.insert_row(stRowValue, false, false);
	iRet = data_process_->do_replace_all(&cache_transaction_node,
					     &stDataRows);
	if (iRet != 0) {
		log4cplus_debug("replace data error: %d, %s", iRet,
				stDataRows.get_err_msg());
		job.set_error(-ENOMEM, CACHE_SVC, "replace data error");
		// 标记加入黑名单
		job.push_black_list_size(stDataRows.data_size());
		cache_.purge_node_and_data(key, cache_transaction_node);
		return DTC_CODE_BUFFER_ERROR;
	}

	if (cache_transaction_node.vd_handle() == INVALID_HANDLE) {
		log4cplus_error("BUG: node[%u] vdhandle=0",
				cache_transaction_node.node_id());
		cache_.purge_node(job.packed_key(), cache_transaction_node);
	}

	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * Function		: buffer_get_data
 * Description	: 处理get请求
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 */
BufferResult BufferProcessAskChain::buffer_get_data(DTCJobOperation &job)
{
	int iRet;

	log4cplus_debug("buffer_get_data start ");
	transaction_find_node(job);
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		if (full_mode_ == false) {
			if (job.flag_no_cache() != 0)
				job.mark_as_pass_thru();
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		}
		--stat_get_hits_; // FullCache Missing treat as miss
			// FullCache Mode: treat as empty & fallthrough
	case DTC_CODE_NODE_EMPTY:
		++stat_get_hits_;
		//发现空节点，直接构建result
		log4cplus_debug("found Empty-Node[%u], response directed",
				job.int_key());
		job.prepare_result();
		job.set_total_rows(0);
		job.set_result_hit_flag(HIT_SUCCESS);
		return DTC_CODE_BUFFER_SUCCESS;
	}

	if (dtc_mode_ == DTC_MODE_CACHE_ONLY) {
		BufferResult cacheRet = check_and_expire(job);
		if (cacheRet != DTC_CODE_BUFFER_GOTO_NEXT_CHAIN)
			return cacheRet;
	}
	++stat_get_hits_;
	log4cplus_debug("[%s:%d]cache hit ", __FILE__, __LINE__);

	transaction_update_lru(false, LRU_READ);
	iRet = data_process_->do_get(job, &cache_transaction_node);
	if (iRet != 0) {
		log4cplus_error("do_get() failed");
		job.set_error_dup(-EIO, CACHE_SVC,
				  data_process_->get_err_msg());
		return DTC_CODE_BUFFER_ERROR;
	}
	log4cplus_debug(" lru_update_level_:%d,LRU_READ:%d", lru_update_level_,
			LRU_READ);
	// Hot Backup
	if (lru_update_level_ < LRU_READ &&
	    write_lru_hotbackup_log(job.packed_key())) {
		// 为避免错误扩大， 给客户端成功响应
		log4cplus_error("hb: log lru key failed");
	}
	// Hot Bakcup
	job.set_result_hit_flag(HIT_SUCCESS);
	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * Function		: buffer_batch_get_data
 * Description	: 处理get请求
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 */
BufferResult BufferProcessAskChain::buffer_batch_get_data(DTCJobOperation &job)
{
	int index;
	int iRet;
	log4cplus_debug("buffer_batch_get_data start ");
	job.prepare_result_no_limit();
	for (index = 0; job.set_batch_cursor(index) >= 0; index++) {
		++stat_get_count_;
		job.set_result_hit_flag(HIT_INIT);
		transaction_find_node(job);
		switch (node_status) {
		case DTC_CODE_NODE_EMPTY:
			++stat_get_hits_;
			job.done_batch_cursor(index);
			log4cplus_debug("[%s:%d]cache empty ", __FILE__,
					__LINE__);
			break;
		case DTC_CODE_NODE_NOTFOUND:
			if (full_mode_)
				job.done_batch_cursor(index);
			log4cplus_debug("[%s:%d]cache miss ", __FILE__,
					__LINE__);
			break;
		case DTC_CODE_NODE_HIT:
			++stat_get_hits_;
			log4cplus_debug("[%s:%d]cache hit ", __FILE__,
					__LINE__);

			transaction_update_lru(false, LRU_BATCH);
			iRet = data_process_->do_get(job,
						     &cache_transaction_node);
			if (iRet != 0) {
				log4cplus_error("do_get() failed");
				job.set_error_dup(-EIO, CACHE_SVC,
						  data_process_->get_err_msg());
				return DTC_CODE_BUFFER_ERROR;
			}
			job.done_batch_cursor(index);
			// Hot Backup
			if (lru_update_level_ < LRU_BATCH &&
			    write_lru_hotbackup_log(job.packed_key())) {
				//为避免错误扩大， 给客户端成功响应
				log4cplus_error("hb: log lru key failed");
			}
			break;
		}
		transaction_end();
	}
	// Hot Bakcup
	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * Function		: buffer_get_rb
 * Description	: 处理Helper的get回读task
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 */
BufferResult BufferProcessAskChain::buffer_get_rb(DTCJobOperation &job)
{
	log4cplus_debug("buffer_get_rb start ");

	job.prepare_result();
	int iRet = job.append_result(job.result);
	if (iRet < 0) {
		log4cplus_info("job append_result error: %d", iRet);
		job.set_error(iRet, CACHE_SVC, "append_result() error");
		return DTC_CODE_BUFFER_ERROR;
	}
	log4cplus_debug("buffer_get_rb success");
	return DTC_CODE_BUFFER_SUCCESS;
}

// helper执行GET回来后，更新内存数据
BufferResult BufferProcessAskChain::buffer_replace_result(DTCJobOperation &job)
{
	int iRet;
	int oldRows = 0;
	log4cplus_debug("cache replace all start!");
	transaction_find_node(job);

	// 数据库回来的记录如果是0行则
	// 1. 设置bits
	// 2. 直接构造0行的result响应包
	if (empty_node_filter_ != NULL) {
		if ((job.result == NULL || job.result->total_rows() == 0)) {
			log4cplus_debug("SET Empty-Node[%u]", job.int_key());
			empty_node_filter_->SET(job.int_key());
			cache_.cache_purge(key);
			return DTC_CODE_BUFFER_SUCCESS;
		} else {
			empty_node_filter_->CLR(job.int_key());
		}
	}
	if (!cache_transaction_node) {
		if (insert_empty_node() == false)
			return DTC_CODE_BUFFER_SUCCESS;
	} else {
		oldRows = cache_.node_rows_count(cache_transaction_node);
	}
	unsigned int uiNodeID = cache_transaction_node.node_id();
	iRet = data_process_->do_replace_all(job, &cache_transaction_node);
	if (iRet != 0 || cache_transaction_node.vd_handle() == INVALID_HANDLE) {
		if (dtc_mode_ == DTC_MODE_CACHE_ONLY) {
			// UNREACHABLE
			log4cplus_info("cache replace data error: %d. node: %u",
				       iRet, uiNodeID);
			job.set_error_dup(-EIO, CACHE_SVC,
					  data_process_->get_err_msg());
			return DTC_CODE_BUFFER_ERROR;
		}
		log4cplus_debug("cache replace data error: %d. purge node: %u",
				iRet, uiNodeID);
		cache_.purge_node_and_data(key, cache_transaction_node);
		cache_.inc_dirty_row(0 - oldRows);
		return DTC_CODE_BUFFER_SUCCESS;
	}
	cache_.inc_total_row(data_process_->get_increase_row_count());

	transaction_update_lru(false, LRU_READ);
	if (oldRows != 0 ||
	    cache_.node_rows_count(cache_transaction_node) != 0) {
		// Hot Backup
		if (lru_update_level_ < LRU_READ &&
		    write_lru_hotbackup_log(job.packed_key())) {
			// 为避免错误扩大， 给客户端成功响应
			log4cplus_error("hb: log lru key failed");
		}
		// Hot Bakcup
	}

	log4cplus_debug("buffer_replace_result success! ");

	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * Function		: buffer_flush_data
 * Description	: 处理flush请求
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 */
BufferResult
BufferProcessAskChain::buffer_flush_data_before_delete(DTCJobOperation &job)
{
	log4cplus_debug("%s", "flush start!");
	transaction_find_node(job);
	if (!cache_transaction_node || !(cache_transaction_node.is_dirty())) {
		log4cplus_debug(
			"node is null or node is clean,return DTC_CODE_BUFFER_SUCCESS");
		return DTC_CODE_BUFFER_SUCCESS;
	}
	unsigned int affected_count;
	Node node = cache_transaction_node;
	int iRet = 0;
	// init
	key_dirty = cache_transaction_node.is_dirty();
	DTCFlushRequest *flushReq = new DTCFlushRequest(this, key);
	if (flushReq == NULL) {
		log4cplus_error("new DTCFlushRequest error: %m");
		return DTC_CODE_BUFFER_ERROR;
	}
	iRet = data_process_->do_flush(flushReq, &cache_transaction_node,
				       affected_count);
	if (iRet != 0) {
		log4cplus_error("do_flush error:%d", iRet);
		return DTC_CODE_BUFFER_ERROR;
	}
	if (affected_count == 0) {
		delete flushReq;
		if (key_dirty)
			cache_.inc_dirty_node(-1);
		cache_transaction_node.clr_dirty();
		cache_.remove_from_lru(cache_transaction_node);
		cache_.insert_to_clean_lru(cache_transaction_node);
		return DTC_CODE_BUFFER_SUCCESS;
	} else {
		commit_flush_request(flushReq, NULL);
		cache_.inc_dirty_row(
			data_process_->get_increase_dirty_row_count());
		if (key_dirty)
			cache_.inc_dirty_node(-1);
		cache_transaction_node.clr_dirty();
		cache_.remove_from_lru(cache_transaction_node);
		cache_.insert_to_clean_lru(cache_transaction_node);
		++stat_flush_count_;
		stat_flush_rows_ += affected_count;
		return DTC_CODE_BUFFER_SUCCESS;
	}
}

/*
 * Function		: buffer_flush_data
 * Description	: 处理flush请求
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 */
BufferResult BufferProcessAskChain::buffer_flush_data(DTCJobOperation &job)
{
	log4cplus_debug("%s", "flush start!");
	transaction_find_node(job);
	if (!cache_transaction_node || !(cache_transaction_node.is_dirty()))
		return DTC_CODE_BUFFER_SUCCESS;
	unsigned int affected_count;
	BufferResult iRet =
		buffer_flush_data(cache_transaction_node, &job, affected_count);
	if (iRet == DTC_CODE_BUFFER_SUCCESS) {
		++stat_flush_count_;
		stat_flush_rows_ += affected_count;
	}
	return (iRet);
}

// called by flush next node
int BufferProcessAskChain::buffer_flush_data_timer(Node &node,
						   unsigned int &affected_count)
{
	int iRet, err = 0;
	// init
	transaction_begin(NULL);
	key_dirty = node.is_dirty();
	key = ((DataChunk *)(PtMalloc::instance()->handle_to_ptr(
		       node.vd_handle())))
		      ->key();
	DTCFlushRequest *flushReq = new DTCFlushRequest(this, key);
	if (flushReq == NULL) {
		log4cplus_error("new DTCFlushRequest error: %m");
		err = -1;
		goto __out;
	}
	iRet = data_process_->do_flush(flushReq, &node, affected_count);
	if (affected_count == 0) {
		delete flushReq;
		if (iRet < 0) {
			err = -2;
			goto __out;
		} else {
			if (key_dirty)
				cache_.inc_dirty_node(-1);
			node.clr_dirty();
			cache_.remove_from_lru(node);
			cache_.insert_to_clean_lru(node);
			err = 1;
			goto __out;
		}
	} else {
		commit_flush_request(flushReq, NULL);
		cache_.inc_dirty_row(
			data_process_->get_increase_dirty_row_count());
		if (iRet == 0) {
			if (key_dirty)
				cache_.inc_dirty_node(-1);
			node.clr_dirty();
			cache_.remove_from_lru(node);
			cache_.insert_to_clean_lru(node);
			err = 2;
			goto __out;
		} else {
			err = -5;
			goto __out;
		}
	}

__out:
	// clear init
	CacheTransaction::Free();
	return err;
}
/*
 * Function		: buffer_flush_data
 * Description	: 处理flush请求
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 * 
 */
BufferResult
BufferProcessAskChain::buffer_flush_data(Node &node, DTCJobOperation *pstTask,
					 unsigned int &affected_count)
{
	int iRet;
	// could called by flush timer event, no transactionFindNode called there, can't trust KeyDirty, recal it
	key_dirty = node.is_dirty();
	log4cplus_debug("%s", "flush node start!");
	int flushCnt = 0;
	DTCFlushRequest *flushReq = NULL;
	if (dtc_mode_ == DTC_MODE_DATABASE_ADDITION) {
		flushReq = new DTCFlushRequest(this, key);
		if (flushReq == NULL) {
			log4cplus_error("new DTCFlushRequest error: %m");
			if (pstTask != NULL)
				pstTask->set_error(-ENOMEM, CACHE_SVC,
						   "new DTCFlushRequest error");
			return DTC_CODE_BUFFER_ERROR;
		}
	}
	iRet = data_process_->do_flush(flushReq, &node, affected_count);
	if (flushReq) {
		flushCnt = flushReq->numReq;
		commit_flush_request(flushReq, pstTask);
		if (iRet != 0) {
			log4cplus_error("do_flush() failed while flush data");
			if (pstTask != NULL)
				pstTask->set_error_dup(
					-EIO, CACHE_SVC,
					data_process_->get_err_msg());

			return DTC_CODE_BUFFER_ERROR;
		}
	}
	cache_.inc_dirty_row(data_process_->get_increase_dirty_row_count());
	if (key_dirty)
		cache_.inc_dirty_node(-1);
	node.clr_dirty();
	key_dirty = 0;
	transaction_update_lru(false, LRU_ALWAYS);
	log4cplus_debug("buffer_flush_data success");
	if (flushCnt == 0)
		return DTC_CODE_BUFFER_SUCCESS;
	else
		return DTC_CODE_BUFFER_UNFINISHED;
}

/*
 * Function		: buffer_purge_data
 * Description	: 处理purge请求
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 * 
 */
BufferResult BufferProcessAskChain::buffer_purge_data(DTCJobOperation &job)
{
	transaction_find_node(job);
	switch (node_status) {
	case DTC_CODE_NODE_EMPTY:
		empty_node_filter_->CLR(job.int_key());
		return DTC_CODE_BUFFER_SUCCESS;

	case DTC_CODE_NODE_NOTFOUND:
		return DTC_CODE_BUFFER_SUCCESS;

	case DTC_CODE_NODE_HIT:
		break;
	}
	BufferResult iRet = DTC_CODE_BUFFER_SUCCESS;
	if (update_mode_ && cache_transaction_node.is_dirty()) {
		unsigned int affected_count;
		iRet = buffer_flush_data(cache_transaction_node, &job,
					 affected_count);
		if (iRet != DTC_CODE_BUFFER_UNFINISHED)
			return iRet;
	}
	++stat_drop_count_;
	stat_drop_rows_ += ((DataChunk *)(PtMalloc::instance()->handle_to_ptr(
				    cache_transaction_node.vd_handle())))
				   ->total_rows();
	cache_.inc_total_row(0LL -
			     ((DataChunk *)(PtMalloc::instance()->handle_to_ptr(
				      cache_transaction_node.vd_handle())))
				     ->total_rows());
	unsigned int uiNodeID = cache_transaction_node.node_id();
	if (cache_.cache_purge(key) != 0) {
		log4cplus_error("PANIC: purge node[id=%u] fail", uiNodeID);
	}
	return iRet;
}

/*
 * Function		: buffer_update_rows
 * Description	: 处理Helper的update job
 * Input		: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 * 
 */
BufferResult BufferProcessAskChain::buffer_update_rows(DTCJobOperation &job,
						       bool async, bool setrows)
{
	int iRet;
	log4cplus_debug("cache update data start! ");
	if (m_bReplaceEmpty == true) {
		BufferResult ret = insert_default_row(job);
		if (ret != DTC_CODE_BUFFER_SUCCESS)
			return (ret);
	}
	int rows = cache_.node_rows_count(cache_transaction_node);
	iRet = data_process_->do_update(job, &cache_transaction_node, log_rows,
					async, setrows);
	if (iRet != 0) {
		if (async == false && !job.flag_black_hole()) {
			cache_.purge_node_and_data(key, cache_transaction_node);
			cache_.inc_total_row(0LL - rows);
			return DTC_CODE_BUFFER_SUCCESS;
		}
		log4cplus_warning("do_update() failed: %d,%s", iRet,
				  data_process_->get_err_msg());
		job.set_error_dup(-EIO, CACHE_SVC,
				  data_process_->get_err_msg());
		transaction_update_lru(async, LRU_ALWAYS);
		goto ERR_RETURN;
	}
	// if update volatile field,node won't be dirty
	transaction_update_lru(
		(job.resultInfo.affected_rows() > 0 &&
		 (job.request_operation() &&
		  job.request_operation()
			  ->has_type_commit()) //has core field modified
		 ) ?
			async :
			false,
		LRU_WRITE);
	cache_.inc_dirty_row(data_process_->get_increase_dirty_row_count());
	// Hot Backup
	if (node_status != DTC_CODE_NODE_HIT ||
	    (job.request_operation() &&
	     job.request_operation()->has_type_commit())) {
		// only write log if some non-volatile field got updated
		// or cache miss and m_bReplaceEmpty is set (equiv insert(default)+update)
		if (write_hotbackup_log(job, cache_transaction_node,
					DTCHotBackup::SYNC_UPDATE)) {
			// 为避免错误扩大， 给客户端成功响应
			log4cplus_error("hb: log update key failed");
		}
	}
	// Hot Bakcup
	return DTC_CODE_BUFFER_SUCCESS;
ERR_RETURN:
	return DTC_CODE_BUFFER_ERROR;
}

// buffer_replace_rows don't allow empty node
BufferResult BufferProcessAskChain::buffer_replace_rows(DTCJobOperation &job,
							bool async,
							bool setrows)
{
	int iRet;
	log4cplus_debug("cache replace rows start!");
	int rows = cache_.node_rows_count(cache_transaction_node);
	iRet = data_process_->do_replace(job, &cache_transaction_node, log_rows,
					 async, setrows);
	if (iRet != 0) {
		if (key_dirty == false && !job.flag_black_hole()) {
			cache_.purge_node_and_data(key, cache_transaction_node);
			cache_.inc_total_row(0LL - rows);
		}
		// 如果是同步replace命令，返回成功
		if (async == false && !job.flag_black_hole())
			return DTC_CODE_BUFFER_SUCCESS;
		log4cplus_error("cache replace rows error: %d,%s", iRet,
				data_process_->get_err_msg());
		job.set_error(-EIO, CACHE_SVC, "do_replace_all() error");
		return DTC_CODE_BUFFER_ERROR;
	}
	cache_.inc_total_row(data_process_->get_increase_row_count());
	cache_.inc_dirty_row(data_process_->get_increase_dirty_row_count());
	BufferResult ret = DTC_CODE_BUFFER_SUCCESS;
	transaction_update_lru(async, LRU_WRITE);
	// Hot Backup
	if (write_hotbackup_log(job, cache_transaction_node,
				DTCHotBackup::SYNC_UPDATE)) {
		// 为避免错误扩大， 给客户端成功响应
		log4cplus_error("hb: log update key failed");
	}
	// Hot Bakcup
	log4cplus_debug("buffer_replace_rows success! ");
	if (cache_transaction_node.vd_handle() == INVALID_HANDLE) {
		log4cplus_error("BUG: node[%u] vdhandle=0",
				cache_transaction_node.node_id());
		cache_.purge_node(job.packed_key(), cache_transaction_node);
		cache_.inc_total_row(0LL - rows);
	}

	return ret;
}

/*
 * Function	: buffer_insert_row
 * Description	: 处理Helper的insert job
 * Input		: job			请求信息
 * Output	: job			返回信息
 * Return	: 成功返回0,失败返回-1
 * 
 */
BufferResult BufferProcessAskChain::buffer_insert_row(DTCJobOperation &job,
						      bool async, bool setrows)
{
	int iRet;
	bool emptyFlag = false;
	if (!cache_transaction_node) {
		emptyFlag = true;
		if (insert_empty_node() == false) {
			if (async == true || job.flag_black_hole()) {
				job.set_error(
					-EIO, CACHE_SVC,
					"allocate_node Error while insert row");
				return DTC_CODE_BUFFER_ERROR;
			}
			return DTC_CODE_BUFFER_SUCCESS;
		}
		RawData stDataRows(&g_stSysMalloc, 1);
		// iRet = stDataRows.do_init(0, job.table_definition()->key_format(), key);
		iRet = stDataRows.do_init(key);
		if (iRet != 0) {
			log4cplus_warning("raw data init error: %d, %s", iRet,
					  stDataRows.get_err_msg());
			job.set_error(-ENOMEM, CACHE_SVC, "new raw-data error");
			cache_.purge_node_and_data(key, cache_transaction_node);
			return DTC_CODE_BUFFER_ERROR;
		}
		iRet = data_process_->do_replace_all(&cache_transaction_node,
						     &stDataRows);
		if (iRet != 0) {
			log4cplus_warning("raw data init error: %d, %s", iRet,
					  stDataRows.get_err_msg());
			job.set_error(-ENOMEM, CACHE_SVC, "new raw-data error");
			cache_.purge_node_and_data(key, cache_transaction_node);
			return DTC_CODE_BUFFER_ERROR;
		}
		if (empty_node_filter_)
			empty_node_filter_->CLR(job.int_key());
	}
	int oldRows = cache_.node_rows_count(cache_transaction_node);
	iRet = data_process_->do_append(job, &cache_transaction_node, log_rows,
					async, setrows);
	if (iRet == -1062) {
		job.set_error(-ER_DUP_ENTRY, CACHE_SVC,
			      "duplicate unique key detected");
		return DTC_CODE_BUFFER_ERROR;
	} else if (iRet != 0) {
		if ((async == false && !job.flag_black_hole()) || emptyFlag) {
			log4cplus_debug("do_append() failed, purge now [%d %s]",
					iRet, data_process_->get_err_msg());
			cache_.inc_total_row(0LL - oldRows);
			cache_.purge_node_and_data(key, cache_transaction_node);
			return DTC_CODE_BUFFER_SUCCESS;
		} else {
			log4cplus_error("do_append() failed while update data");
			job.set_error_dup(-EIO, CACHE_SVC,
					  data_process_->get_err_msg());
			return DTC_CODE_BUFFER_ERROR;
		}
	}
	transaction_update_lru(async, LRU_WRITE);
	cache_.inc_total_row(data_process_->get_increase_row_count());
	if (async == true)
		cache_.inc_dirty_row(
			data_process_->get_increase_dirty_row_count());
	// Hot Backup
	if (write_hotbackup_log(job, cache_transaction_node,
				DTCHotBackup::SYNC_INSERT)) {
		// 为避免错误扩大， 给客户端成功响应
		log4cplus_error("hb: log update key failed");
	}
	// Hot Bakcup
	log4cplus_debug("buffer_insert_row success");
	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * Function		: buffer_delete_rows
 * Description	: 处理del请求
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 成功返回0,失败返回-1
 * 
 */
BufferResult BufferProcessAskChain::buffer_delete_rows(DTCJobOperation &job)
{
	int iRet;
	log4cplus_debug("buffer_delete_rows start! ");
	uint32_t oldRows = cache_.node_rows_count(cache_transaction_node);
	int all_row_delete = job.all_rows();
	// 如果没有del条件则删除整个节点
	if (job.all_rows() != 0) {
	empty:
		if (lossy_mode_ || job.flag_black_hole()) {
			job.resultInfo.set_affected_rows(oldRows);
		}
		// row cnt statistic dec by 1
		cache_.inc_total_row(0LL - oldRows);
		// dirty node cnt staticstic dec by 1
		if (key_dirty) {
			cache_.inc_dirty_node(-1);
		}
		// dirty row cnt statistic dec, if count dirty row error, let statistic wrong with it
		if (all_row_delete) {
			int old_dirty_rows = data_process_->get_dirty_row_count(
				job, &cache_transaction_node);
			if (old_dirty_rows > 0)
				cache_.inc_dirty_row(old_dirty_rows);
		} else {
			cache_.inc_dirty_row(
				data_process_->get_increase_dirty_row_count());
		}
		cache_.purge_node_and_data(key, cache_transaction_node);
		if (empty_node_filter_)
			empty_node_filter_->SET(job.int_key());
		// Hot Backup
		Node stEmpytNode;
		if (write_hotbackup_log(job, stEmpytNode,
					DTCHotBackup::SYNC_PURGE))
		//		if(hbLog.write_update_key(job.packed_key(), DTCHotBackup::SYNC_UPDATE))
		{
			// 为避免错误扩大， 给客户端成功响应
			log4cplus_error("hb: log update key failed");
		}
		// Hot Bakcup

		return DTC_CODE_BUFFER_SUCCESS;
	}
	// delete error handle is too simple, statistic can not trust if error happen here
	iRet = data_process_->do_delete(job, &cache_transaction_node, log_rows);
	if (iRet != 0) {
		log4cplus_error("do_delete() failed: %d,%s", iRet,
				data_process_->get_err_msg());
		job.set_error_dup(-EIO, CACHE_SVC,
				  data_process_->get_err_msg());
		if (!key_dirty) {
			cache_.inc_total_row(0LL - oldRows);
			cache_.purge_node_and_data(key, cache_transaction_node);
		}
		return DTC_CODE_BUFFER_ERROR;
	}
	// Delete to empty
	uint32_t uiTotalRows =
		((DataChunk *)(PtMalloc::instance()->handle_to_ptr(
			 cache_transaction_node.vd_handle())))
			->total_rows();
	if (uiTotalRows == 0)
		goto empty;

	cache_.inc_dirty_row(data_process_->get_increase_dirty_row_count());
	cache_.inc_total_row(data_process_->get_increase_row_count());

	transaction_update_lru(false, LRU_WRITE);
	// Hot Backup
	if (write_hotbackup_log(job, cache_transaction_node,
				DTCHotBackup::SYNC_DELETE)) {
		// 为避免错误扩大， 给客户端成功响应
		log4cplus_error("hb: log update key failed");
	}
	// Hot Bakcup
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::check_allowed_insert(DTCJobOperation &job)
{
	int rows = cache_.node_rows_count(cache_transaction_node);
	// single rows checker
	if (table_define_infomation_->key_as_uniq_field() && rows != 0) {
		job.set_error(-ER_DUP_ENTRY, CACHE_SVC,
			      "duplicate unique key detected");
		return DTC_CODE_BUFFER_ERROR;
	}
	if (node_rows_limit_ > 0 && rows >= node_rows_limit_) {
		// check weather allowed do_execute insert operation
		job.set_error(
			-EC_NOT_ALLOWED_INSERT, __FUNCTION__,
			"rows exceed limit, not allowed insert any more data");
		return DTC_CODE_BUFFER_ERROR;
	}
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult
BufferProcessAskChain::buffer_sync_insert_precheck(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_sync_insert begin");
	// 这种模式下，不支持insert操作
	if (m_bReplaceEmpty == true) {
		job.set_error(
			-EC_BAD_COMMAND, CACHE_SVC,
			"insert cmd from client, not support under replace mode");
		log4cplus_info(
			"insert cmd from client, not support under replace mode");
		return DTC_CODE_BUFFER_ERROR;
	}
	if (table_define_infomation_->key_as_uniq_field() ||
	    node_rows_limit_ > 0) {
		transaction_find_node(job);

		// single rows checker
		if (node_status == DTC_CODE_NODE_HIT &&
		    check_allowed_insert(job) == DTC_CODE_BUFFER_ERROR)
			return DTC_CODE_BUFFER_ERROR;
	}
	return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
}

BufferResult BufferProcessAskChain::buffer_sync_insert(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_sync_insert begin");
	// 这种模式下，不支持insert操作
	if (m_bReplaceEmpty == true) {
		job.set_error(
			-EC_BAD_COMMAND, CACHE_SVC,
			"insert cmd from client, not support under replace mode");
		log4cplus_info(
			"insert cmd from client, not support under replace mode");
		return DTC_CODE_BUFFER_ERROR;
	}
	// 如果自增量字段是key，则会更新key
	if (job.resultInfo.insert_id() > 0)
		job.update_packed_key(job.resultInfo.insert_id());

	transaction_find_node(job);
	// Missing is NO-OP, otherwise insert it
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_EMPTY:
	case DTC_CODE_NODE_HIT:
		if (lossy_mode_) {
			job.set_error(0, NULL, NULL);
			job.resultInfo.set_affected_rows(0);
		}
		break;
	}
	return buffer_insert_row(job, false /* async */,
				 lossy_mode_ /* setrows */);
}

BufferResult BufferProcessAskChain::buffer_sync_update(DTCJobOperation &job)
{
	bool setrows = lossy_mode_;
	log4cplus_debug("%s", "buffer_sync_update begin");
	// NOOP sync update
	if (job.request_operation() == NULL) {
		// no field need to update
		// 如果helper更新的纪录数为0则直接返回
		return DTC_CODE_BUFFER_SUCCESS;
	} else if (setrows == false && job.resultInfo.affected_rows() == 0) {
		if (job.request_operation()->has_type_commit() == 0) {
			// pure volatile update, ignore upstream affected-rows
			setrows = true;
		} else if (job.request_condition() &&
			   job.request_condition()->has_type_timestamp()) {
			// update base timestamp fields, ignore upstream affected-rows
			setrows = true;
		} else {
			log4cplus_debug("%s", "helper's affected rows is zero");
			// 如果helper更新的纪录数为0则直接返回
			return DTC_CODE_BUFFER_SUCCESS;
		}
	}
	transaction_find_node(job);
	// Missing or Empty is NO-OP except EmptyAsDefault logical
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_EMPTY:
		if (m_bReplaceEmpty == true)
			break;
		if (lossy_mode_) {
			job.set_error(0, NULL, NULL);
			job.resultInfo.set_affected_rows(0);
		}
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_HIT:
		if (lossy_mode_) {
			job.set_error(0, NULL, NULL);
			job.resultInfo.set_affected_rows(0);
		}
		break;
	}
	return buffer_update_rows(job, false /*Async*/, setrows);
}

BufferResult BufferProcessAskChain::buffer_sync_replace(DTCJobOperation &job)
{
	const int setrows = lossy_mode_;
	log4cplus_debug("%s", "buffer_sync_replace begin");
	// NOOP sync update
	if (lossy_mode_ == false && job.resultInfo.affected_rows() == 0) {
		log4cplus_debug("%s", "helper's affected rows is zero");
		// 如果helper更新的纪录数为0则直接返回
		return DTC_CODE_BUFFER_SUCCESS;
	}
	transaction_find_node(job);
	// missing node is NO-OP, empty node insert it, otherwise replace it
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_EMPTY:
		if (lossy_mode_) {
			job.set_error(0, NULL, NULL);
			job.resultInfo.set_affected_rows(0);
		}
		return buffer_insert_row(job, false, setrows);
	case DTC_CODE_NODE_HIT:
		if (lossy_mode_) {
			job.set_error(0, NULL, NULL);
			job.resultInfo.set_affected_rows(0);
		}
		break;
	}
	return buffer_replace_rows(job, false, lossy_mode_);
}

BufferResult BufferProcessAskChain::buffer_sync_delete(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_sync_delete begin");
	// didn't check zero affected_rows
	transaction_find_node(job);
	// missing and empty is NO-OP, otherwise delete it
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_EMPTY:
		if (lossy_mode_) {
			job.set_error(0, NULL, NULL);
			job.resultInfo.set_affected_rows(0);
		}
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_HIT:
		break;
	}

	return buffer_delete_rows(job);
}

BufferResult BufferProcessAskChain::buffer_nodb_insert(DTCJobOperation &job)
{
	BufferResult iRet;
	log4cplus_debug("%s", "buffer_asyn_prepare_insert begin");
	// 这种模式下，不支持insert操作
	if (m_bReplaceEmpty == true) {
		job.set_error(
			-EC_BAD_COMMAND, CACHE_SVC,
			"insert cmd from client, not support under replace mode");
		log4cplus_info(
			"insert cmd from client, not support under replace mode");
		return DTC_CODE_BUFFER_ERROR;
	}
	transaction_find_node(job);
	if (node_status == DTC_CODE_NODE_HIT) {
		iRet = check_and_expire(job);
		if (iRet == DTC_CODE_BUFFER_ERROR) {
			return iRet;
		} else if (iRet == DTC_CODE_BUFFER_SUCCESS) {
			node_status = DTC_CODE_NODE_NOTFOUND;
			cache_transaction_node = Node();
		}
	}
	if (node_status == DTC_CODE_NODE_HIT &&
	    check_allowed_insert(job) == DTC_CODE_BUFFER_ERROR)
		return DTC_CODE_BUFFER_ERROR;

	// update key expire time
	if (job.request_operation() &&
	    job.update_key_expire_time(max_expire_time_) != 0) {
		job.set_error(-EC_BAD_INVALID_FIELD, CACHE_SVC,
			      "key expire time illegal");
		return DTC_CODE_BUFFER_ERROR;
	}
	return buffer_insert_row(job, false /* async */, true /* setrows */);
}

BufferResult BufferProcessAskChain::buffer_nodb_update(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_fullmode_prepare_update begin");
	transaction_find_node(job);
	// missing & empty is NO-OP,
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
	case DTC_CODE_NODE_EMPTY:
		if (m_bReplaceEmpty == true)
			break;
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_HIT:
		break;
	}
	BufferResult cacheRet = check_and_expire(job);
	if (cacheRet != DTC_CODE_BUFFER_GOTO_NEXT_CHAIN)
		return cacheRet;
	// update key expire time
	if (job.request_operation() &&
	    job.update_key_expire_time(max_expire_time_) != 0) {
		job.set_error(-EC_BAD_INVALID_FIELD, CACHE_SVC,
			      "key expire time illegal");
		return DTC_CODE_BUFFER_ERROR;
	}
	return buffer_update_rows(job, false /*Async*/, true /*setrows*/);
}

BufferResult BufferProcessAskChain::buffer_nodb_replace(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_asyn_prepare_replace begin");
	transaction_find_node(job);
	// update key expire time
	if (job.request_operation() &&
	    job.update_key_expire_time(max_expire_time_) != 0) {
		job.set_error(-EC_BAD_INVALID_FIELD, CACHE_SVC,
			      "key expire time illegal");
		return DTC_CODE_BUFFER_ERROR;
	}
	// missing & empty insert it, otherwise replace it
	switch (node_status) {
	case DTC_CODE_NODE_EMPTY:
	case DTC_CODE_NODE_NOTFOUND:
		return buffer_insert_row(job, false, true /* setrows */);
	case DTC_CODE_NODE_HIT:
		break;
	}
	BufferResult cacheRet = check_and_expire(job);
	if (cacheRet == DTC_CODE_BUFFER_ERROR) {
		return cacheRet;
	} else if (cacheRet == DTC_CODE_BUFFER_SUCCESS) {
		node_status = DTC_CODE_NODE_NOTFOUND;
		cache_transaction_node = Node();
		return buffer_insert_row(job, false, true /* setrows */);
	}
	return buffer_replace_rows(job, false, true);
}

BufferResult BufferProcessAskChain::buffer_nodb_delete(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_fullmode_delete begin");
	transaction_find_node(job);
	// missing & empty is NO-OP
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
	case DTC_CODE_NODE_EMPTY:
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_HIT:
		break;
	}
	return buffer_delete_rows(job);
}

BufferResult BufferProcessAskChain::buffer_async_insert(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_async_insert begin");
	// 这种模式下，不支持insert操作
	if (m_bReplaceEmpty == true) {
		job.set_error(
			-EC_BAD_COMMAND, CACHE_SVC,
			"insert cmd from client, not support under replace mode");
		log4cplus_info(
			"insert cmd from client, not support under replace mode");
		return DTC_CODE_BUFFER_ERROR;
	}
	transaction_find_node(job);
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		if (full_mode_ == false)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		if (update_mode_ == MODE_FLUSH)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		break;
	case DTC_CODE_NODE_EMPTY:
		if (update_mode_ == MODE_FLUSH)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		break;
	case DTC_CODE_NODE_HIT:
		if (check_allowed_insert(job) == DTC_CODE_BUFFER_ERROR)
			return DTC_CODE_BUFFER_ERROR;
		if (update_mode_ == MODE_FLUSH &&
		    !(cache_transaction_node.is_dirty()))
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		break;
	}
	log4cplus_debug("%s", "buffer_async_insert data begin");
	// 对insert 操作命中数据进行采样
	++stat_insert_hits_;

	return buffer_insert_row(job, true /* async */, true /* setrows */);
}

BufferResult BufferProcessAskChain::buffer_async_update(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_asyn_update begin");
	transaction_find_node(job);
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		if (full_mode_ == false)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		// FALLTHROUGH
	case DTC_CODE_NODE_EMPTY:
		if (m_bReplaceEmpty == true) {
			if (update_mode_ == MODE_FLUSH)
				return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
			break;
		}
		return DTC_CODE_BUFFER_SUCCESS;
	case DTC_CODE_NODE_HIT:
		if (update_mode_ == MODE_FLUSH &&
		    !(cache_transaction_node.is_dirty()))
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		break;
	}

	log4cplus_debug("%s", "buffer_async_update update data begin");
	// 对update 操作命中数据进行采样
	++stat_update_hits_;
	return buffer_update_rows(job, true /*Async*/, true /*setrows*/);
}

BufferResult BufferProcessAskChain::buffer_async_replace(DTCJobOperation &job)
{
	log4cplus_debug("%s", "buffer_asyn_prepare_replace begin");
	transaction_find_node(job);
	switch (node_status) {
	case DTC_CODE_NODE_NOTFOUND:
		if (full_mode_ == false)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		if (update_mode_ == MODE_FLUSH)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		if (table_define_infomation_->key_as_uniq_field() == false)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		return buffer_insert_row(job, true, true);
	case DTC_CODE_NODE_EMPTY:
		if (update_mode_ == MODE_FLUSH)
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		return buffer_insert_row(job, true, true);
	case DTC_CODE_NODE_HIT:
		if (update_mode_ == MODE_FLUSH &&
		    !(cache_transaction_node.is_dirty()))
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		break;
	}
	return buffer_replace_rows(job, true, true);
}

/*
 * Function		: deal_single_database_addition_ask
 * Description	: 处理incoming job
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 0 			成功
 *				: -1			失败
 */
BufferResult
BufferProcessAskChain::deal_single_database_addition_ask(DTCJobOperation &job)
{
	job.renew_timestamp();
	error_message_[0] = 0;
	job.field_type(0);
	// 取命令字
	int iCmd = job.request_code();
	log4cplus_debug(
		"BufferProcessAskChain::deal_single_database_addition_ask cmd is %d ",
		iCmd);
	switch (iCmd) {
	case DRequest::Get:
		// set hit flag init status
		job.set_result_hit_flag(HIT_INIT);
		if (job.count_only() && (job.requestInfo.limit_start() ||
					 job.requestInfo.limit_count())) {
			job.set_error(
				-EC_BAD_COMMAND, CACHE_SVC,
				"There's nothing to limit because no fields required");
			return DTC_CODE_BUFFER_ERROR;
		}
		// 如果命中黑名单，则purge掉当前节点，走PassThru模式
		if (black_list_->in_blacklist(job.packed_key())) {
			/* 
				 * 理论上是在黑名单的节点是不可能在cache中的
				 * 为了防止异常，预purge。
				 */
			log4cplus_debug(
				"blacklist hit, passthough to datasource");
			buffer_purge_data(job);
			job.mark_as_pass_thru();
			return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		}
		log4cplus_debug("blacklist miss, normal process");
		++stat_get_count_;
		return buffer_get_data(job);
	case DRequest::Insert:
		++stat_insert_count_;
		if (update_mode_ == MODE_ASYNC && insert_mode_ != MODE_SYNC)
			return buffer_async_insert(job);
		// 标示task将提交给helper
		return buffer_sync_insert_precheck(job);
	case DRequest::Update:
		++stat_update_count_;
		if (update_mode_)
			return buffer_async_update(job);
		// 标示task将提交给helper
		return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
		// 如果clinet 上送Delete 操作，删除Cache中数据，同时提交Helper
		// 现阶段异步Cache暂时不支持Delete操作
	case DRequest::Delete:
		if (update_mode_ != MODE_SYNC) {
			if (job.request_condition() &&
			    job.request_condition()->has_type_rw()) {
				job.set_error(
					-EC_BAD_ASYNC_CMD, CACHE_SVC,
					"Delete base non ReadOnly fields");
				return DTC_CODE_BUFFER_ERROR;
			}
			// 异步delete前先flush
			BufferResult iRet = DTC_CODE_BUFFER_SUCCESS;
			iRet = buffer_flush_data_before_delete(job);
			if (iRet == DTC_CODE_BUFFER_ERROR)
				return iRet;
		}
		// 对于delete操作，直接提交DB，不改变原有逻辑
		++stat_delete_count_;
		// 标示task将提交给helper
		return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
	case DRequest::Purge:
		// 删除指定key在cache中的数据
		++stat_purge_count_;
		return buffer_purge_data(job);
	case DRequest::Flush:
		if (update_mode_)
			// flush指定key在cache中的数据
			return buffer_flush_data(job);
		else
			return DTC_CODE_BUFFER_SUCCESS;
	case DRequest::Replace:
		// 如果是淘汰的数据，不作处理
		++stat_update_count_;
		// 限制key字段作为唯一字段才能使用replace命令
		if (!(job.table_definition()->key_part_of_uniq_field()) ||
		    job.table_definition()->has_auto_increment()) {
			job.set_error(
				-EC_BAD_COMMAND, CACHE_SVC,
				"replace cmd require key fields part of uniq-fields and no auto-increment field");
			return DTC_CODE_BUFFER_ERROR;
		}
		if (update_mode_)
			return buffer_async_replace(job);
		// 标示task将提交给helper
		return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
	case DRequest::TYPE_SYSTEM_COMMAND:
		return buffer_process_admin(job);
	default:
		job.set_error(-EC_BAD_COMMAND, CACHE_SVC,
			      "invalid cmd from client");
		log4cplus_info("invalid cmd[%d] from client", iCmd);
		break;
	}
	return DTC_CODE_BUFFER_ERROR;
}

/*
 * Function		: deal_batch_database_addition_ask
 * Description	: 处理incoming batch job
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 0 			成功
 *				: -1			失败
 */
BufferResult
BufferProcessAskChain::deal_batch_database_addition_ask(DTCJobOperation &job)
{
	job.renew_timestamp();
	error_message_[0] = 0;

	// 取命令字
	int iCmd = job.request_code();
	if (node_empty_limit_) {
		int bsize = job.get_batch_size();
		if (bsize * 10 > node_empty_limit_) {
			job.set_error(-EC_TOO_MANY_KEY_VALUE, __FUNCTION__,
				      "batch count exceed LimitEmptyNodes/10");
			return DTC_CODE_BUFFER_ERROR;
		}
	}
	switch (iCmd) {
	case DRequest::Get:
		return buffer_batch_get_data(job);

		// unknwon command treat as OK, fallback to split mode
	default:
		break;
	}
	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * Function		: reply_connector_answer
 * Description	: 处理task from helper reply
 * Input			: job			请求信息
 * Output		: job			返回信息
 * Return		: 0 			成功
 *				: -1			失败
 */

BufferResult BufferProcessAskChain::reply_connector_answer(DTCJobOperation &job)
{
	job.renew_timestamp();
	error_message_[0] = '\0';
	int iLimit = 0;

	int iCmd = job.request_code();
	switch (iCmd) {
	// 一定是cache miss,全部replace入cache
	case DRequest::Get:
		if (job.flag_pass_thru()) {
			if (job.result)
				job.pass_all_result(job.result);
			return DTC_CODE_BUFFER_SUCCESS;
		}

		// ATTN: if failed, node always purged
		if (job.result &&
		    ((node_size_limit_ > 0 &&
		      job.result->data_len() >= node_size_limit_) ||
		     (node_rows_limit_ > 0 &&
		      job.result->total_rows() >= node_rows_limit_))) {
			log4cplus_error(
				"key[%d] rows[%d] size[%d] exceed limit",
				job.int_key(), job.result->total_rows(),
				job.result->data_len());
			iLimit = 1;
		}
		// don't add empty node if job back from blackhole
		if (!iLimit && !job.flag_black_hole())
			buffer_replace_result(job);

		return buffer_get_rb(job);
		// 没有回读则必定是multirow,新数据附在原有数据后面
	case DRequest::Insert:
		if (job.flag_black_hole())
			return buffer_nodb_insert(job);
		if (insert_order_ == INSERT_ORDER_PURGE) {
			buffer_purge_data(job);
			return DTC_CODE_BUFFER_SUCCESS;
		}
		return buffer_sync_insert(job);
	case DRequest::Update:
		if (job.flag_black_hole())
			return buffer_nodb_update(job);

		if (insert_order_ == INSERT_ORDER_PURGE &&
		    job.resultInfo.affected_rows() > 0) {
			buffer_purge_data(job);
			return DTC_CODE_BUFFER_SUCCESS;
		}
		return buffer_sync_update(job);
	case DRequest::Delete:
		if (job.flag_black_hole())
			return buffer_nodb_delete(job);
		return buffer_sync_delete(job);
	case DRequest::Replace:
		if (job.flag_black_hole())
			return buffer_nodb_replace(job);
		return buffer_sync_replace(job);
	case DRequest::TYPE_SYSTEM_COMMAND:
		if (job.requestInfo.admin_code() ==
		    DRequest::SystemCommand::Migrate) {
			const DTCFieldValue *condition =
				job.request_condition();
			const DTCValue *key = condition->field_value(0);
			Node node =
				cache_.cache_find_auto_chose_hash(key->bin.ptr);
			int rows = cache_.node_rows_count(node);
			log4cplus_debug("migrate replay ,row %d", rows);
			cache_.inc_total_row(0LL - rows);
			cache_.purge_node_and_data(key->bin.ptr, node);
			log4cplus_debug("should purgenode everything");
			g_key_route_ask_instance->key_migrated(key->bin.ptr);
			delete (job.request_operation());
			job.set_request_operation(NULL);
			return DTC_CODE_BUFFER_SUCCESS;
		}
		if (job.requestInfo.admin_code() ==
			    DRequest::SystemCommand::MigrateDB ||
		    job.requestInfo.admin_code() ==
			    DRequest::SystemCommand::MigrateDBSwitch) {
			return DTC_CODE_BUFFER_SUCCESS;
		} else {
			job.set_error(-EC_BAD_COMMAND, CACHE_SVC,
				      "invalid cmd from helper");
		}
	case DRequest::Replicate:
		// 处理主从同步
		return buffer_process_replicate(job);
	default:
		job.set_error(-EC_BAD_COMMAND, CACHE_SVC,
			      "invalid cmd from helper");
	}

	return DTC_CODE_BUFFER_ERROR;
}

BufferResult
BufferProcessAskChain::deal_single_cache_only_ask(DTCJobOperation &job)
{
	// nodb mode always blackhole-d
	job.mark_as_black_hole();
	job.renew_timestamp();
	error_message_[0] = 0;
	// 取命令字
	int iCmd = job.request_code();
	switch (iCmd) {
	case DRequest::Get:
		if (job.count_only() && (job.requestInfo.limit_start() ||
					 job.requestInfo.limit_count())) {
			job.set_error(
				-EC_BAD_COMMAND, CACHE_SVC,
				"There's nothing to limit because no fields required");
			return DTC_CODE_BUFFER_ERROR;
		}
		++stat_get_count_;
		job.set_result_hit_flag(HIT_INIT);
		return buffer_get_data(job);
	case DRequest::Insert:
		++stat_insert_count_;
		return buffer_nodb_insert(job);
	case DRequest::Update:
		++stat_update_count_;
		return buffer_nodb_update(job);
	case DRequest::Delete:
		++stat_delete_count_;
		return buffer_nodb_delete(job);
	case DRequest::Purge:
		//删除指定key在cache中的数据
		++stat_purge_count_;
		return buffer_purge_data(job);
	case DRequest::Flush:
		return DTC_CODE_BUFFER_SUCCESS;
		// 如果是淘汰的数据，不作处理
	case DRequest::Replace:
		++stat_update_count_;
		// 限制key字段作为唯一字段才能使用replace命令
		if (!(job.table_definition()->key_part_of_uniq_field()) ||
		    job.table_definition()->has_auto_increment()) {
			job.set_error(
				-EC_BAD_COMMAND, CACHE_SVC,
				"replace cmd require key fields part of uniq-fields and no auto-increment field");
			return DTC_CODE_BUFFER_ERROR;
		}
		return buffer_nodb_replace(job);
	case DRequest::TYPE_SYSTEM_COMMAND:
		return buffer_process_admin(job);
	default:
		job.set_error(-EC_BAD_COMMAND, CACHE_SVC,
			      "invalid cmd from client");
		log4cplus_info("invalid cmd[%d] from client", iCmd);
		break;
	}
	return DTC_CODE_BUFFER_ERROR;
}

/*
 * 当DTC后端使用诸如Rocksdb之类的单机内嵌式持久引擎时，主从同步需要从存储侧拉取全量
 * 数据，这里处理从存储引擎侧的返回值并返回给hotback主从同步端，注意：不对当前cache
 * 做任何更改
 * 
 */
BufferResult
BufferProcessAskChain::buffer_process_replicate(DTCJobOperation &job)
{
	//	int iRet;
	log4cplus_info("do cache process replicate start!");
	// switch back the tabledef
	job.set_request_code(DRequest::TYPE_SYSTEM_COMMAND);
	// 数据库回来的记录如果是0行，则表示全量同步结束
	if ((job.result == NULL || job.result->total_rows() == 0)) {
		log4cplus_info("full replicate stage finished! key:[%u]",
			       job.int_key());
		job.set_table_definition(job.get_replicate_table());
		job.set_error(-EC_FULL_SYNC_COMPLETE,
			      "buffer_process_replicate",
			      "full sync finished!");
		return DTC_CODE_BUFFER_ERROR;
	}
	// 处理返回值
	RowValue row(job.get_replicate_table());
	RawData rawdata(&g_stSysMalloc, 1);
	job.prepare_result_no_limit();
	if (job.result != NULL) {
		ResultSet *pstResultSet = job.result;
		for (int i = 0; i < pstResultSet->total_rows(); i++) {
			RowValue *pstRow = pstResultSet->_fetch_row();
			if (pstRow == NULL) {
				log4cplus_info("%s!",
					       "call FetchRow func error");
				rawdata.destory();
				// hotback can not handle error exception now, just continue
				log4cplus_error(
					"replicate: get data from storage failed!");
				continue;
			}
			// 设置key
			job.set_request_key(pstRow->field_value(0));
			job.build_packed_key();
			row[2] = (*pstRow)[0];
			// only bring back the key list
			log4cplus_debug("append_row flag");
			job.append_row(&row);
			rawdata.destory();
		}
	}
	log4cplus_info("do cache process replicate finished! ");
	job.set_table_definition(job.get_replicate_table());
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::reply_flush_answer(DTCJobOperation &job)
{
	error_message_[0] = '\0';
	int iCmd = job.request_code();
	switch (iCmd) {
	// 如果是淘汰的数据，不作处理
	case DRequest::Replace:
		return DTC_CODE_BUFFER_SUCCESS;
	default:
		job.set_error(-EC_BAD_COMMAND, CACHE_SVC,
			      "invalid cmd from helper");
	}
	return DTC_CODE_BUFFER_ERROR;
}

BufferResult BufferProcessAskChain::deal_flush_exeption(DTCJobOperation &job)
{
	// do_execute timeout
	error_message_[0] = '\0';
	switch (job.request_code()) {
	case DRequest::Insert:
		if (lossy_mode_ == true && job.result_code() == -ER_DUP_ENTRY) {
			// upstream is un-trusted
			job.renew_timestamp();
			return buffer_sync_insert(job);
		}
		// FALLTHROUGH
	case DRequest::Delete:
		switch (job.result_code()) {
		case -EC_UPSTREAM_ERROR:
		case -CR_SERVER_LOST:
			if (update_mode_ == MODE_SYNC) {
				log4cplus_info(
					"SQL do_execute result unknown, purge data");
				buffer_purge_data(job);
			} else {
				log4cplus_error(
					"SQL do_execute result unknown, data may be corrupted");
			}
			break;
		}
		break;
	case DRequest::Update:
		switch (job.result_code()) {
		case -ER_DUP_ENTRY:
			if (lossy_mode_ == true) {
				// upstream is un-trusted
				job.renew_timestamp();
				return buffer_sync_update(job);
			}
			// FALLTHROUGH
		case -EC_UPSTREAM_ERROR:
		case -CR_SERVER_LOST:
			if (update_mode_ == MODE_SYNC) {
				log4cplus_info(
					"SQL do_execute result unknown, purge data");
				buffer_purge_data(job);
			}
			// must be cache miss
			break;
		}
		break;
	}
	return DTC_CODE_BUFFER_ERROR;
}

BufferResult BufferProcessAskChain::check_and_expire(DTCJobOperation &job)
{
	uint32_t expire, now;
	int iRet = data_process_->get_expire_time(
		job.table_definition(), &cache_transaction_node, expire);
	if (iRet != 0) {
		log4cplus_error("get_expire_time failed");
		job.set_error_dup(-EIO, CACHE_SVC,
				  data_process_->get_err_msg());
		return DTC_CODE_BUFFER_ERROR;
	}
	if (expire != 0 && expire <= (now = time(NULL))) {
		// expired
		++stat_expire_count_;
		log4cplus_debug(
			"key: %u expired, purge current key when update, expire time: %d, current time: %d",
			job.int_key(), expire, now);
		if (job.request_code() == DRequest::Get) {
			job.prepare_result();
			job.set_total_rows(0);
		}
		cache_.inc_total_row(
			0LL - cache_.node_rows_count(cache_transaction_node));
		if (cache_.cache_purge(key) != 0)
			log4cplus_error("PANIC: purge node[id=%u] fail",
					cache_transaction_node.node_id());
		return DTC_CODE_BUFFER_SUCCESS;
	}
	return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
}

void BufferProcessAskChain::job_ask_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("BufferProcessAskChain enter job_ask_procedure");
	table_define_infomation_ =
		TableDefinitionManager::instance()->get_cur_table_def();
	uint64_t now_unix_time = GET_TIMESTAMP() / 1000;
	if (job_operation->is_expired(now_unix_time)) {
		log4cplus_debug(
			"job time out, throw it for availability, now is [%lld] expire is [%lld]",
			(long long)now_unix_time,
			(long long)job_operation->get_expire_time());
		stat_buffer_process_expire_count_++;
		job_operation->set_error(-EC_TASK_TIMEOUT,
					 "buffer_process_unit", "job time out");
		job_operation->turn_around_job_answer();
		return;
	}

	unsigned blacksize = 0;
	transaction_begin(job_operation);

	if (job_operation->result_code() < 0) {
		job_operation->mark_as_hit(); /* mark as hit if result done */
		job_operation->turn_around_job_answer();
	} else if (job_operation->is_batch_request()) {
		switch (deal_batch_database_addition_ask(*job_operation)) {
		default:
			job_operation->set_error(-EC_SERVER_ERROR,
						 "buffer_process_unit",
						 last_error_message());
			job_operation
				->mark_as_hit(); /* mark as hit if result done */
			job_operation->turn_around_job_answer();
			break;

		case DTC_CODE_BUFFER_SUCCESS:
			job_operation
				->mark_as_hit(); /* mark as hit if result done */
			job_operation->turn_around_job_answer();
			break;

		case DTC_CODE_BUFFER_ERROR:
			if (job_operation->result_code() >= 0)
				job_operation->set_error(-EC_SERVER_ERROR,
							 "buffer_process_unit",
							 last_error_message());
			job_operation
				->mark_as_hit(); /* mark as hit if result done */
			job_operation->turn_around_job_answer();
			break;
		}
	} else if (dtc_mode_ == DTC_MODE_DATABASE_ADDITION) {
		BufferResult result =
			deal_single_database_addition_ask(*job_operation);
		switch (result) {
		default:
			if (!job_operation->flag_black_hole()) {
				// add to black list.
				blacksize =
					job_operation->pop_black_list_size();
				if (blacksize > 0) {
					log4cplus_debug(
						"add to blacklist, key=%d size=%u",
						job_operation->int_key(),
						blacksize);
					black_list_->add_blacklist(
						job_operation->packed_key(),
						blacksize);
				}
			}
		case DTC_CODE_BUFFER_ERROR:
			if (job_operation->result_code() >= 0)
				job_operation->set_error(-EC_SERVER_ERROR,
							 "buffer_process",
							 last_error_message());

		case DTC_CODE_BUFFER_SUCCESS:
			job_operation
				->mark_as_hit(); /* mark as hit if result done */
			job_operation->turn_around_job_answer();
			break;
		case DTC_CODE_BUFFER_GOTO_NEXT_CHAIN:
			log4cplus_debug("push job to next-unit");
			job_operation->push_reply_dispatcher(&cache_reply_);

			main_chain.job_ask_procedure(job_operation);
			break;
		case DTC_CODE_BUFFER_UNFINISHED:
			break;
		case DTC_CODE_BUFFER_TO_REMOTE_TARGET: //migrate command，to remote dtc target.
			job_operation->push_reply_dispatcher(&cache_reply_);
			remote_chain.job_ask_procedure(job_operation);
			break;
		case DTC_CODE_BUFFER_TO_HOTBACKUP_TARGET: {
			log4cplus_debug("push job to hotback-up thread");
			break;
		}
		}
	} else if (dtc_mode_ == DTC_MODE_CACHE_ONLY) {
		BufferResult result =
			deal_single_cache_only_ask(*job_operation);
		switch (result) {
		default:
		case DTC_CODE_BUFFER_ERROR:
			if (job_operation->result_code() >= 0)
				job_operation->set_error(-EC_SERVER_ERROR,
							 "buffer_process_unit",
							 last_error_message());

		case DTC_CODE_BUFFER_GOTO_NEXT_CHAIN:
		case DTC_CODE_BUFFER_SUCCESS:
			job_operation
				->mark_as_hit(); /* mark as hit if result done */
			job_operation->turn_around_job_answer();
			break;
		case DTC_CODE_BUFFER_UNFINISHED:
			break;
		case DTC_CODE_BUFFER_TO_REMOTE_TARGET: //migrate command，to remote dtc target.
			job_operation->push_reply_dispatcher(&cache_reply_);
			remote_chain.job_ask_procedure(job_operation);
			break;
		case DTC_CODE_BUFFER_TO_HOTBACKUP_TARGET: {
			log4cplus_debug("push job to hotback thread");
			break;
		}
		}
	} else {
		log4cplus_error("dtc mode error: %d", dtc_mode_);
	}

	transaction_end();

	//delay purge.
	cache_.delay_purge_notify();
	log4cplus_debug("BufferProcessAskChain leave job_ask_procedure");
}

void BufferProcessAskChain::job_answer_procedure(DTCJobOperation *job_operation)
{
	if (DRequest::ReloadConfig == job_operation->request_code() &&
	    TaskTypeHelperReloadConfig == job_operation->request_type()) {
		/* delete job only */
		log4cplus_debug("reload config job reply ,just delete job");
		delete job_operation;
		return;
	}

	transaction_begin(job_operation);

	if (job_operation->result_code() < 0) {
		deal_flush_exeption(*job_operation);
	} else if (job_operation->result_code() > 0) {
		log4cplus_info("result_code() > 0: from %s msg %s",
			       job_operation->resultInfo.error_from(),
			       job_operation->resultInfo.error_message());
	}
	if (job_operation->result_code() >= 0 &&
	    reply_connector_answer(*job_operation) != DTC_CODE_BUFFER_SUCCESS) {
		if (job_operation->result_code() >= 0)
			job_operation->set_error(-EC_SERVER_ERROR,
						 "reply_connector_answer",
						 last_error_message());
	}

	if (!job_operation->flag_black_hole()) {
		// add to black list.
		unsigned blacksize = job_operation->pop_black_list_size();
		if (blacksize > 0) {
			log4cplus_debug("add to blacklist, key=%d size=%u",
					job_operation->int_key(), blacksize);
			black_list_->add_blacklist(job_operation->packed_key(),
						   blacksize);
		}
	}

	job_operation->turn_around_job_answer();

	transaction_end();

	//delay purge.
	cache_.delay_purge_notify();
}

MARKER_STAMP BufferProcessAskChain::calculate_current_marker()
{
	time_t now;

	time(&now);
	return now - (now % marker_interval_);
}

void BufferProcessAskChain::set_drop_count(int c)
{
	//	Cache.set_drop_count(c);
}

void BufferProcessAskChain::get_dirty_stat()
{
	//	uint64_t ullMaxNode;
	//	uint64_t ullMaxRow;
	const double rate = 0.9;

	if (PtMalloc::instance()->user_alloc_size() >=
	    PtMalloc::instance()->total_size() * rate) {
		//		ullMaxNode = Cache.get_total_used_node();
		//		ullMaxRow = Cache.total_used_row();
	} else {
		if (PtMalloc::instance()->user_alloc_size() > 0) {
			//			double enlarge = PtMalloc::instance()->total_size() * rate / PtMalloc::instance()->user_alloc_size();
			//			ullMaxNode = (uint64_t)(Cache.get_total_used_node() * enlarge);
			//			ullMaxRow = (uint64_t)(Cache.total_used_row() * enlarge);
		} else {
			//			ullMaxNode = 0;
			//			ullMaxRow = 0;
		}
	}
}

void BufferProcessAskChain::set_flush_parameter(int intvl, int mreq,
						int mintime, int maxtime)
{
	// require v4 cache
	if (cache_.get_cache_info()->version < 4)
		return;

	/*
	if(intvl < 60)
		intvl = 60;
	else if(intvl > 43200)
		intvl = 43200;
	*/

	/* marker time interval changed to 1sec */
	intvl = 1;
	marker_interval_ = intvl;

	/* 1. make sure at least one time marker exist
	 * 2. init first marker time and last marker time
	 * */
	Node stTimeNode = cache_.first_time_marker();
	if (!stTimeNode)
		cache_.insert_time_marker(calculate_current_marker());
	cache_.first_time_marker_time();
	cache_.last_time_marker_time();

	if (mreq <= 0)
		mreq = 1;
	if (mreq > 10000)
		mreq = 10000;

	if (mintime < 10)
		mintime = 10;
	if (maxtime <= mintime)
		maxtime = mintime * 2;

	max_flush_request_ = mreq;
	min_dirty_time_ = mintime;
	max_dirty_time_ = maxtime;

	//get_dirty_stat();

	/*attach timer only if async mode or sync mode but mem dirty*/
	if (update_mode_ == MODE_ASYNC ||
	    (update_mode_ == MODE_SYNC && memory_dirty_ == true)) {
		/* check for expired dirty node every second */
		flush_timer_ = owner->get_timer_list(1);
		attach_timer(flush_timer_);
	}
}

int BufferProcessAskChain::commit_flush_request(DTCFlushRequest *req,
						DTCJobOperation *callbackTask)
{
	req->wait = callbackTask;

	if (req->numReq == 0)
		delete req;
	else
		current_pend_flush_request_++;

	stat_currentFlush_request_ = current_pend_flush_request_;
	return 0;
}

void BufferProcessAskChain::complete_flush_request(DTCFlushRequest *req)
{
	delete req;
	current_pend_flush_request_--;
	stat_currentFlush_request_ = current_pend_flush_request_;

	calculate_flush_speed(0);

	if (current_pend_flush_request_ < pend_flush_request_)
		flush_next_node();
}

void BufferProcessAskChain::job_timer_procedure(void)
{
	log4cplus_debug("enter timer procedure");
	int ret = 0;

	MARKER_STAMP job_operation = calculate_current_marker();
	if (cache_.first_time_marker_time() != job_operation)
		cache_.insert_time_marker(job_operation);

	calculate_flush_speed(1);

	/* flush next node return
	 * 1: no dirty node exist, sync dtc, should not attach timer again
	 * 0: one flush request created, nFlushReq inc in flush_next_node, notinue
	 * others: on flush request created due to some reason, should break for another flush timer event, otherwise may be    
	 * block here, eg. no dirty node exist, and in async mode
	 * */
	while (current_pend_flush_request_ < pend_flush_request_) {
		ret = flush_next_node();
		if (ret == 0) {
			continue;
		} else {
			break;
		}
	}

	/*SYNC + memory_dirty_/ASYNC need to reattach flush timer*/
	if ((update_mode_ == MODE_SYNC && memory_dirty_ == true) ||
	    update_mode_ == MODE_ASYNC)
		attach_timer(flush_timer_);

	log4cplus_debug("leave timer procedure");
}

int BufferProcessAskChain::oldest_dirty_node_alarm()
{
	Node stHead = cache_.dirty_lru_head();
	Node stNode = stHead.Prev();

	if (cache_.is_time_marker(stNode)) {
		stNode = stNode.Prev();
		if (cache_.is_time_marker(stNode) || stNode == stHead) {
			return 0;
		} else {
			return 1;
		}
	} else if (stNode == stHead) {
		return 0;
	} else {
		return 1;
	}
}

/*flush speed(nFlushReq) only depend on oldest dirty node existing time*/
void BufferProcessAskChain::calculate_flush_speed(int is_flush_timer)
{
	delete_tail_time_markers();

	// time base
	int m, v;
	unsigned int t1 = cache_.first_time_marker_time();
	unsigned int t2 = cache_.last_time_marker_time();
	//initialized t1 and t2, so no need of test for this
	v = t1 - t2;

	//if start with sync and mem dirty, flush as fast as we can
	if (update_mode_ == MODE_SYNC) {
		if (memory_dirty_ == false) {
			pend_flush_request_ = 0;
		} else {
			pend_flush_request_ = max_flush_request_;
		}
		goto __stat;
	}

	//alarm if oldest dirty node exist too much time, flush at fastest speed
	if (v >= max_dirty_time_) {
		pend_flush_request_ = max_flush_request_;
		if (oldest_dirty_node_alarm() && is_flush_timer) {
			log4cplus_info(
				"oldest dirty node exist time > max dirty time");
		}
	} else if (v >= min_dirty_time_) {
		m = 1 + (v - min_dirty_time_) * (max_flush_request_ - 1) /
				(max_dirty_time_ - min_dirty_time_);
		if (m > pend_flush_request_)
			pend_flush_request_ = m;
	} else {
		pend_flush_request_ = 0;
	}

__stat:
	if (pend_flush_request_ > max_flush_request_)
		pend_flush_request_ = max_flush_request_;

	stat_maxflush_request_ = pend_flush_request_;
	stat_oldestdirty_time_ = v;
}

/* return -1: encount the only time marker
 * return  1: no dirty node exist, clear mem dirty
 * return  2: no dirty node exist, in async mode
 * return -2: no flush request created
 * return  0: one flush request created
 * */
int BufferProcessAskChain::flush_next_node(void)
{
	unsigned int affected_count = 0;
	MARKER_STAMP stamp;
	static MARKER_STAMP last_rm_stamp;

	Node stHead = cache_.dirty_lru_head();
	Node stNode = stHead;
	Node stPreNode = stNode.Prev();

	/*case 1: delete continues time marker, until 
     *        encount a normal node/head node, go next
     *        encount the only time marker*/
	while (1) {
		stNode = stPreNode;
		stPreNode = stNode.Prev();

		if (!cache_.is_time_marker(stNode))
			break;

		if (cache_.first_time_marker_time() == stNode.Time()) {
			if (update_mode_ == MODE_SYNC &&
			    memory_dirty_ == true) {
				/* delete this time marker, flush all dirty node */
				cache_.remove_time_marker(stNode);
				stNode = stPreNode;
				stPreNode = stNode.Prev();
				while (stNode != stHead) {
					buffer_flush_data_timer(stNode,
								affected_count);
					stNode = stPreNode;
					stPreNode = stNode.Prev();
				}

				disable_timer();
				memory_dirty_ = false;
				log4cplus_info("mem clean now for sync cache");
				return 1;
			}
			return -1;
		}

		stamp = stNode.Time();
		if (stamp > last_rm_stamp) {
			last_rm_stamp = stamp;
		}

		log4cplus_debug("remove time marker in dirty lru, time %u",
				stNode.Time());
		cache_.remove_time_marker(stNode);
	}

	/*case 2: this the head node, clear mem dirty if nessary, return, should not happen*/
	if (stNode == stHead) {
		if (update_mode_ == MODE_SYNC && memory_dirty_ == true) {
			disable_timer();
			memory_dirty_ = false;
			log4cplus_info("mem clean now for sync cache");
			return 1;
		} else {
			return 2;
		}
	}

	/*case 3: this a normal node, flush it.
     * 	  return -2 if no flush request added to cache process
     * */
	int iRet = buffer_flush_data_timer(stNode, affected_count);
	if (iRet == -1 || iRet == -2 || iRet == -3 || iRet == 1) {
		return -2;
	}

	return 0;
}

void BufferProcessAskChain::delete_tail_time_markers()
{
	Node stHead = cache_.dirty_lru_head();
	Node stNode = stHead;
	Node stPreNode = stNode.Prev();

	while (1) {
		stNode = stPreNode;
		stPreNode = stNode.Prev();

		if (stNode == stHead ||
		    cache_.first_time_marker_time() == stNode.Time())
			break;

		if (cache_.is_time_marker(stNode) &&
		    cache_.is_time_marker(stPreNode))
			cache_.remove_time_marker(stNode);
		else
			break;
	}
}

BufferResult BufferProcessAskChain::buffer_process_admin(DTCJobOperation &Job)
{
	log4cplus_debug("BufferProcess::buffer_process_admin admin_code is %d ",
			Job.requestInfo.admin_code());
	if (Job.requestInfo.admin_code() ==
		    DRequest::SystemCommand::QueryServerInfo ||
	    Job.requestInfo.admin_code() == DRequest::SystemCommand::LogoutHB ||
	    Job.requestInfo.admin_code() ==
		    DRequest::SystemCommand::GetUpdateKey) {
		if (hotbackup_lru_feature_ == NULL) { // 热备功能尚未启动
			Job.set_error(-EBADRQC, CACHE_SVC,
				      "hot-backup not active yet");
			return DTC_CODE_BUFFER_ERROR;
		}
	}

	switch (Job.requestInfo.admin_code()) {
	case DRequest::SystemCommand::QueryServerInfo:
		return buffer_query_serverinfo(Job);

	case DRequest::SystemCommand::RegisterHB:
		return buffer_register_hb(Job);

	case DRequest::SystemCommand::LogoutHB:
		return buffer_logout_hb(Job);

	case DRequest::SystemCommand::GetKeyList:
		return buffer_get_key_list(Job);

	case DRequest::SystemCommand::GetUpdateKey:
		return buffer_get_update_key(Job);

	case DRequest::SystemCommand::GetRawData:
		return buffer_get_raw_data(Job);

	case DRequest::SystemCommand::ReplaceRawData:
		return buffer_replace_raw_data(Job);

	case DRequest::SystemCommand::AdjustLRU:
		return buffer_adjust_lru(Job);

	case DRequest::SystemCommand::VerifyHBT:
		return buffer_verify_hbt(Job);

	case DRequest::SystemCommand::GetHBTime:
		return buffer_get_hbt(Job);

	case DRequest::SystemCommand::kNodeHandleChange:
		return buffer_nodehandlechange(Job);

	case DRequest::SystemCommand::Migrate:
		return buffer_migrate(Job);

	case DRequest::SystemCommand::ClearCache:
		return buffer_clear_cache(Job);

	case DRequest::SystemCommand::MigrateDB:
	case DRequest::SystemCommand::MigrateDBSwitch:
		if (update_mode() || is_mem_dirty()) {
			log4cplus_error("try to migrate when cache is async");
			Job.set_error(-EC_SERVER_ERROR, "cache process",
				      "try to migrate when cache is async");
			return DTC_CODE_BUFFER_ERROR;
		}
		return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;

	case DRequest::SystemCommand::ColExpandStatus:
		return buffer_check_expand_status(Job);

	case DRequest::SystemCommand::col_expand:
		return buffer_column_expand(Job);

	case DRequest::SystemCommand::ColExpandDone:
		return buffer_column_expand_done(Job);

	case DRequest::SystemCommand::ColExpandKey:
		return buffer_column_expand_key(Job);

	default:
		Job.set_error(-EBADRQC, CACHE_SVC,
			      "invalid admin cmd code from client");
		log4cplus_info("invalid admin cmd code[%d] from client",
			       Job.requestInfo.admin_code());
		break;
	}

	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult
BufferProcessAskChain::buffer_check_expand_status(DTCJobOperation &Job)
{
	if (update_mode() || is_mem_dirty()) {
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "try to column expand when cache is async");
		log4cplus_error("try to column expand when cache is async");
		return DTC_CODE_BUFFER_ERROR;
	}

	int ret = 0;
	// get table.conf
	RowValue stRow(Job.table_definition());
	Job.update_row(stRow);
	log4cplus_debug("value[len: %d]", stRow[3].bin.len);
	DTCTableDefinition *t;
	// parse table.conf to tabledef
	// release t by DEC_DELETE, not delete
	if (stRow[3].bin.ptr == NULL ||
	    (t = TableDefinitionManager::instance()->load_buffered_table(
		     stRow[3].bin.ptr)) == NULL) {
		log4cplus_error("expand column with illegal ");
		Job.set_error(-EC_SERVER_ERROR, "cache process table.yaml",
			      "table.yaml illegal");
		return DTC_CODE_BUFFER_ERROR;
	}
	if ((ret = cache_.check_expand_status()) == -1) {
		// check tabledef
		if (t->is_same_table(TableDefinitionManager::instance()
					     ->get_new_table_def())) {
			log4cplus_info(
				"expand same column while expanding, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPAND_DUPLICATE, "cache process",
				"expand same column while expanding, canceled");
		} else {
			log4cplus_error(
				"new expanding job while expand, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPANDING, "cache process",
				"new expanding job while expand, canceled");
		}
		// release t
		DEC_DELETE(t);
		return DTC_CODE_BUFFER_ERROR;
	} else if (ret == -2) {
		log4cplus_error("column expand not enabled");
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "column expand not enabled");
		DEC_DELETE(t);
		return DTC_CODE_BUFFER_ERROR;
	}

	log4cplus_debug("buffer_check_expand_status ok");
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::buffer_column_expand(DTCJobOperation &Job)
{
	int ret = 0;
	// get table.conf
	RowValue stRow(Job.table_definition());
	Job.update_row(stRow);
	log4cplus_debug("value[len: %d]", stRow[3].bin.len);
	DTCTableDefinition *t;
	// parse table.conf to tabledef
	// release t by DEC_DELETE, not delete
	if (stRow[3].bin.ptr == NULL ||
	    (t = TableDefinitionManager::instance()->load_buffered_table(
		     stRow[3].bin.ptr)) == NULL) {
		log4cplus_error("expand column with illegal table.yaml");
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "table.yaml illegal");
		return DTC_CODE_BUFFER_ERROR;
	}
	// check expanding
	if ((ret = cache_.check_expand_status()) == -1) {
		// check tabledef
		if (t->is_same_table(TableDefinitionManager::instance()
					     ->get_new_table_def())) {
			log4cplus_info(
				"expand same column while expanding, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPAND_DUPLICATE, "cache process",
				"expand same column while expanding, canceled");
		} else {
			log4cplus_error(
				"new expanding job while expand, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPANDING, "cache process",
				"new expanding job while expand, canceled");
		}
		// release t
		DEC_DELETE(t);
		return DTC_CODE_BUFFER_ERROR;
	} else if (ret == -2) {
		log4cplus_error("column expand not enabled");
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "column expand not enabled");
		DEC_DELETE(t);
		return DTC_CODE_BUFFER_ERROR;
	}
	if (t->is_same_table(
		    TableDefinitionManager::instance()->get_cur_table_def())) {
		log4cplus_info("expand same column, canceled");
		Job.set_error(-EC_ERR_COL_EXPAND_DUPLICATE, "cache process",
			      "expand same column, canceled");
		DEC_DELETE(t);
		return DTC_CODE_BUFFER_ERROR;
	}
	// if ok
	if (TableDefinitionManager::instance()->get_cur_table_idx() !=
	    cache_.shm_table_idx()) {
		log4cplus_error(
			"tabledefmanager's idx and shm's are different, need restart");
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "tabledefmanager's idx and shm's are different");
		DEC_DELETE(t);
		return DTC_CODE_BUFFER_ERROR;
	}
	// set new table for tabledefmanger
	// copy table.conf to shm
	if ((ret = cache_.try_col_expand(stRow[3].bin.ptr, stRow[3].bin.len)) !=
	    0) {
		log4cplus_error("try col expand error, ret: %d", ret);
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "try col expand error");
		DEC_DELETE(t);
		return DTC_CODE_BUFFER_ERROR;
	}
	TableDefinitionManager::instance()->set_new_table_def(
		t, (cache_.shm_table_idx() + 1));
	TableDefinitionManager::instance()->renew_table_file_def(
		stRow[3].bin.ptr, stRow[3].bin.len);
	TableDefinitionManager::instance()->save_db_config();
	cache_.col_expand(stRow[3].bin.ptr, stRow[3].bin.len);

	if (dtc_mode_ == DTC_MODE_CACHE_ONLY)
		write_hotbackup_log(_DTC_HB_COL_EXPAND_, stRow[3].bin.ptr,
				    stRow[3].bin.len,
				    DTCHotBackup::SYNC_COLEXPAND_CMD);
	log4cplus_debug("buffer_column_expand ok");
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult
BufferProcessAskChain::buffer_column_expand_done(DTCJobOperation &Job)
{
	int ret = 0;
	// get table.conf
	RowValue stRow(Job.table_definition());
	Job.update_row(stRow);
	log4cplus_debug("value[len: %d]", stRow[3].bin.len);
	DTCTableDefinition *t;
	// parse table.conf to tabledef
	// release t by DEC_DELETE, not delete
	if (stRow[3].bin.ptr == NULL ||
	    (t = TableDefinitionManager::instance()->load_buffered_table(
		     stRow[3].bin.ptr)) == NULL) {
		log4cplus_error("expand column with illegal table.yaml");
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "table.yaml illegal");
		return DTC_CODE_BUFFER_ERROR;
	}
	if ((ret = cache_.check_expand_status()) == -2) {
		log4cplus_error(
			"expand done when not expand job begin or feature not enabled");
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      "expand done when not expand job begin");
		return DTC_CODE_BUFFER_ERROR;
	} else if (ret == 0) {
		// check tabledef
		if (t->is_same_table(TableDefinitionManager::instance()
					     ->get_cur_table_def())) {
			log4cplus_info(
				"expand done same column while expanding not start, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPAND_DONE_DUPLICATE,
				"cache process",
				"expand same column while expanding not start, canceled");
		} else {
			log4cplus_error(
				"new expand done job while expanding not start, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPAND_DONE_DISTINCT,
				"cache process",
				"new expanding job while expanding not start, canceled");
		}
		return DTC_CODE_BUFFER_ERROR;
	} else {
		// check tabledef
		if (!t->is_same_table(TableDefinitionManager::instance()
					      ->get_new_table_def())) {
			log4cplus_error(
				"new expand done job while expanding, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPAND_DONE_DISTINCT,
				"cache process",
				"new expanding job done while expanding, canceled");
			return DTC_CODE_BUFFER_ERROR;
		}
	}

	//若是有源的，则重新载入配置文件到helper
	if (dtc_mode_ == DTC_MODE_DATABASE_ADDITION) {
		char *buf = stRow[3].bin.ptr;
		char *bufLocal = (char *)MALLOC(strlen(buf) + 1);
		memset(bufLocal, 0, strlen(buf) + 1);
		strcpy(bufLocal, buf);
		DbConfig *dbconfig = DbConfig::load_buffered(bufLocal);
		FREE(bufLocal);
		if (!dbconfig) {
			log4cplus_error(
				"reload dbconfig for collect failed, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPAND_DONE_DISTINCT,
				"cache process",
				"reload dbconfig for collect failed, canceled");
			return DTC_CODE_BUFFER_ERROR;
		}
		if (collect_load_config(dbconfig)) {
			log4cplus_error(
				"reload config to collect failed, canceled");
			Job.set_error(
				-EC_ERR_COL_EXPAND_DONE_DISTINCT,
				"cache process",
				"reload config to collect failed, canceled");
			return DTC_CODE_BUFFER_ERROR;
		}
	}

	TableDefinitionManager::instance()->renew_cur_table_def();
	TableDefinitionManager::instance()->save_new_table_conf();
	DTCColExpand::instance()->expand_done();

	if (dtc_mode_ == DTC_MODE_CACHE_ONLY)
		write_hotbackup_log(_DTC_HB_COL_EXPAND_DONE_, stRow[3].bin.ptr,
				    stRow[3].bin.len,
				    DTCHotBackup::SYNC_COLEXPAND_CMD);
	log4cplus_debug("buffer_column_expand_done ok");

	//若是有源的，则需要通知work helper重新载入配置文件
	if (dtc_mode_ == DTC_MODE_DATABASE_ADDITION) {
		DTCJobOperation *pJob = new DTCJobOperation(
			TableDefinitionManager::instance()->get_cur_table_def());
		if (NULL == pJob) {
			log4cplus_error(
				"cannot notify work helper reload config, new job error, possible memory exhausted!");
		} else {
			log4cplus_error(
				"notify work helper reload config start!");
			pJob->set_request_type(TaskTypeHelperReloadConfig);
			pJob->set_request_code(DRequest::ReloadConfig);
			pJob->push_reply_dispatcher(&cache_reply_);

			main_chain.job_ask_procedure(pJob);
		}
	}
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult
BufferProcessAskChain::buffer_column_expand_key(DTCJobOperation &Job)
{
	if (cache_.check_expand_status() != -1) {
		log4cplus_error(
			"expand one when not expand job begin or feature not enabled");
		Job.set_error(-EC_ERR_COL_NOT_EXPANDING, "cache process",
			      "expand one when not expand job begin");
		return DTC_CODE_BUFFER_ERROR;
	}
	int iRet = 0;

	const DTCFieldValue *condition = Job.request_condition();
	const DTCValue *key;

	// TODO this may need fix, as we do not check whether this field is key
	if (!condition || condition->num_fields() < 1 ||
	    condition->field_id(0) != 2) {
		Job.set_error(-EC_ERR_COL_NO_KEY, "cache process",
			      "no key value append for col expand");
		log4cplus_error("no key value append for col expand");
		return DTC_CODE_BUFFER_ERROR;
	}
	key = condition->field_value(0);
	Node stNode = cache_.cache_find_auto_chose_hash(key->bin.ptr);
	if (!stNode) {
		log4cplus_info("key not exist for col expand");
		return DTC_CODE_BUFFER_SUCCESS;
	}

	iRet = data_process_->expand_node(Job, &stNode);
	if (iRet == -4) {
		Job.set_error(-EC_ERR_COL_EXPAND_NO_MEM, "cache process",
			      data_process_->get_err_msg());
		log4cplus_error("no mem to expand for key, %s",
				data_process_->get_err_msg());
		return DTC_CODE_BUFFER_ERROR;
	} else if (iRet != 0) {
		Job.set_error(-EC_SERVER_ERROR, "cache process",
			      data_process_->get_err_msg());
		log4cplus_error("expand key error: %s",
				data_process_->get_err_msg());
		return DTC_CODE_BUFFER_ERROR;
	}
	// hotbackup for nodb mode
	if (dtc_mode_ == DTC_MODE_CACHE_ONLY)
		write_hotbackup_log(key->bin.ptr, NULL, 0,
				    DTCHotBackup::SYNC_COLEXPAND);

	log4cplus_debug("buffer_column_expand_key ok");
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::buffer_register_hb(DTCJobOperation &Job)
{
	if (hotbackup_lru_feature_ == NULL) { // 共享内存还没有激活热备特性
		NEW(HBFeature, hotbackup_lru_feature_);
		if (hotbackup_lru_feature_ == NULL) {
			log4cplus_error("new hot-backup feature error: %m");
			Job.set_error(-EC_SERVER_ERROR, "buffer_register_hb",
				      "new hot-backup feature fail");
			return DTC_CODE_BUFFER_ERROR;
		}
		int iRet = hotbackup_lru_feature_->init(time(NULL));
		if (iRet == -ENOMEM) {
			Node stNode;
			if (cache_.try_purge_size(1, stNode) == 0)
				iRet = hotbackup_lru_feature_->init(time(NULL));
		}
		if (iRet != 0) {
			log4cplus_error("init hot-backup feature error: %d",
					iRet);
			Job.set_error(-EC_SERVER_ERROR, "buffer_register_hb",
				      "init hot-backup feature fail");
			return DTC_CODE_BUFFER_ERROR;
		}
		iRet = cache_.add_feature(HOT_BACKUP,
					  hotbackup_lru_feature_->get_handle());
		if (iRet != 0) {
			log4cplus_error("add hot-backup feature error: %d",
					iRet);
			Job.set_error(-EC_SERVER_ERROR, "buffer_register_hb",
				      "add hot-backup feature fail");
			return DTC_CODE_BUFFER_ERROR;
		}
	}
	if (hotbackup_lru_feature_->master_uptime() == 0)
		hotbackup_lru_feature_->master_uptime() = time(NULL);

	//开启变更key日志
	log_hotbackup_key_switch_ = true;

	int64_t hb_timestamp = hotbackup_lru_feature_->master_uptime();
	Job.versionInfo.set_master_hb_timestamp(hb_timestamp);
	Job.versionInfo.set_slave_hb_timestamp(
		hotbackup_lru_feature_->slave_uptime());

	Job.set_request_type(TaskTypeRegisterHbLog);
	dispatch_hot_back_task(&Job);
	return DTC_CODE_BUFFER_TO_HOTBACKUP_TARGET;
}

BufferResult BufferProcessAskChain::buffer_logout_hb(DTCJobOperation &Job)
{
	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * 遍历cache中所有的Node节点
 */
BufferResult BufferProcessAskChain::buffer_get_key_list(DTCJobOperation &Job)
{
	uint32_t lst, lcnt;
	lst = Job.requestInfo.limit_start();
	lcnt = Job.requestInfo.limit_count();

	log4cplus_debug("buffer_get_key_list start, limit[%u %u]", lst, lcnt);

	// if the storage is Rocksdb, do replicate through it directly in full sync stage,
	// just dispath the job to helper unit
	if (dtc_mode_ == DTC_MODE_DATABASE_ADDITION &&
	    dbConfig->dstype == 2 /* rocksdb */) {
		log4cplus_info("proc local replicate!");
		Job.set_request_code(DRequest::Replicate);
		// Job.SetRequestType(TaskTypeHelperReplicate);
		Job.set_request_type(TaskTypeRead);

		// due to the hotback has a different table definition with the normal query, so
		// need to switch table definition during query the storage
		DTCTableDefinition *repTab = Job.table_definition();

		Job.set_table_definition(
			TableDefinitionManager::instance()->get_cur_table_def());
		Job.set_replicate_table(repTab);

		return DTC_CODE_BUFFER_GOTO_NEXT_CHAIN;
	}

	//遍历完所有的Node节点
	if (lst > cache_.max_node_id()) {
		Job.set_error(-EC_FULL_SYNC_COMPLETE, "buffer_get_key_list",
			      "node id is overflow");
		return DTC_CODE_BUFFER_ERROR;
	}

	Job.prepare_result_no_limit();

	RowValue r(Job.table_definition());
	RawData rawdata(&g_stSysMalloc, 1);

	for (unsigned i = lst; i < lst + lcnt; ++i) {
		if (i < cache_.get_min_valid_node_id())
			continue;
		if (i > cache_.max_node_id())
			break;

		//查找对应的Node节点
		Node node = I_SEARCH(i);
		if (!node)
			continue;
		if (node.not_in_lru_list())
			continue;
		if (cache_.is_time_marker(node))
			continue;

		// 解码Key
		DataChunk *keyptr = M_POINTER(DataChunk, node.vd_handle());

		//发送packedkey
		r[2] = TableDefinitionManager::instance()
			       ->get_cur_table_def()
			       ->packed_key(keyptr->key());

		//解码Value
		if (data_process_->get_node_all_rows_count(&node, &rawdata)) {
			rawdata.destory();
			continue;
		}

		r[3].Set((char *)(rawdata.get_addr()),
			 (int)(rawdata.data_size()));

		log4cplus_debug("append_row flag");
		Job.append_row(&r);

		rawdata.destory();
	}

	return DTC_CODE_BUFFER_SUCCESS;
}

/*
 * hot backup拉取更新key或者lru变更，如果没有则挂起请求,直到
 * 1. 超时
 * 2. 有更新key, 或者LRU变更
 */
BufferResult BufferProcessAskChain::buffer_get_update_key(DTCJobOperation &Job)
{
	log4cplus_debug("buffer_get_update_key start");
	Job.set_request_type(TaskTypeReadHbLog);
	dispatch_hot_back_task(&Job);
	return DTC_CODE_BUFFER_TO_HOTBACKUP_TARGET;
}

BufferResult BufferProcessAskChain::buffer_get_raw_data(DTCJobOperation &Job)
{
	int iRet;

	const DTCFieldValue *condition = Job.request_condition();
	const DTCValue *key;

	log4cplus_debug("buffer_get_raw_data start ");

	RowValue stRow(Job.table_definition()); //一行数据
	RawData stNodeData(&g_stSysMalloc, 1);

	Job.prepare_result_no_limit();

	for (int i = 0; i < condition->num_fields(); i++) {
		key = condition->field_value(i);
		stRow[1].u64 = DTCHotBackup::HAS_VALUE; //表示附加value字段
		stRow[2].Set(key->bin.ptr, key->bin.len);

		Node stNode = cache_.cache_find_auto_chose_hash(key->bin.ptr);
		if (!stNode) { //master没有该key的数据
			stRow[1].u64 = DTCHotBackup::KEY_NOEXIST;
			stRow[3].Set(0);
			log4cplus_debug("append_row flag");
			Job.append_row(&stRow);
			continue;
		} else {
			iRet = data_process_->get_node_all_rows_count(
				&stNode, &stNodeData);
			if (iRet != 0) {
				log4cplus_error("get raw-data failed");
				Job.set_error_dup(-EIO, CACHE_SVC,
						  data_process_->get_err_msg());
				return DTC_CODE_BUFFER_ERROR;
			}
			stRow[3].Set((char *)(stNodeData.get_addr()),
				     (int)(stNodeData.data_size()));
		}

		log4cplus_debug("append_row flag");
		Job.append_row(&stRow); //当前行添加到task中
		stNodeData.destory();
	}

	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult
BufferProcessAskChain::buffer_replace_raw_data(DTCJobOperation &Job)
{
	log4cplus_debug("buffer_replace_raw_data start ");

	int iRet;

	const DTCFieldValue *condition = Job.request_condition();
	const DTCValue *key;

	RowValue stRow(Job.table_definition()); //一行数据
	RawData stNodeData(&g_stSysMalloc, 1);
	if (condition->num_fields() < 1) {
		log4cplus_debug("%s", "replace raw data need key");
		Job.set_error_dup(-EC_KEY_NEEDED, CACHE_SVC,
				  data_process_->get_err_msg());
		return DTC_CODE_BUFFER_ERROR;
	}

	key = condition->field_value(0);
	stRow[2].Set(key->bin.ptr, key->bin.len);
	Job.update_row(stRow); //获取数据

	log4cplus_debug("value[len: %d]", stRow[3].bin.len);

	//调整备机的空节点过滤
	if (stRow[1].u64 & DTCHotBackup::EMPTY_NODE && empty_node_filter_) {
		empty_node_filter_->SET(*(unsigned int *)(key->bin.ptr));
	}

	//key在master不存在, 或者是空节点，purge cache.
	if (stRow[1].u64 & DTCHotBackup::KEY_NOEXIST ||
	    stRow[1].u64 & DTCHotBackup::EMPTY_NODE) {
		log4cplus_debug("purge slave data");
		Node stNode = cache_.cache_find_auto_chose_hash(key->bin.ptr);
		int rows = cache_.node_rows_count(stNode);
		log4cplus_debug("migrate replay ,row %d", rows);
		cache_.inc_total_row(0LL - rows);
		cache_.cache_purge(key->bin.ptr);
		return DTC_CODE_BUFFER_SUCCESS;
	}

	// 解析成raw data
	ALLOC_HANDLE_T hData = g_stSysMalloc.Malloc(stRow[3].bin.len);
	if (hData == INVALID_HANDLE) {
		log4cplus_error("malloc error: %m");
		Job.set_error(-ENOMEM, CACHE_SVC, "malloc error");
		return DTC_CODE_BUFFER_ERROR;
	}

	memcpy(g_stSysMalloc.handle_to_ptr(hData), stRow[3].bin.ptr,
	       stRow[3].bin.len);

	if ((iRet = stNodeData.do_attach(
		     hData, 0, table_define_infomation_->key_format())) != 0) {
		log4cplus_error("parse raw-data error: %d, %s", iRet,
				stNodeData.get_err_msg());
		Job.set_error(-EC_BAD_RAW_DATA, CACHE_SVC, "bad raw data");
		return DTC_CODE_BUFFER_ERROR;
	}

	// 检查packed key是否匹配
	DTCValue packed_key = TableDefinitionManager::instance()
				      ->get_cur_table_def()
				      ->packed_key(stNodeData.key());
	if (packed_key.bin.len != key->bin.len ||
	    memcmp(packed_key.bin.ptr, key->bin.ptr, key->bin.len)) {
		log4cplus_error(
			"packed key miss match, key size=%d, packed key size=%d",
			key->bin.len, packed_key.bin.len);
		log4cplus_error("packed key miss match, packed_key %s,key %s",
				packed_key.bin.ptr, key->bin.ptr);
		Job.set_error(-EC_BAD_RAW_DATA, CACHE_SVC,
			      "packed key miss match");
		return DTC_CODE_BUFFER_ERROR;
	}

	// 查找分配node节点
	unsigned int uiNodeID;
	Node stNode = cache_.cache_find_auto_chose_hash(key->bin.ptr);

	if (!stNode) {
		for (int i = 0; i < 2; i++) {
			stNode = cache_.cache_allocation(key->bin.ptr);
			if (!(!stNode))
				break;
			if (cache_.try_purge_size(1, stNode) != 0)
				break;
		}
		if (!stNode) {
			log4cplus_error("alloc cache node error");
			Job.set_error(-EIO, CACHE_SVC,
				      "alloc cache node error");
			return DTC_CODE_BUFFER_ERROR;
		}
		stNode.vd_handle() = INVALID_HANDLE;
	} else {
		cache_.remove_from_lru(stNode);
		cache_.insert_to_clean_lru(stNode);
	}

	uiNodeID = stNode.node_id();

	// 替换数据
	iRet = data_process_->do_replace_all(&stNode, &stNodeData);
	if (iRet != 0) {
		if (dtc_mode_ == DTC_MODE_CACHE_ONLY) {
			/* FIXME: no backup db, can't purge data, no recover solution yet */
			log4cplus_error("cache replace raw data error: %d, %s",
					iRet, data_process_->get_err_msg());
			Job.set_error(-EIO, CACHE_SVC,
				      "ReplaceRawData() error");
			return DTC_CODE_BUFFER_ERROR;
		} else {
			log4cplus_error(
				"cache replace raw data error: %d, %s. purge node: %u",
				iRet, data_process_->get_err_msg(), uiNodeID);
			cache_.purge_node_and_data(key->bin.ptr, stNode);
			return DTC_CODE_BUFFER_SUCCESS;
		}
	}

	cache_.inc_total_row(data_process_->get_increase_row_count());

	log4cplus_debug("buffer_replace_raw_data success! ");

	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::buffer_adjust_lru(DTCJobOperation &Job)
{
	const DTCFieldValue *condition = Job.request_condition();
	const DTCValue *key;

	log4cplus_debug("buffer_adjust_lru start ");

	RowValue stRow(Job.table_definition()); //一行数据

	for (int i = 0; i < condition->num_fields(); i++) {
		key = condition->field_value(i);

		Node stNode;
		int newhash, oldhash;
		if (g_hash_changing) {
			if (g_target_new_hash) {
				oldhash = 0;
				newhash = 1;
			} else {
				oldhash = 1;
				newhash = 0;
			}

			stNode = cache_.cache_find(key->bin.ptr, oldhash);
			if (!stNode) {
				stNode = cache_.cache_find(key->bin.ptr,
							   newhash);
			} else {
				cache_.move_to_new_hash(key->bin.ptr, stNode);
			}
		} else {
			if (g_target_new_hash) {
				stNode = cache_.cache_find(key->bin.ptr, 1);
			} else {
				stNode = cache_.cache_find(key->bin.ptr, 0);
			}
		}
		if (!stNode) {
			//		            continue;
			Job.set_error(-EC_KEY_NOTEXIST, CACHE_SVC,
				      "key not exist");
			return DTC_CODE_BUFFER_ERROR;
		}
		cache_.remove_from_lru(stNode);
		cache_.insert_to_clean_lru(stNode);
	}

	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::buffer_verify_hbt(DTCJobOperation &Job)
{
	log4cplus_debug("buffer_verify_hbt start ");

	if (hotbackup_lru_feature_ == NULL) { // 共享内存还没有激活热备特性
		NEW(HBFeature, hotbackup_lru_feature_);
		if (hotbackup_lru_feature_ == NULL) {
			log4cplus_error("new hot-backup feature error: %m");
			Job.set_error(-EC_SERVER_ERROR, "buffer_register_hb",
				      "new hot-backup feature fail");
			return DTC_CODE_BUFFER_ERROR;
		}
		int iRet = hotbackup_lru_feature_->init(0);
		if (iRet == -ENOMEM) {
			Node stNode;
			if (cache_.try_purge_size(1, stNode) == 0)
				iRet = hotbackup_lru_feature_->init(0);
		}
		if (iRet != 0) {
			log4cplus_error("init hot-backup feature error: %d",
					iRet);
			Job.set_error(-EC_SERVER_ERROR, "buffer_register_hb",
				      "init hot-backup feature fail");
			return DTC_CODE_BUFFER_ERROR;
		}
		iRet = cache_.add_feature(HOT_BACKUP,
					  hotbackup_lru_feature_->get_handle());
		if (iRet != 0) {
			log4cplus_error("add hot-backup feature error: %d",
					iRet);
			Job.set_error(-EC_SERVER_ERROR, "buffer_register_hb",
				      "add hot-backup feature fail");
			return DTC_CODE_BUFFER_ERROR;
		}
	}

	int64_t master_timestamp = Job.versionInfo.master_hb_timestamp();
	if (hotbackup_lru_feature_->slave_uptime() == 0) {
		hotbackup_lru_feature_->slave_uptime() = master_timestamp;
	} else if (hotbackup_lru_feature_->slave_uptime() != master_timestamp) {
		log4cplus_error(
			"hot backup timestamp incorrect, master[%lld], this slave[%lld]",
			(long long)master_timestamp,
			(long long)(hotbackup_lru_feature_->slave_uptime()));
		Job.set_error(-EC_ERR_SYNC_STAGE, "buffer_verify_hbt",
			      "verify hot backup timestamp fail");
		return DTC_CODE_BUFFER_ERROR;
	}

	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::buffer_get_hbt(DTCJobOperation &Job)
{
	log4cplus_debug("buffer_get_hbt start ");

	if (hotbackup_lru_feature_ == NULL) { // 共享内存还没有激活热备特性
		Job.versionInfo.set_master_hb_timestamp(0);
		Job.versionInfo.set_slave_hb_timestamp(0);
	} else {
		Job.versionInfo.set_master_hb_timestamp(
			hotbackup_lru_feature_->master_uptime());
		Job.versionInfo.set_slave_hb_timestamp(
			hotbackup_lru_feature_->slave_uptime());
	}

	log4cplus_debug("master-up-time: %lld, slave-up-time: %lld",
			(long long)(Job.versionInfo.master_hb_timestamp()),
			(long long)(Job.versionInfo.slave_hb_timestamp()));

	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult
BufferProcessAskChain::buffer_query_serverinfo(DTCJobOperation &Job)
{
	log4cplus_debug("buffer_query_serverinfo start");
	Job.set_request_type(TaskTypeQueryHbLogInfo);
	dispatch_hot_back_task(&Job);
	return DTC_CODE_BUFFER_TO_HOTBACKUP_TARGET;
}

/* finished in one cache process cycle */
BufferResult
BufferProcessAskChain::buffer_nodehandlechange(DTCJobOperation &Job)
{
	log4cplus_debug("buffer_nodehandlechange start ");

	const DTCFieldValue *condition = Job.request_condition();
	const DTCValue *key = condition->field_value(0);
	Node node;
	MEM_HANDLE_T node_handle;
	RawData node_raw_data(PtMalloc::instance(), 0);
	/* no need of private raw data, just for copy */
	char *private_buff = NULL;
	int buff_len;
	MEM_HANDLE_T new_node_handle;

	if (condition->num_fields() < 1) {
		log4cplus_debug("%s", "nodehandlechange need key");
		Job.set_error_dup(-EC_KEY_NEEDED, CACHE_SVC,
				  data_process_->get_err_msg());
		return DTC_CODE_BUFFER_ERROR;
	}

	/* packed key -> node id -> node handle -> node raw data -> private buff*/
	int newhash, oldhash;
	if (g_hash_changing) {
		if (g_target_new_hash) {
			oldhash = 0;
			newhash = 1;
		} else {
			oldhash = 1;
			newhash = 0;
		}
		node = cache_.cache_find(key->bin.ptr, oldhash);
		if (!node) {
			node = cache_.cache_find(key->bin.ptr, newhash);
		} else {
			cache_.move_to_new_hash(key->bin.ptr, node);
		}
	} else {
		if (g_target_new_hash) {
			node = cache_.cache_find(key->bin.ptr, 1);
		} else {
			node = cache_.cache_find(key->bin.ptr, 0);
		}
	}

	if (!node) {
		log4cplus_debug("%s", "key not exist for defragmentation");
		Job.set_error(-ER_KEY_NOT_FOUND, CACHE_SVC, "node not found");
		return DTC_CODE_BUFFER_ERROR;
	}

	node_handle = node.vd_handle();
	if (node_handle == INVALID_HANDLE) {
		Job.set_error(-EC_BAD_RAW_DATA, CACHE_SVC, "chunk not exist");
		return DTC_CODE_BUFFER_ERROR;
	}

	node_raw_data.do_attach(node_handle,
				table_define_infomation_->key_fields() - 1,
				table_define_infomation_->key_format());

	if ((private_buff = (char *)MALLOC(node_raw_data.data_size())) ==
	    NULL) {
		log4cplus_error("no mem");
		Job.set_error(-ENOMEM, CACHE_SVC, "malloc error");
		return DTC_CODE_BUFFER_ERROR;
	}

	memcpy(private_buff, node_raw_data.get_addr(),
	       node_raw_data.data_size());
	buff_len = node_raw_data.data_size();
	if (node_raw_data.destory()) {
		log4cplus_error("node raw data detroy error");
		Job.set_error(-ENOMEM, CACHE_SVC, "free error");
		FREE_IF(private_buff);
		return DTC_CODE_BUFFER_ERROR;
	}
	log4cplus_debug("old node handle: " UINT64FMT_T ", raw data size %d",
			node_handle, buff_len);

	/* new chunk */
	/* new node handle -> new node handle ptr <- node raw data ptr*/
	new_node_handle = PtMalloc::instance()->Malloc(buff_len);
	log4cplus_debug("new node handle: " UINT64FMT_T, new_node_handle);

	if (new_node_handle == INVALID_HANDLE) {
		log4cplus_error("malloc error: %m");
		Job.set_error(-ENOMEM, CACHE_SVC, "malloc error");
		FREE_IF(private_buff);
		return DTC_CODE_BUFFER_ERROR;
	}

	memcpy(PtMalloc::instance()->handle_to_ptr(new_node_handle),
	       private_buff, buff_len);

	/* free node raw data, set node handle */
	node.vd_handle() = new_node_handle;
	FREE_IF(private_buff);

	log4cplus_debug("buffer_nodehandlechange success! ");
	return DTC_CODE_BUFFER_SUCCESS;
}

BufferResult BufferProcessAskChain::buffer_migrate(DTCJobOperation &Job)
{
	if (g_key_route_ask_instance == 0) {
		log4cplus_error("not support migrate cmd @ bypass mode");
		Job.set_error(-EC_SERVER_ERROR, "buffer_migrate",
			      "Not Support @ Bypass Mode");
		return DTC_CODE_BUFFER_ERROR;
	}
	int iRet;

	const DTCFieldValue *ui = Job.request_operation();
	const DTCValue key = TableDefinitionManager::instance()
				     ->get_cur_table_def()
				     ->packed_key(Job.packed_key());
	if (key.bin.ptr == 0 || key.bin.len <= 0) {
		Job.set_error(-EC_KEY_NEEDED, "buffer_migrate",
			      "need set migrate key");
		return DTC_CODE_BUFFER_ERROR;
	}

	log4cplus_debug("cache_cache_migrate start ");

	RowValue stRow(Job.table_definition()); //一行数据
	RawData stNodeData(&g_stSysMalloc, 1);

	Node stNode = cache_.cache_find_auto_chose_hash(key.bin.ptr);

	//如果有updateInfo则说明请求从DTC过来
	int flag = 0;
	if (ui && ui->field_value(0)) {
		flag = ui->field_value(0)->s64;
	}
	if ((flag & 0xFF) == DTCMigrate::FROM_SERVER) {
		log4cplus_debug("this migrate cmd is from DTC");
		RowValue stRow(Job.table_definition()); //一行数据
		RawData stNodeData(&g_stSysMalloc, 1);
		stRow[2].Set(key.bin.ptr, key.bin.len);
		Job.update_row(stRow); //获取数据

		log4cplus_debug("value[len: %d]", stRow[3].bin.len);

		//key在master不存在, 或者是空节点，purge cache.
		if (stRow[1].u64 & DTCHotBackup::KEY_NOEXIST ||
		    stRow[1].u64 & DTCHotBackup::EMPTY_NODE) {
			log4cplus_debug("purge slave data");
			cache_.cache_purge(key.bin.ptr);
			return DTC_CODE_BUFFER_SUCCESS;
		}

		// 解析成raw data
		ALLOC_HANDLE_T hData = g_stSysMalloc.Malloc(stRow[3].bin.len);
		if (hData == INVALID_HANDLE) {
			log4cplus_error("malloc error: %m");
			Job.set_error(-ENOMEM, CACHE_SVC, "malloc error");
			return DTC_CODE_BUFFER_ERROR;
		}

		memcpy(g_stSysMalloc.handle_to_ptr(hData), stRow[3].bin.ptr,
		       stRow[3].bin.len);

		if ((iRet = stNodeData.do_attach(
			     hData, 0,
			     table_define_infomation_->key_format())) != 0) {
			log4cplus_error("parse raw-data error: %d, %s", iRet,
					stNodeData.get_err_msg());
			Job.set_error(-EC_BAD_RAW_DATA, CACHE_SVC,
				      "bad raw data");
			return DTC_CODE_BUFFER_ERROR;
		}

		// 检查packed key是否匹配
		DTCValue packed_key = TableDefinitionManager::instance()
					      ->get_cur_table_def()
					      ->packed_key(stNodeData.key());
		if (packed_key.bin.len != key.bin.len ||
		    memcmp(packed_key.bin.ptr, key.bin.ptr, key.bin.len)) {
			log4cplus_error(
				"packed key miss match, key size=%d, packed key size=%d",
				key.bin.len, packed_key.bin.len);

			Job.set_error(-EC_BAD_RAW_DATA, CACHE_SVC,
				      "packed key miss match");
			return DTC_CODE_BUFFER_ERROR;
		}

		// 查找分配node节点
		unsigned int uiNodeID;

		if (!stNode) {
			for (int i = 0; i < 2; i++) {
				stNode = cache_.cache_allocation(key.bin.ptr);
				if (!(!stNode))
					break;
				if (cache_.try_purge_size(1, stNode) != 0)
					break;
			}
			if (!stNode) {
				log4cplus_error("alloc cache node error");
				Job.set_error(-EIO, CACHE_SVC,
					      "alloc cache node error");
				return DTC_CODE_BUFFER_ERROR;
			}
			stNode.vd_handle() = INVALID_HANDLE;
		} else {
			cache_.remove_from_lru(stNode);
			cache_.insert_to_clean_lru(stNode);
		}
		if ((flag >> 8) & 0xFF) //如果为脏节点
		{
			cache_.remove_from_lru(stNode);
			cache_.insert_to_dirty_lru(stNode);
		}

		uiNodeID = stNode.node_id();

		// 替换数据
		iRet = data_process_->do_replace_all(&stNode, &stNodeData);
		if (iRet != 0) {
			if (dtc_mode_ == DTC_MODE_CACHE_ONLY) {
				/* FIXME: no backup db, can't purge data, no recover solution yet */
				log4cplus_error(
					"cache replace raw data error: %d, %s",
					iRet, data_process_->get_err_msg());
				Job.set_error(-EIO, CACHE_SVC,
					      "ReplaceRawData() error");
				return DTC_CODE_BUFFER_ERROR;
			} else {
				log4cplus_error(
					"cache replace raw data error: %d, %s. purge node: %u",
					iRet, data_process_->get_err_msg(),
					uiNodeID);
				cache_.purge_node_and_data(key.bin.ptr, stNode);
				return DTC_CODE_BUFFER_SUCCESS;
			}
		}
		if (write_hotbackup_log(key.bin.ptr, stNode,
					DTCHotBackup::SYNC_UPDATE)) {
			log4cplus_error(
				"buffer_migrate: log update key failed");
		}
		cache_.inc_total_row(data_process_->get_increase_row_count());

		Job.prepare_result_no_limit();

		return DTC_CODE_BUFFER_SUCCESS;
	}

	log4cplus_debug("this migrate cmd is from api");
	//请求从工具过来，我们需要构造请求发给其他dtc

	if (!stNode) {
		Job.set_error(-EC_KEY_NOTEXIST, "buffer_migrate",
			      "this key not found in cache");
		return DTC_CODE_BUFFER_ERROR;
	}
	//获取该节点的raw-data，构建replace请求给后端helper
	iRet = data_process_->get_node_all_rows_count(&stNode, &stNodeData);
	if (iRet != 0) {
		log4cplus_error("get raw-data failed");
		Job.set_error_dup(-EIO, CACHE_SVC,
				  data_process_->get_err_msg());
		return DTC_CODE_BUFFER_ERROR;
	}

	DTCFieldValue *uitmp = new DTCFieldValue(4);
	if (uitmp == NULL) {
		Job.set_error(-EIO, CACHE_SVC,
			      "migrate:new DTCFieldValue error");
		return DTC_CODE_BUFFER_ERROR;
	}
	//id0 {"type", DField::Unsigned, 4, DTCValue::Make(0), 0}
	//type的最后一个字节用来表示请求来着其他dtc还是api
	//倒数第二个字节表示节点是否为脏
	uitmp->add_value(0, DField::Set, DField::Unsigned,
			 DTCValue::Make(DTCMigrate::FROM_SERVER |
					(stNode.is_dirty() << 8)));

	//id1 {"flag", DField::Unsigned, 1, DTCValue::Make(0), 0},
	uitmp->add_value(1, DField::Set, DField::Unsigned,
			 DTCValue::Make(DTCHotBackup::HAS_VALUE));
	//id2 {"key", DField::Binary, 255, DTCValue::Make(0), 0},

	//id3 {"value", DField::Binary, MAXPACKETSIZE, DTCValue::Make(0), 0},

	FREE_IF(Job.migratebuf);
	Job.migratebuf = (char *)calloc(1, stNodeData.data_size());
	if (Job.migratebuf == NULL) {
		log4cplus_error("create buffer failed");
		Job.set_error(-EIO, CACHE_SVC,
			      "migrate:get raw data,create buffer failed");
		return DTC_CODE_BUFFER_ERROR;
	}
	memcpy(Job.migratebuf, (char *)(stNodeData.get_addr()),
	       (int)(stNodeData.data_size()));
	uitmp->add_value(3, DField::Set, DField::Binary,
			 DTCValue::Make(Job.migratebuf,
					stNodeData.data_size()));
	Job.set_request_operation(uitmp);
	g_key_route_ask_instance->key_migrating(stNodeData.key());

	return DTC_CODE_BUFFER_TO_REMOTE_TARGET;
}

BufferResult BufferProcessAskChain::buffer_clear_cache(DTCJobOperation &Job)
{
	if (update_mode_ != MODE_SYNC) {
		log4cplus_error("try to clear cache for async mode, abort...");
		Job.set_error(-EC_SERVER_ERROR, "buffer_clear_cache",
			      "can not clear cache for aync mode, abort");
		return DTC_CODE_BUFFER_ERROR;
	}
	// clean and rebuild
	int64_t mu = 0, su = 0;
	if (hotbackup_lru_feature_ != NULL) {
		mu = hotbackup_lru_feature_->master_uptime();
		su = hotbackup_lru_feature_->slave_uptime();
	}
	// table.conf in shm is set in clear_create
	int ret = cache_.clear_create();
	if (ret < 0) {
		log4cplus_error("clear and create cache error: %s",
				cache_.error());
		if (ret == -1) {
			log4cplus_error("fault error, exit...");
			exit(-1);
		}
		if (ret == -2) {
			log4cplus_error("error, abort...");
			Job.set_error(-EC_SERVER_ERROR, "buffer_clear_cache",
				      "clear cache_ error, abort");
			return DTC_CODE_BUFFER_ERROR;
		}
	}
	data_process_->change_mallocator(PtMalloc::instance());
	// setup hotbackup
	if (hotbackup_lru_feature_ != NULL) {
		hotbackup_lru_feature_->detach();
		// no need consider no enough mem, as mem is just cleared
		hotbackup_lru_feature_->init(0);
		int iRet = cache_.add_feature(
			HOT_BACKUP, hotbackup_lru_feature_->get_handle());
		if (iRet != 0) {
			log4cplus_error("add hot-backup feature error: %d",
					iRet);
			exit(-1);
		}
		hotbackup_lru_feature_->master_uptime() = mu;
		hotbackup_lru_feature_->slave_uptime() = su;
	}
	// hotbackup
	char buf[16];
	memset(buf, 0, sizeof(buf));
	Node node;
	if (write_hotbackup_log(buf, node, DTCHotBackup::SYNC_CLEAR))
		log4cplus_error("hb: log clear cache error");

	return DTC_CODE_BUFFER_SUCCESS;
}
