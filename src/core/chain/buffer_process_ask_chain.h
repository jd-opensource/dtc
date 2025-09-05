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

#ifndef __BUFFER_PROCESS_ASK_CHAIN__
#define __BUFFER_PROCESS_ASK_CHAIN__

#include <sys/mman.h>
#include <time.h>

#include "protocol.h"
#include "value.h"
#include "field/field.h"
#include "section.h"
#include "table/table_def.h"
#include "task/task_request.h"
#include "list/list.h"
#include "fence_queue.h"
#include "buffer_pond.h"
#include "poll/poller_base.h"
#include "config/dbconfig.h"
#include "queue/lqueue.h"
#include "stat_dtc.h"
#include "data_process.h"
#include "empty_filter.h"
#include "namespace.h"
#include "task_pendlist.h"
#include "data_chunk.h"
#include "hb_log.h"
#include "lru_bit.h"
#include "hb_feature.h"
#include "blacklist/blacklist_unit.h"
#include "expire_time.h"
#include "buffer_process_answer_chain.h"

DTC_BEGIN_NAMESPACE

class DTCFlushRequest;
class BufferProcessAskChain;
class DTCTableDefinition;
class TaskPendingList;
enum BufferResult {
	DTC_CODE_BUFFER_ERROR = -1,
	DTC_CODE_BUFFER_SUCCESS = 0,
	DTC_CODE_BUFFER_GOTO_NEXT_CHAIN = 1, // transmit job to connector.
	DTC_CODE_BUFFER_UNFINISHED = 2, // waitting for flush module process.
	DTC_CODE_BUFFER_TO_REMOTE_TARGET = 3, // transmit job remote dtc.
	DTC_CODE_BUFFER_TO_HOTBACKUP_TARGET =
		4 // transmit job to hot back-up progress.
};
typedef unsigned int MARKER_STAMP;

class FlushReplyNotify : public JobAnswerInterface<DTCJobOperation> {
    private:
	BufferProcessAskChain *flush_reply_notify_owner_;

    public:
	FlushReplyNotify(BufferProcessAskChain *buffer_process)
		: flush_reply_notify_owner_(buffer_process)
	{
	}

	virtual ~FlushReplyNotify()
	{
	}
	virtual void job_answer_procedure(DTCJobOperation *);
};

class HotBackReplay : public JobAnswerInterface<DTCJobOperation> {
    public:
	HotBackReplay()
	{
	}
	virtual ~HotBackReplay()
	{
	}
	virtual void job_answer_procedure(DTCJobOperation *job);
};

enum { LRU_NONE = 0,
       LRU_BATCH,
       LRU_READ,
       LRU_WRITE,
       LRU_ALWAYS = 999,
};

enum { DTC_CODE_NODE_NOTFOUND, DTC_CODE_NODE_EMPTY, DTC_CODE_NODE_HIT };

struct CacheTransaction {
	DTCJobOperation *current_task;
	const char *key;
	Node cache_transaction_node;
	int old_rows;
	uint8_t node_status;
	uint8_t key_dirty;
	uint8_t node_empty;
	uint8_t lru_update;
	// OLD ASYNC TRANSATION LOG
	int log_type;
	// OLD ASYNC TRANSATION LOG
	RawData *log_rows;

	void do_init(DTCJobOperation *job)
	{
		memset(this, 0, sizeof(CacheTransaction));
		current_task = job;
	}

	void Free(void)
	{
		log_rows = NULL;
		log_type = 0;

		key = NULL;
		cache_transaction_node = Node::Empty();
		node_status = 0;
		key_dirty = 0;
		old_rows = 0;
		node_empty = 0;
		lru_update = 0;
	}
};

class BufferProcessAskChain : public JobAskInterface<DTCJobOperation>,
			      private TimerObject,
			      public PurgeNodeProcessor,
			      public CacheTransaction {
	// base members
    protected:
	// cache chain control
	ChainJoint<DTCJobOperation> main_chain;
	// send command to remote dtc for migrate.
	ChainJoint<DTCJobOperation> remote_chain;
	// hblog job output
	ChainJoint<DTCJobOperation> hotbackup_chain;

	BufferProcessAnswerChain cache_reply_;

	// table info
	DTCTableDefinition *table_define_infomation_;
	// cache memory management
	BufferPond cache_;
	DataProcess *data_process_;
	BlockProperties cache_info_;

	DTC_MODE dtc_mode_;
	// full cache
	bool full_mode_;
	bool lossy_mode_;
	// treat empty key as default value, flat bitmap emulation
	bool m_bReplaceEmpty;
	// lru update level
	int lru_update_level_;
	// working mode
	EUpdateMode async_server_;
	EUpdateMode update_mode_;
	EUpdateMode insert_mode_;
	// indicate mem dirty when start with sync dtc
	bool memory_dirty_;
	// server side sorting
	unsigned char insert_order_;

	// cache protection
	// node size limit
	int node_size_limit_;
	// node rows limit
	int node_rows_limit_;
	// empty nodes limit
	int node_empty_limit_;

	// generated error message
	char error_message_[256];

	int max_expire_count_;
	int max_expire_time_;

    protected:
	// stat subsystem
	StatCounter stat_get_count_;
	StatCounter stat_get_hits_;
	StatCounter stat_insert_count_;
	StatCounter stat_insert_hits_;
	StatCounter stat_update_count_;
	StatCounter stat_update_hits_;
	StatCounter stat_delete_count_;
	StatCounter stat_delete_hits_;
	StatCounter stat_purge_count_;

	StatCounter stat_drop_count_;
	StatCounter stat_drop_rows_;
	StatCounter stat_flush_count_;
	StatCounter stat_flush_rows_;
	StatSample stat_incsync_step_;

	StatCounter stat_maxflush_request_;
	StatCounter stat_currentFlush_request_;
	StatCounter stat_oldestdirty_time_;
	StatCounter stat_asyncflush_count_;

	StatCounter stat_expire_count_;
	StatCounter stat_buffer_process_expire_count_;

    protected:
	// async flush members
	FlushReplyNotify flush_reply_;
	TimerList *flush_timer_;
	// current pending node
	volatile int current_pend_flush_request_;
	// pending node limit
	volatile int pend_flush_request_;
	// max speed
	volatile unsigned short max_flush_request_;
	volatile unsigned short marker_interval_;
	volatile int min_dirty_time_;
	volatile int max_dirty_time_;
	// async log writer
	int async_log_;
	// empty node filter.
	EmptyNodeFilter *empty_node_filter_;
	// Hot Backup
	// record update key.
	bool log_hotbackup_key_switch_;
	// record lru change.
	HBFeature *hotbackup_lru_feature_;
	// BlackList
	BlackListUnit *black_list_;
	TimerList *blacklist_timer_;
	// BlackList
	ExpireTime *key_expire;
	TimerList *key_expire_timer_;
	HotBackReplay hotback_reply_;

    private:
	// level 1 processing
	// GET entrance
	BufferResult buffer_get_data(DTCJobOperation &job);
	// GET batch entrance
	BufferResult buffer_batch_get_data(DTCJobOperation &job);
	// GET response, DB --> cache
	BufferResult buffer_replace_result(DTCJobOperation &job);
	// GET response, DB --> client
	BufferResult buffer_get_rb(DTCJobOperation &job);

	// implementation some admin/purge/flush function
	BufferResult buffer_process_admin(DTCJobOperation &job);
	BufferResult buffer_purge_data(DTCJobOperation &job);
	BufferResult buffer_flush_data(DTCJobOperation &job);
	BufferResult buffer_flush_data_before_delete(DTCJobOperation &job);
	int buffer_flush_data_timer(Node &node, unsigned int &affected_count);
	BufferResult buffer_flush_data(Node &node, DTCJobOperation *pstTask,
				       unsigned int &affected_count);

	// sync mode operation, called by reply
	BufferResult buffer_sync_insert_precheck(DTCJobOperation &job);
	BufferResult buffer_sync_insert(DTCJobOperation &job);
	BufferResult buffer_sync_update(DTCJobOperation &job);
	BufferResult buffer_sync_replace(DTCJobOperation &job);
	BufferResult buffer_sync_delete(DTCJobOperation &job);

	// async mode operation, called by entrance
	BufferResult buffer_async_insert(DTCJobOperation &job);
	BufferResult buffer_async_update(DTCJobOperation &job);
	BufferResult buffer_async_replace(DTCJobOperation &job);

	// fullcache mode operation, called by entrance
	BufferResult buffer_nodb_insert(DTCJobOperation &job);
	BufferResult buffer_nodb_update(DTCJobOperation &job);
	BufferResult buffer_nodb_replace(DTCJobOperation &job);
	BufferResult buffer_nodb_delete(DTCJobOperation &job);

	// level 2 operation
	// level 2: INSERT with async compatible, create node & clear empty filter
	BufferResult buffer_insert_row(DTCJobOperation &job, bool async,
				       bool setrows);
	// level 2: UPDATE with async compatible, accept empty node only if EmptyAsDefault
	BufferResult buffer_update_rows(DTCJobOperation &job, bool async,
					bool setrows);
	// level 2: REPLACE with async compatible, don't allow empty node
	BufferResult buffer_replace_rows(DTCJobOperation &job, bool async,
					 bool setrows);
	// level 2: DELETE has no async mode, don't allow empty node
	BufferResult buffer_delete_rows(DTCJobOperation &job);

	// very low level
	// empty node inset default value to cache memory.
	// auto clear empty filter
	BufferResult insert_default_row(DTCJobOperation &job);
	bool insert_empty_node(void);

	// hot back-up
	BufferResult buffer_register_hb(DTCJobOperation &job);
	BufferResult buffer_logout_hb(DTCJobOperation &job);
	BufferResult buffer_get_key_list(DTCJobOperation &job);
	BufferResult buffer_get_update_key(DTCJobOperation &job);
	BufferResult buffer_get_raw_data(DTCJobOperation &job);
	BufferResult buffer_replace_raw_data(DTCJobOperation &job);
	BufferResult buffer_adjust_lru(DTCJobOperation &job);
	BufferResult buffer_verify_hbt(DTCJobOperation &job);
	BufferResult buffer_get_hbt(DTCJobOperation &job);

	//memory tidy
	BufferResult buffer_nodehandlechange(DTCJobOperation &job);

	// column expand related
	BufferResult buffer_check_expand_status(DTCJobOperation &job);
	BufferResult buffer_column_expand(DTCJobOperation &job);
	BufferResult buffer_column_expand_done(DTCJobOperation &job);
	BufferResult buffer_column_expand_key(DTCJobOperation &job);

	//imgrate
	BufferResult buffer_migrate(DTCJobOperation &job);

	// clear cache(only support nodb mode)
	BufferResult buffer_clear_cache(DTCJobOperation &job);

	/* we can still purge clean node if hit ratio is ok */
	BufferResult cache_purgeforhit(DTCJobOperation &job);

	//rows limit
	BufferResult check_allowed_insert(DTCJobOperation &job);

	BufferResult buffer_query_serverinfo(DTCJobOperation &job);

	// master-slave copy
	BufferResult buffer_process_replicate(DTCJobOperation &job);

	// hot back-up log
	int write_hotbackup_log(const char *key, char *pstChunk,
				unsigned int uiNodeSize, int iType);
	int write_hotbackup_log(const char *key, Node &node, int iType);
	int write_hotbackup_log(DTCJobOperation &job, Node &node, int iType);
	int write_lru_hotbackup_log(const char *key);

    public:
	virtual void purge_node_processor(const char *key, Node node);

	//inc flush job stat(created by flush dirty node function)
	void inc_async_flush_stat()
	{
		stat_asyncflush_count_++;
	}

    private:
	virtual void job_ask_procedure(DTCJobOperation *);
	void job_answer_procedure(DTCJobOperation *);
	// flush internal
	virtual void job_timer_procedure(void);

	int flush_next_node(void);
	void delete_tail_time_markers();
	void get_dirty_stat();
	void calculate_flush_speed(int is_flush_timer);
	MARKER_STAMP calculate_current_marker();

	BufferProcessAskChain(const BufferProcessAskChain &robj);
	BufferProcessAskChain &operator=(const BufferProcessAskChain &robj);

    public:
	BufferProcessAskChain(PollerBase *, DTCTableDefinition *,
			      EUpdateMode async);
	~BufferProcessAskChain(void);

	const DTCTableDefinition *table_definition(void) const
	{
		return table_define_infomation_;
	}
	const char *last_error_message(void) const
	{
		return error_message_[0] ? error_message_ : "unknown error";
	}

	void set_limit_node_size(int node_size)
	{
		node_size_limit_ = node_size;
	}

	/* 0 =  no limit */
	void set_limit_node_rows(int rows)
	{
		node_rows_limit_ = rows < 0 ? 0 : rows;
		return;
	}

	/*
		 * 0 = no limit,
		 * 1-999: invalid, use 1000 instead
		 * 1000-1G: max empty node count
		 * >1G: invalid, no limit
		 */
	void set_limit_empty_nodes(int nodes)
	{
		node_empty_limit_ = nodes <= 0 ? 0 :
						 nodes < 1000 ?
						 1000 :
						 nodes > (1 << 30) ? 0 : nodes;
		return;
	}

	void disable_auto_purge(void)
	{
		cache_.disable_try_purge();
	}

	void set_date_expire_alert_time(int time)
	{
		cache_.set_date_expire_alert_time(time);
	}

	int set_buffer_size_and_version(unsigned long cache_size,
					unsigned int cache_version);
	int open_init_buffer(int key_name, int enable_empty_filter,
			     int enable_auto_clean_dirty_buffer);

	int update_mode(void) const
	{
		return update_mode_;
	}
	int enable_no_db_mode(void);
	void enable_lossy_data_source(int v)
	{
		lossy_mode_ = v == 0 ? false : true;
	}
	int disable_lru_update(int);
	int disable_async_log(int);

	//DTC MODE: database in addition.
	BufferResult deal_single_database_addition_ask(DTCJobOperation &job);
	BufferResult deal_batch_database_addition_ask(DTCJobOperation &job);
	BufferResult reply_connector_answer(DTCJobOperation &job);

	//DTC MODE: cache only.
	BufferResult deal_single_cache_only_ask(DTCJobOperation &job);

	//Flush
	BufferResult reply_flush_answer(DTCJobOperation &job);
	BufferResult deal_flush_exeption(DTCJobOperation &job);

	void print_row(const RowValue *r);
	int set_insert_order(int o);
	void set_replace_empty(bool v)
	{
		m_bReplaceEmpty = v;
	}

	// stage relate
	void register_next_chain(JobAskInterface<DTCJobOperation> *p)
	{
		main_chain.register_next_chain(p);
	}
	void bind_dispatcher_remote(JobAskInterface<DTCJobOperation> *p)
	{
		remote_chain.register_next_chain(p);
	}
	void bind_hb_log_dispatcher(JobAskInterface<DTCJobOperation> *p)
	{
		hotbackup_chain.register_next_chain(p);
	}

	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}
	ChainJoint<DTCJobOperation> *get_remote_chain()
	{
		return &remote_chain;
	}
	ChainJoint<DTCJobOperation> *get_hotbackup_chain()
	{
		return &hotbackup_chain;
	}

	// flush api
	void set_flush_parameter(int, int, int, int);
	void set_drop_count(int); // to be remove
	int commit_flush_request(DTCFlushRequest *, DTCJobOperation *);
	void complete_flush_request(DTCFlushRequest *);
	void push_flush_queue(DTCJobOperation *p)
	{
		p->push_reply_dispatcher(&flush_reply_);
		main_chain.indirect_notify(p);
	}
	inline bool is_mem_dirty()
	{
		return memory_dirty_;
	}
	int oldest_dirty_node_alarm();

	// expire
	BufferResult check_and_expire(DTCJobOperation &job);

	friend class TaskPendingList;
	friend class BufferProcessAnswerChain;

    public:
	// transaction implementation
	inline void transaction_begin(DTCJobOperation *job)
	{
		CacheTransaction::do_init(job);
	}
	void transaction_end(void);
	inline int transaction_find_node(DTCJobOperation &job);
	inline void transaction_update_lru(bool async, int type);
	void dispatch_hot_back_task(DTCJobOperation *job)
	{
		job->push_reply_dispatcher(&hotback_reply_);
		hotbackup_chain.job_ask_procedure(job);
	}
};

DTC_END_NAMESPACE

#endif
