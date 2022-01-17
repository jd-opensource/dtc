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
#ifndef __H_DTC_REQUEST_REAL_H__
#define __H_DTC_REQUEST_REAL_H__

#include "../request/request_base_all.h"
#include "task_base.h"
#include "stop_watch.h"
#include "hotback_task.h"
class DecoderBase;
class MultiRequest;
class NCKeyValueList;
class NCRequest;
class AgentMultiRequest;
class ClientAgent;

class TaskOwnerInfo {
    private:
	const struct sockaddr *clientaddr;
	void *volatile ownerInfo;
	int ownerIndex;

    public:
	TaskOwnerInfo(void) : clientaddr(NULL), ownerInfo(NULL), ownerIndex(0)
	{
	}
	virtual ~TaskOwnerInfo(void)
	{
	}

	void set_owner_info(void *info, int idx, const struct sockaddr *addr)
	{
		ownerInfo = info;
		ownerIndex = idx;
		clientaddr = addr;
	}

	inline void Clean()
	{
		ownerInfo = NULL;
	}

	void clear_owner_info(void)
	{
		ownerInfo = NULL;
	}

	template <typename T> T *OwnerInfo(void) const
	{
		return (T *)ownerInfo;
	}

	const struct sockaddr *OwnerAddress(void) const
	{
		return clientaddr;
	}
	int owner_index(void) const
	{
		return ownerIndex;
	}
};

class DTCJobOperation : public DtcJob,
			public TaskReplyList<DTCJobOperation, 10>,
			public TaskOwnerInfo {
    public:
	DTCJobOperation(DTCTableDefinition *t = NULL)
		: DtcJob(t, TaskRoleServer), blacklist_size(0), timestamp(0),
		  barrier_hash(0), packedKey(NULL), expire_time(0),
		  multi_key(NULL), keyList(NULL), batch_key(NULL),
		  agent_multi_req(NULL), owner_client_(NULL), recv_buf(NULL),
		  recv_len(0), recv_packet_cnt(0), resource_id(0),
		  packet_version(0), resource_owner(NULL), resource_seq(0){};

	virtual ~DTCJobOperation();

	inline DTCJobOperation(const DTCJobOperation &rq)
	{
		DTCJobOperation();
		Copy(rq);
	}

	int Copy(const DTCJobOperation &rq);
	int Copy(const DTCJobOperation &rq, const DTCValue *newkey);
	int Copy(const RowValue &);
	int Copy(NCRequest &, const DTCValue *);

    public:
	void Clean();
	// msecond: absolute ms time
	uint64_t default_expire_time(void)
	{
		return 5000 /*default:5000ms*/;
	}
	const uint64_t get_expire_time(void) const
	{
		return expire_time;
	}

	int is_expired(uint64_t now) const
	{
		// client gone, always expired
		if (OwnerInfo<void>() == NULL)
			return 1;
		// flush cmd never time out
		if (requestType == TaskTypeCommit)
			return 0;
		return expire_time <= now;
	}
	uint32_t Timestamp(void) const
	{
		return timestamp;
	}
	void renew_timestamp(void)
	{
		timestamp = time(NULL);
	}

	const char *packed_key(void) const
	{
		return packedKey;
	}
	unsigned long barrier_key(void) const
	{
		return barrier_hash;
	}
	DTCValue *multi_key_array(void)
	{
		return multi_key;
	}

    public:
	int build_packed_key(void);
	void calculate_barrier_key(void);

	int prepare_process(void);
	int update_packed_key(uint64_t);
	void push_black_list_size(const unsigned size)
	{
		blacklist_size = size;
	}
	unsigned pop_black_list_size(void)
	{
		register unsigned ret = blacklist_size;
		blacklist_size = 0;
		return ret;
	}

	void dump_update_info(const char *prefix) const;
	void update_key(RowValue &r)
	{
		if (multi_key)
			r.copy_value(multi_key, 0, key_fields());
		else
			r[0] = *request_key();
	}
	void update_key(RowValue *r)
	{
		if (r)
			update_key(*r);
	}
	int update_row(RowValue &row)
	{
		row.update_timestamp(timestamp,
				     requestCode != DRequest::Update ||
					     (updateInfo &&
					      updateInfo->has_type_commit()));
		return DtcJob::update_row(row);
	}

	void set_remote_addr(const char *addr)
	{
		strncpy(remoteAddr, addr, sizeof(remoteAddr));
		remoteAddr[sizeof(remoteAddr) - 1] = 0;
	}

	const char *remote_addr()
	{
		return remoteAddr;
	}
	HotBackTask &get_hot_back_task()
	{
		return hotbacktask;
	}

    private:
	/* following filed should be clean:
	 * blacklist_size
	 * timestamp
	 * barrier_hash
	 * expire_time
	 *
	 * */
	/* 加入黑名单的大小 */
	unsigned blacklist_size;
	uint32_t timestamp;

	unsigned long barrier_hash;
	char *packedKey;
	char packedKeyBuf[8];
	uint64_t expire_time; /* ms */ /* derived from packet */
	DTCValue *multi_key;
	char remoteAddr[32];

    private:
	int build_multi_key_values(void);
	int build_single_string_key(void);
	int build_single_int_key(void);
	int build_multi_int_key(void);

	void free_packed_key(void)
	{
		if (packedKey && packedKey != packedKeyBuf)
			FREE_CLEAR(packedKey);
		FREE_IF(multi_key);
	}
	/* for batch request*/
    private:
	const NCKeyValueList *keyList;
	/* need clean when job begin in use(deleted when batch request finished) */
	MultiRequest *batch_key;

	/* for agent request */
    private:
	AgentMultiRequest *agent_multi_req;
	ClientAgent *owner_client_;
	char *recv_buf;
	int recv_len;
	int recv_packet_cnt;
	uint8_t packet_version;

    public:
	unsigned int key_val_count() const
	{
		return versionInfo.get_tag(11) ? versionInfo.get_tag(11)->s64 :
						 1;
	}
	const Array *key_type_list() const
	{
		return (Array *)&(versionInfo.get_tag(12)->bin);
	}
	const Array *key_name_list() const
	{
		return (Array *)&(versionInfo.get_tag(13)->bin);
	}
	const Array *key_val_list() const
	{
		return (Array *)&(requestInfo.key()->bin);
	}
	const NCKeyValueList *internal_key_val_list() const
	{
		return keyList;
	}

	int is_batch_request(void) const
	{
		return batch_key != NULL;
	}
	int get_batch_size(void) const;
	void set_batch_key_list(MultiRequest *bk)
	{
		batch_key = bk;
	}
	MultiRequest *get_batch_key_list(void)
	{
		return batch_key;
	}
	int set_batch_cursor(int i);
	void done_batch_cursor(int i);

    public:
	/* for agent request */
	void set_owner_client(ClientAgent *client);
	ClientAgent *owner_client();
	void clear_owner_client();

	int decode_agent_request();
	inline void save_recved_result(char *buff, int len, int pktcnt,
				       uint8_t pktver)
	{
		recv_buf = buff;
		recv_len = len;
		recv_packet_cnt = pktcnt;
		packet_version = pktver;
	}
	bool is_agent_request_completed();
	void done_one_agent_sub_request();

	void link_to_owner_client(ListObject<AgentMultiRequest> &head);

	int agent_sub_task_count();
	void copy_reply_for_agent_sub_task();
	DTCJobOperation *curr_agent_sub_task(int index);

	bool is_curr_sub_task_processed(int index);

    private:
	void pass_recved_result_to_agent_muti_req();

    public: // timing
	stopwatch_usec_t responseTimer;
	void response_timer_start(void)
	{
		responseTimer.start();
	}
	unsigned int resource_id;
	DecoderBase *resource_owner;
	uint32_t resource_seq;

    private:
	/*use as async hotback job*/
	HotBackTask hotbacktask;
};

#endif
