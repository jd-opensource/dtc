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
#include <stdio.h>
#include <stdlib.h>

#include "mem_check.h"
#include "multi_request.h"
#include "../task/task_request.h"
#include "task_pkey.h"
#include "../poll/poller_base.h"
#include "../log/log.h"
#include "compiler.h"
#include "key_list.h"
#include "agent/agent_multi_request.h"
#include "agent/agent_client.h"

DTCJobOperation::~DTCJobOperation()
{
	free_packed_key();

	if (agent_multi_req) {
		delete agent_multi_req;
		agent_multi_req = NULL;
	}

	if (recv_buf) {
		free(recv_buf);
		recv_buf = NULL;
	}
}

void DTCJobOperation::Clean()
{
	blacklist_size = 0;
	timestamp = 0;
	barrier_hash = 0;
	expire_time = 0;
	keyList = NULL;
	batch_key = NULL;
	DtcJob::Clean();
	TaskOwnerInfo::Clean();
}

int DTCJobOperation::build_packed_key(void)
{
	if (unlikely(key_format() == 0))
		return build_single_string_key();
	if (likely(key_fields() == 1))
		return build_single_int_key();
	else
		return build_multi_int_key();
}

int DTCJobOperation::build_single_string_key(void)
{
	DTCValue empty("");
	const DTCValue *key = request_key();
	if (key == NULL)
		key = &empty;

	if (key->str.len > field_size(0)) {
		set_error(-EC_KEY_NEEDED, "key verify",
			  "string key size overflow");
		return -1;
	}

	FREE_IF(packedKey);
	packedKey = (char *)MALLOC(key->str.len + 1);
	if (packedKey == NULL) {
		set_error(-EC_SERVER_ERROR, "key verify", "no enough memory");
		return -1;
	}

	int i;
	if (field_type(0) == DField::String) {
		*packedKey = key->str.len;
		for (i = 0; i < key->str.len; i++)
			packedKey[i + 1] = INTERNAL_TO_LOWER(key->str.ptr[i]);
		log4cplus_debug("key type[string]");
	} else {
		*packedKey = key->str.len;
		memcpy(packedKey + 1, key->str.ptr, key->str.len);
		log4cplus_debug("key type[binary]");
	}
	return 0;
}

int DTCJobOperation::build_single_int_key(void)
{
	if (unlikely(key_format() > 8)) {
		FREE_IF(packedKey);
		packedKey = (char *)calloc(1, key_format());
		if (packedKey == NULL) {
			set_error(-EC_SERVER_ERROR, "key verify",
				  "no enough memory");
			return -1;
		}
	} else {
		packedKey = packedKeyBuf;
		memset(packedKey, 0, key_format());
	}

	const DTCValue *key = request_key();
	if (likely(key != NULL)) {
		if (unlikely(TaskPackedKey::store_value(packedKey, key->u64,
							field_type(0),
							field_size(0)) < 0)) {
			set_error(-EC_KEY_NEEDED, "key verify",
				  "int key value overflow");
			return -1;
		}
	} else if (!(key_auto_increment() &&
		     request_code() == DRequest::Insert)) {
		set_error(-EC_KEY_NEEDED, "key verify", "No Key Sepcified");
		return -1;
	}
	return 0;
}

int DTCJobOperation::build_multi_key_values()
{
	uint32_t keymask = 0;
	FREE_IF(multi_key);
	multi_key = (DTCValue *)calloc(sizeof(DTCValue), key_fields());
	if (multi_key == NULL) {
		set_error(-ENOMEM, "key verify", "NO ENOUGH MEMORY");
		return -1;
	}

	if (key != NULL) {
		multi_key[0] = *key;
		keymask |= 1;
	}

	const DTCFieldValue *kop =
		request_code() == DRequest::Insert ||
				request_code() == DRequest::Replace ?
			updateInfo :
			conditionInfo;

	if (kop == NULL) {
		set_error(-EC_KEY_NEEDED, "key verify",
			  "Missing Key Component");
		return -1;
	}
	for (int i = 0; i < kop->num_fields(); i++) {
		const int id = kop->field_id(i);
		/* NOT KEY */
		if (id >= key_fields())
			continue;

		if ((keymask & (1 << id)) != 0) {
			set_error(-EC_BAD_MULTIKEY, "key verify",
				  "duplicate key field");
			return -1;
		}

		keymask |= 1 << id;
		const int vt = kop->field_type(i);
		DTCValue *val = kop->field_value(i);
		// the operation must be EQ/Set, already checked in task_base.cc
		if (field_size(id) < 8 && val->s64 < 0 &&
		    ((field_type(id) == DField::Unsigned &&
		      vt == DField::Signed) ||
		     (field_type(id) == DField::Signed &&
		      vt == DField::Unsigned))) {
			set_error(-EC_KEY_OVERFLOW, "key verify",
				  "int key value overflow");
			return -1;
		}
		multi_key[id] = *val;
	}

	const int ai = auto_increment_field_id();
	if (ai >= 0 && ai < key_fields() && request_code() == DRequest::Insert)
		keymask |= 1U << ai;
	//keymask |= 1U;

	if ((keymask + 1) != (1U << key_fields())) {
		set_error(-EC_KEY_NEEDED, "key verify",
			  "Missing Key Component");
		return -1;
	}
	return 0;
}

int DTCJobOperation::build_multi_int_key(void)
{
	/* Build first key field */
	if (build_single_int_key() < 0)
		return -1;

	log4cplus_debug(
		"DTCJobOperation::build_multi_int_key: flag_multi_key_val: %d",
		flag_multi_key_val());
	if (flag_multi_key_val() == 0) {
		// NOTICE, in batch request, build_packed_key only called by set_batch_cursor
		// which already set the corresponding multi_key member
		if (build_multi_key_values() < 0)
			return -1;
	}

	for (int i = 1; i < key_fields(); i++) {
		if (TaskPackedKey::store_value(packedKey + field_offset(i),
					       multi_key[i].u64, field_type(i),
					       field_size(i)) < 0) {
			set_error(-EC_KEY_OVERFLOW, "key verify",
				  "int key value overflow");
			return -1;
		}
	}
	return 0;
}

int DTCJobOperation::update_packed_key(uint64_t val)
{
	int id = auto_increment_field_id();
	if (id >= 0 && id < key_fields()) {
		if (multi_key)
			multi_key[id].u64 = val;
		if (TaskPackedKey::store_value(packedKey + field_offset(id),
					       val, field_type(id),
					       field_size(id)) < 0) {
			set_error(-EC_KEY_NEEDED, "key verify",
				  "int key value overflow");
			return -1;
		}
	}
	return 0;
}

void DTCJobOperation::calculate_barrier_key(void)
{
	if (packedKey) {
		barrier_hash = TaskPackedKey::calculate_barrier_key(
			table_definition(), packedKey);
	}
}

int DTCJobOperation::get_batch_size(void) const
{
	return batch_key != NULL ? batch_key->total_count() : 0;
}

void DTCJobOperation::dump_update_info(const char *prefix) const
{
	/* for debug */
	return;

	char buff[1024];
	buff[0] = 0;
	char *s = buff;

	const DTCFieldValue *p = request_operation();
	if (!p)
		return;

	for (int i = 0; i < p->num_fields(); i++) {
		//only dump setbits operations
		if (p->field_operation(i) == DField::SetBits) {
			uint64_t f_v = p->field_value(i)->u64;

			const int len = 8;
			unsigned int off = f_v >> 32;
			unsigned int size = off >> 24;
			off &= 0xFFFFFF;
			unsigned int value = f_v & 0xFFFFFFFF;

			if (off >= 8 * len || size == 0)
				break;
			if (size > 32)
				size = 32;
			if (size > 8 * len - off)
				size = 8 * len - off;

			s += snprintf(s, sizeof(buff) - (s - buff), " [%d %d]",
				      off, value);
		}
	}

	if (s != buff)
		log4cplus_debug("[%s]%u%s", prefix, int_key(), buff);

	return;
}

#define ERR_RET(ret_msg, fmt, args...)                                         \
	do {                                                                   \
		set_error(err, "decoder", ret_msg);                            \
		log4cplus_debug(fmt, ##args);                                  \
		return -1;                                                     \
	} while (0)
int DTCJobOperation::prepare_process(void)
{
	int err;

	if (1) {
		/* timeout present */
		int client_timeout = requestInfo.tag_present(1) == 0 ?
					     default_expire_time() :
					     requestInfo.get_expire_time(
						     versionInfo.CTLibIntVer());

		log4cplus_debug("client api set timeout %d ms", client_timeout);
		struct timeval now;
		gettimeofday(&now, NULL);

		responseTimer = (int)(now.tv_sec * 1000000ULL + now.tv_usec);
		expire_time = now.tv_sec * 1000ULL + now.tv_usec / 1000 +
			      client_timeout;
		timestamp = now.tv_sec;
	}

	if (0) {
#if 0
		// internal API didn't call PreparePrcess() !!!
		// move checker to task_api.cc
#endif
	} else if (flag_multi_key_val()) {
		// batch key present
		if (!versionInfo.tag_present(10) ||
		    !versionInfo.tag_present(11) ||
		    !versionInfo.tag_present(12) ||
		    !versionInfo.tag_present(13)) {
			err = -EC_KEY_NEEDED;
			ERR_RET("require key field info",
				"require key field info: %d", err);
		}
		if ((int)(versionInfo.get_tag(10)->u64) != key_fields()) {
			err = -EC_KEY_NEEDED;
			ERR_RET("key field count incorrect",
				"key field count incorrect. request: %d, table: %d",
				(int)(versionInfo.get_tag(10)->u64),
				key_fields());
		}
		if (versionInfo.get_tag(11)->u64 < 1) {
			err = -EC_KEY_NEEDED;
			ERR_RET("require key value", "require key value");
		}
		Array keyTypeList(versionInfo.get_tag(12)->bin);
		Array keyNameList(versionInfo.get_tag(13)->bin);
		uint8_t keyType;
		DTCBinary keyName;
		for (unsigned int i = 0; i < versionInfo.get_tag(10)->u64;
		     i++) {
			if (keyTypeList.Get(keyType) != 0 ||
			    keyNameList.Get(keyName) != 0) {
				err = -EC_KEY_NEEDED;
				ERR_RET("require key field info",
					"require key field info: %d", err);
			}
			int fid = field_id(keyName.ptr);
			if (fid < 0 || fid >= key_fields()) {
				err = -EC_BAD_FIELD_NAME;
				ERR_RET("bad key field name",
					"bad key field name: %s", keyName.ptr);
			}
			int tabKeyType = field_type(fid);
			if (keyType >= DField::TotalType ||
			    !validktype[keyType][tabKeyType]) {
				err = -EC_BAD_KEY_TYPE;
				ERR_RET("key type incorrect",
					"key[%s] type[%d] != table-def[%d]",
					keyName.ptr, keyType,
					field_type(field_id(keyName.ptr)));
			}
		}
	} else if (request_code() == DRequest::TYPE_SYSTEM_COMMAND) {
		// admin requests
		// Migrate命令需要在barrier里排队，所以需要校验是否有key以及计算hash
		if (DRequest::SystemCommand::Migrate ==
		    requestInfo.admin_code()) {
			const DTCFieldValue *condition = request_condition();
			if (condition == 0 || condition->num_fields() <= 0 ||
			    condition->field_value(0) == 0) {
				err = -EC_KEY_NEEDED;
				ERR_RET("migrate commond",
					"need set migrate key");
			} else if (condition->num_fields() > 1) {
				err = -EC_SERVER_ERROR;
				ERR_RET("migrate commond",
					"not support bulk migrate now");
			}
			const DTCValue *key = condition->field_value(0);

			if (key->str.len > 8) {
				if (packedKey != packedKeyBuf &&
				    packedKey != 0) {
					free(packedKey);
					packedKey = NULL;
				}
				packedKey = (char *)calloc(1, key->str.len);
				if (packedKey == NULL) {
					set_error(-EC_SERVER_ERROR,
						  "key verify",
						  "no enough memory");
					return -1;
				}
			} else {
				packedKey = packedKeyBuf;
				memset(packedKey, 0, key_format());
			}

			if (packedKey == NULL) {
				set_error(-EC_SERVER_ERROR, "key verify",
					  "no enough memory");
				return -1;
			}
			memcpy(packedKey, key->bin.ptr, key->bin.len);

			calculate_barrier_key();
		}
	} else {
		// single key request
		err = build_packed_key();
		if (err < 0)
			return err;

		calculate_barrier_key();
	}

	return 0;
}

/* for agent request */
int DTCJobOperation::decode_agent_request()
{
	if (agent_multi_req) {
		delete agent_multi_req;
		agent_multi_req = NULL;
	}

	agent_multi_req = new AgentMultiRequest(this);
	if (agent_multi_req == NULL) {
		log4cplus_error("no mem new AgentMultiRequest");
		return -1;
	}

	pass_recved_result_to_agent_muti_req();

	if (agent_multi_req->decode_agent_request() < 0) {
		log4cplus_error("agent multi request decode error");
		return -1;
	}

	return 0;
}

void DTCJobOperation::pass_recved_result_to_agent_muti_req()
{
	agent_multi_req->save_recved_result(recv_buf, recv_len, recv_packet_cnt,
					    packet_version);
	recv_buf = NULL;
	recv_len = recv_packet_cnt = 0;
	packet_version = 0;
}

bool DTCJobOperation::is_agent_request_completed()
{
	return agent_multi_req->is_completed();
}

void DTCJobOperation::done_one_agent_sub_request()
{
	AgentMultiRequest *ownerReq = OwnerInfo<AgentMultiRequest>();
	if (ownerReq)
		ownerReq->complete_task(owner_index());
	else
		delete this;
}

void DTCJobOperation::link_to_owner_client(ListObject<AgentMultiRequest> &head)
{
	if (owner_client_ && agent_multi_req)
		agent_multi_req->ListAdd(head);
}

void DTCJobOperation::set_owner_client(ClientAgent *client)
{
	owner_client_ = client;
}

ClientAgent *DTCJobOperation::owner_client()
{
	return owner_client_;
}

void DTCJobOperation::clear_owner_client()
{
	owner_client_ = NULL;
}

int DTCJobOperation::agent_sub_task_count()
{
	return agent_multi_req->packet_count();
}

bool DTCJobOperation::is_curr_sub_task_processed(int index)
{
	return agent_multi_req->is_curr_task_processed(index);
}

DTCJobOperation *DTCJobOperation::curr_agent_sub_task(int index)
{
	return agent_multi_req->curr_task(index);
}

void DTCJobOperation::copy_reply_for_agent_sub_task()
{
	agent_multi_req->copy_reply_for_sub_task();
}
