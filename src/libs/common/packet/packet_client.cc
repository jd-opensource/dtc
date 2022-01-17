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
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <new>

#include "value.h"
#include "section.h"
#include "protocol.h"
#include "version.h"
#include "packet.h"
#include "../../libs/dep/dtcint.h"
#include "../table/table_def.h"
#include "../decode/decode.h"

#include "../log/log.h"

template <class T>
int Templateencode_request(NCRequest &rq, const DTCValue *kptr, T *tgt)
{
	NCServer *sv = rq.server;
	int key_type = rq.keytype_;
	const char *tab_name = rq.tablename_;

	const char *accessKey = sv->access_token_.c_str();

	DTC_HEADER_V1 header;

	header.version = 1;
	header.scts = 8;
	header.flags = rq.table_definition_ ?
			       DRequest::Flag::KeepAlive :
			       DRequest::Flag::KeepAlive +
				       DRequest::Flag::NeedTableDefinition;
	header.flags |= (rq.flags &
			 (DRequest::Flag::no_cache | DRequest::Flag::NoResult |
			  DRequest::Flag::no_next_server |
			  DRequest::Flag::MultiKeyValue));
	header.cmd = rq.cmd;

	DTCVersionInfo vi;
	// tablename & hash
	vi.set_table_name(tab_name);
	if (rq.table_definition_)
		vi.set_table_hash(rq.table_definition_->table_hash());
	vi.set_serial_nr(sv->get_next_serialnr());
	// app version
	if (sv->appname_)
		vi.set_tag(5, sv->appname_);
	// lib version
	vi.set_tag(6, "ctlib-v" DTC_VERSION);
	vi.set_tag(9, key_type);

	// hot backup id
	vi.set_hot_backup_id(rq.hotbackup_id);

	// hot backup timestamp
	vi.set_master_hb_timestamp(rq.master_hotbackup_timestamp_);
	vi.set_slave_hb_timestamp(rq.slave_hotbackup_timestamp_);
	if (sv->table_definition_ && rq.adminCode != 0)
		vi.set_data_table_hash(sv->table_definition_->table_hash());

	// accessKey
	vi.set_access_key(accessKey);

	Array kt(0, NULL);
	Array kn(0, NULL);
	Array kv(0, NULL);
	int isbatch = 0;
	if (rq.flags & DRequest::Flag::MultiKeyValue) {
		if (sv->simple_batch_key() &&
		    rq.key_value_list_.KeyCount() == 1) {
			/* single field single key batch, convert to normal */
			kptr = rq.key_value_list_.val;
			header.flags &= ~DRequest::Flag::MultiKeyValue;
		} else {
			isbatch = 1;
		}
	}

	if (isbatch) {
		int keyFieldCnt = rq.key_value_list_.get_key_fields_count();
		int keyCount = rq.key_value_list_.KeyCount();
		int i, j;
		vi.set_tag(10, keyFieldCnt);
		vi.set_tag(11, keyCount);
		// key type
		kt.ptr = (char *)MALLOC(sizeof(uint8_t) * keyFieldCnt);
		if (kt.ptr == NULL)
			throw std::bad_alloc();
		for (i = 0; i < keyFieldCnt; i++)
			kt.Add((uint8_t)(rq.key_value_list_.get_key_type(i)));
		vi.set_tag(12, DTCValue::Make(kt.ptr, kt.len));
		// key name
		kn.ptr = (char *)MALLOC((256 + sizeof(uint32_t)) * keyFieldCnt);
		if (kn.ptr == NULL)
			throw std::bad_alloc();
		for (i = 0; i < keyFieldCnt; i++)
			kn.Add(rq.key_value_list_.get_key_name(i));
		vi.set_tag(13, DTCValue::Make(kn.ptr, kn.len));
		// key value
		unsigned int buf_size = 0;
		for (j = 0; j < keyCount; j++) {
			for (i = 0; i < keyFieldCnt; i++) {
				DTCValue &v = rq.key_value_list_(j, i);
				switch (rq.key_value_list_.get_key_type(i)) {
				case DField::Signed:
				case DField::Unsigned:
					if (buf_size <
					    kv.len + sizeof(uint64_t)) {
						if (REALLOC(kv.ptr,
							    buf_size + 256) ==
						    NULL)
							throw std::bad_alloc();
						buf_size += 256;
					}
					kv.Add(v.u64);
					break;
				case DField::String:
				case DField::Binary:
					if (buf_size <
					    (unsigned int)kv.len +
						    sizeof(uint32_t) +
						    v.bin.len) {
						if (REALLOC(kv.ptr,
							    buf_size +
								    sizeof(uint32_t) +
								    v.bin.len) ==
						    NULL)
							throw std::bad_alloc();
						buf_size += sizeof(uint32_t) +
							    v.bin.len;
					}
					kv.Add(v.bin.ptr, v.bin.len);
					break;
				default:
					break;
				}
			}
		}
	}

	DTCRequestInfo ri;
	// key
	if (isbatch)
		ri.set_key(DTCValue::Make(kv.ptr, kv.len));
	else if (kptr)
		ri.set_key(*kptr);
	// cmd
	if (sv->get_timeout()) {
		ri.set_timeout(sv->get_timeout());
	}
	//limit
	if (rq.limitCount) {
		ri.set_limit_start(rq.limitStart);
		ri.set_limit_count(rq.limitCount);
	}
	if (rq.adminCode > 0) {
		ri.set_admin_code(rq.adminCode);
	}

	/* calculate version info */
	header.len[DRequest::Section::VersionInfo] =
		encoded_bytes_simple_section(vi, DField::None);

	//log4cplus_info("header.len:%d, vi.access_key:%s, vi.access_key().len:%d", header.len[DRequest::Section::VersionInfo], vi.access_key().ptr, vi.access_key().len);

	/* no table definition */
	header.len[DRequest::Section::table_definition] = 0;

	/* calculate rq info */
	header.len[DRequest::Section::RequestInfo] =
		encoded_bytes_simple_section(ri, isbatch ? DField::String :
							   key_type);

	/* no result info */
	header.len[DRequest::Section::ResultInfo] = 0;

	/* copy update info */
	header.len[DRequest::Section::UpdateInfo] =
		encoded_bytes_field_value(rq.ui);

	/* copy condition info */
	header.len[DRequest::Section::ConditionInfo] =
		encoded_bytes_field_value(rq.ci);

	/* full set */
	header.len[DRequest::Section::FieldSet] =
		encoded_bytes_field_set(rq.fs);

	/* no result set */
	header.len[DRequest::Section::DTCResultSet] = 0;

	const int len = Packet::encode_header_v1(header);
	char *p = tgt->allocate_simple(len);

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);
	p = encode_simple_section(p, vi, DField::None);
	p = encode_simple_section(p, ri, isbatch ? DField::String : key_type);
	p = encode_field_value(p, rq.ui);
	p = encode_field_value(p, rq.ci);
	p = encode_field_set(p, rq.fs);

	FREE(kt.ptr);
	FREE(kn.ptr);
	FREE(kv.ptr);

	return 0;
}

char *Packet::allocate_simple(int len)
{
	buf = (BufferChain *)MALLOC(sizeof(BufferChain) + sizeof(struct iovec) +
				    len);
	if (buf == NULL)
		throw std::bad_alloc();

	buf->nextBuffer = NULL;
	/* never use usedBytes here */
	buf->totalBytes = sizeof(struct iovec) + len;
	v = (struct iovec *)buf->data;
	nv = 1;
	char *p = buf->data + sizeof(struct iovec);
	v->iov_base = p;
	v->iov_len = len;
	bytes = len;
	return p;
}

int Packet::encode_request(NCRequest &rq, const DTCValue *kptr)
{
	return Templateencode_request(rq, kptr, this);
}

class SimpleBuffer : public DTCBinary {
    public:
	char *allocate_simple(int size);
};

char *SimpleBuffer::allocate_simple(int size)
{
	len = size;
	ptr = (char *)MALLOC(len);
	if (ptr == NULL)
		throw std::bad_alloc();
	return ptr;
}

int Packet::encode_simple_request(NCRequest &rq, const DTCValue *kptr,
				  char *&ptr, int &len)
{
	SimpleBuffer buf;
	int ret = Templateencode_request(rq, kptr, &buf);
	ptr = buf.ptr;
	len = buf.len;
	return ret;
}
