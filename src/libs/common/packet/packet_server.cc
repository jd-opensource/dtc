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

#include "version.h"
#include "packet.h"
#include "../table/table_def.h"
#include "../decode/decode.h"
#include "../task/task_request.h"

#include "../log/log.h"

/* not yet pollized*/
int Packet::encode_detect(const DTCTableDefinition *tdef, int sn)
{
	PacketHeader header;

	header.version = 1;
	header.scts = 8;
	header.flags = DRequest::Flag::KeepAlive;
	header.cmd = DRequest::Get;

	DTCVersionInfo vi;
	// tablename & hash
	vi.set_table_name(tdef->table_name());
	vi.set_table_hash(tdef->table_hash());
	vi.set_serial_nr(sn);
	// app version
	vi.set_tag(5, "dtcd");
	// lib version
	vi.set_tag(6, "ctlib-v" DTC_VERSION);
	vi.set_tag(9, tdef->field_type(0));

	DTCRequestInfo ri;
	// key
	ri.set_key(DTCValue::Make(0));
	//	ri.set_timeout(30);

	// field set
	char fs[4] = { 1, 0, 0, char(0xFF) };

	/* calculate version info */
	header.len[DRequest::Section::VersionInfo] =
		encoded_bytes_simple_section(vi, DField::None);

	/* no table definition */
	header.len[DRequest::Section::table_definition] = 0;

	/* encode request info */
	header.len[DRequest::Section::RequestInfo] =
		encoded_bytes_simple_section(ri, tdef->key_type());

	/* no result info */
	header.len[DRequest::Section::ResultInfo] = 0;

	/* encode update info */
	header.len[DRequest::Section::UpdateInfo] = 0;

	/* encode condition info */
	header.len[DRequest::Section::ConditionInfo] = 0;

	/* full set */
	header.len[DRequest::Section::FieldSet] = 4;

	/* no result set */
	header.len[DRequest::Section::DTCResultSet] = 0;

	bytes = encode_header(header);
	const int len = bytes;

	/* exist and large enough, use. else free and malloc */
	int total_len = sizeof(BufferChain) + sizeof(struct iovec) + len;
	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	} else if (buf &&
		   buf->totalBytes < (int)(total_len - sizeof(BufferChain))) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	}

	/* usedBtytes never used for Packet's buf */
	buf->nextBuffer = NULL;
	v = (struct iovec *)buf->data;
	nv = 1;
	char *p = buf->data + sizeof(struct iovec);
	v->iov_base = p;
	v->iov_len = len;

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);
	p = encode_simple_section(p, vi, DField::None);
	p = encode_simple_section(p, ri, tdef->key_type());

	// encode field set
	memcpy(p, fs, 4);
	p += 4;

	if (p - (char *)v->iov_base != len)
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			len);

	return 0;
}

int Packet::encode_reload_config(const DTCTableDefinition *tdef, int sn)
{
	PacketHeader header;

	header.version = 1;
	header.scts = 8;
	header.flags = DRequest::Flag::KeepAlive;
	header.cmd = DRequest::ReloadConfig;

	DTCVersionInfo vi;
	// tablename & hash
	vi.set_table_name(tdef->table_name());
	vi.set_table_hash(tdef->table_hash());
	vi.set_serial_nr(sn);
	// app version
	vi.set_tag(5, "dtcd");
	// lib version
	vi.set_tag(6, "ctlib-v" DTC_VERSION);
	vi.set_tag(9, tdef->field_type(0));

	DTCRequestInfo ri;
	// key
	ri.set_key(DTCValue::Make(0));
	//	ri.set_timeout(30);

	// field set
	char fs[4] = { 1, 0, 0, char(0xFF) };

	/* calculate version info */
	header.len[DRequest::Section::VersionInfo] =
		encoded_bytes_simple_section(vi, DField::None);

	/* no table definition */
	header.len[DRequest::Section::table_definition] = 0;

	/* encode request info */
	header.len[DRequest::Section::RequestInfo] =
		encoded_bytes_simple_section(ri, tdef->key_type());

	/* no result info */
	header.len[DRequest::Section::ResultInfo] = 0;

	/* encode update info */
	header.len[DRequest::Section::UpdateInfo] = 0;

	/* encode condition info */
	header.len[DRequest::Section::ConditionInfo] = 0;

	/* full set */
	header.len[DRequest::Section::FieldSet] = 4;

	/* no result set */
	header.len[DRequest::Section::DTCResultSet] = 0;

	bytes = encode_header(header);
	const int len = bytes;

	/* pool, exist and large enough, use. else free and malloc */
	int total_len = sizeof(BufferChain) + sizeof(struct iovec) + len;
	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	} else if (buf &&
		   buf->totalBytes < (int)(total_len - sizeof(BufferChain))) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	}

	/* usedBtytes never used for Packet's buf */
	buf->nextBuffer = NULL;
	v = (struct iovec *)buf->data;
	nv = 1;
	char *p = buf->data + sizeof(struct iovec);
	v->iov_base = p;
	v->iov_len = len;

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);
	p = encode_simple_section(p, vi, DField::None);
	p = encode_simple_section(p, ri, tdef->key_type());

	// encode field set
	memcpy(p, fs, 4);
	p += 4;

	if (p - (char *)v->iov_base != len)
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			len);

	return 0;
}

static char *EncodeBinary(char *p, const char *src, int len)
{
	if (len)
		memcpy(p, src, len);
	return p + len;
}

static char *EncodeBinary(char *p, const DTCBinary &b)
{
	return EncodeBinary(p, b.ptr, b.len);
}

int Packet::encode_fetch_data(DTCJobOperation &job)
{
	const DTCTableDefinition *tdef = job.table_definition();
	PacketHeader header;

	header.version = 1;
	header.scts = 8;
	header.flags = DRequest::Flag::KeepAlive;
	header.cmd = job.flag_fetch_data() ? DRequest::Get : job.request_code();

	// save & remove limit information
	uint32_t limitStart = job.requestInfo.limit_start();
	uint32_t limitCount = job.requestInfo.limit_count();
	if (job.request_code() != DRequest::Replicate) {
		job.requestInfo.set_limit_start(0);
		job.requestInfo.set_limit_count(0);
	}

	/* calculate version info */
	header.len[DRequest::Section::VersionInfo] =
		encoded_bytes_simple_section(job.versionInfo, DField::None);

	/* no table definition */
	header.len[DRequest::Section::table_definition] = 0;

	/* encode request info */
	header.len[DRequest::Section::RequestInfo] =
		encoded_bytes_simple_section(job.requestInfo, tdef->key_type());

	/* no result info */
	header.len[DRequest::Section::ResultInfo] = 0;

	/* no update info */
	header.len[DRequest::Section::UpdateInfo] = 0;

	/* encode condition info */
	header.len[DRequest::Section::ConditionInfo] = encoded_bytes_multi_key(
		job.multi_key_array(), job.table_definition());

	/* full set */
	header.len[DRequest::Section::FieldSet] =
		tdef->packed_field_set(job.flag_field_set_with_key()).len;

	/* no result set */
	header.len[DRequest::Section::DTCResultSet] = 0;

	bytes = encode_header(header);
	const int len = bytes;

	/* pool, exist and large enough, use. else free and malloc */
	int total_len = sizeof(BufferChain) + sizeof(struct iovec) + len;
	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	} else if (buf &&
		   buf->totalBytes < total_len - (int)sizeof(BufferChain)) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	}

	buf->nextBuffer = NULL;
	v = (struct iovec *)buf->data;
	nv = 1;
	char *p = buf->data + sizeof(struct iovec);
	v->iov_base = p;
	v->iov_len = len;

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);
	p = encode_simple_section(p, job.versionInfo, DField::None);
	p = encode_simple_section(p, job.requestInfo, tdef->key_type());
	// restore limit info
	job.requestInfo.set_limit_start(limitStart);
	job.requestInfo.set_limit_count(limitCount);
	p = encode_multi_key(p, job.multi_key_array(), job.table_definition());
	p = EncodeBinary(p,
			 tdef->packed_field_set(job.flag_field_set_with_key()));

	if (p - (char *)v->iov_base != len)
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			len);

	return 0;
}

int Packet::encode_pass_thru(DtcJob &job)
{
	const DTCTableDefinition *tdef = job.table_definition();
	PacketHeader header;

	header.version = 1;
	header.scts = 8;
	header.flags = DRequest::Flag::KeepAlive;
	header.cmd = job.request_code();

	/* calculate version info */
	header.len[DRequest::Section::VersionInfo] =
		encoded_bytes_simple_section(job.versionInfo, DField::None);

	/* no table definition */
	header.len[DRequest::Section::table_definition] = 0;

	/* encode request info */
	header.len[DRequest::Section::RequestInfo] =
		encoded_bytes_simple_section(job.requestInfo, tdef->key_type());

	/* no result info */
	header.len[DRequest::Section::ResultInfo] = 0;

	/* encode update info */
	header.len[DRequest::Section::UpdateInfo] =
		job.request_operation() ?
			encoded_bytes_field_value(*job.request_operation()) :
			0;

	/* encode condition info */
	header.len[DRequest::Section::ConditionInfo] =
		job.request_condition() ?
			encoded_bytes_field_value(*job.request_condition()) :
			0;

	/* full set */
	header.len[DRequest::Section::FieldSet] =
		job.request_fields() ?
			encoded_bytes_field_set(*job.request_fields()) :
			0;

	/* no result set */
	header.len[DRequest::Section::DTCResultSet] = 0;

	bytes = encode_header(header);
	const int len = bytes;

	/* pool, exist and large enough, use. else free and malloc */
	int total_len = sizeof(BufferChain) + sizeof(struct iovec) + len;
	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	} else if (buf &&
		   buf->totalBytes < total_len - (int)sizeof(BufferChain)) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	}

	buf->nextBuffer = NULL;
	v = (struct iovec *)buf->data;
	nv = 1;
	char *p = buf->data + sizeof(struct iovec);
	v->iov_base = p;
	v->iov_len = len;

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);
	p = encode_simple_section(p, job.versionInfo, DField::None);
	p = encode_simple_section(p, job.requestInfo, tdef->key_type());
	if (job.request_operation())
		p = encode_field_value(p, *job.request_operation());
	if (job.request_condition())
		p = encode_field_value(p, *job.request_condition());
	if (job.request_fields())
		p = encode_field_set(p, *job.request_fields());

	if (p - (char *)v->iov_base != len)
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			len);

	return 0;
}

int Packet::encode_forward_request(DTCJobOperation &job)
{
	if (job.flag_pass_thru())
		return encode_pass_thru(job);
	if (job.flag_fetch_data())
		return encode_fetch_data(job);
	if (job.request_code() == DRequest::Get ||
	    job.request_code() == DRequest::Replicate)
		return encode_fetch_data(job);
	return encode_pass_thru(job);
}

int Packet::encode_result(DtcJob &job, int mtu, uint32_t ts)
{
	const DTCTableDefinition *tdef = job.table_definition();
	// rp指向返回数据集
	ResultPacket *rp =
		job.result_code() >= 0 ? job.get_result_packet() : NULL;
	BufferChain *rb = NULL;
	int nrp = 0, lrp = 0, off = 0;

	if (mtu <= 0) {
		mtu = MAXPACKETSIZE;
	}

	/* rp may exist but no result */
	if (rp && (rp->numRows || rp->totalRows)) {
		//rb指向数据结果集缓冲区起始位置
		rb = rp->bc;
		if (rb)
			rb->Count(nrp, lrp);
		off = 5 - encoded_bytes_length(rp->numRows);
		encode_length(rb->data + off, rp->numRows);
		lrp -= off;
		job.resultInfo.set_total_rows(rp->totalRows);
	} else {
		if (rp && rp->totalRows == 0 && rp->bc) {
			FREE(rp->bc);
			rp->bc = NULL;
		}
		job.resultInfo.set_total_rows(0);
		if (job.result_code() == 0) {
			job.set_error(0, NULL, NULL);
		}
		//任务出现错误的时候，可能结果集里面还有值，此时需要将结果集的buffer释放掉
		else if (job.result_code() < 0) {
			ResultPacket *resultPacket = job.get_result_packet();
			if (resultPacket) {
				if (resultPacket->bc) {
					FREE(resultPacket->bc);
					resultPacket->bc = NULL;
				}
			}
		}
	}
	if (ts) {
		job.resultInfo.set_time_info(ts);
	}
	job.versionInfo.set_serial_nr(job.request_serial());
	job.versionInfo.set_tag(6, "ctlib-v" DTC_VERSION);
	if (job.result_key() == NULL && job.request_key() != NULL)
		job.set_result_key(*job.request_key());

	PacketHeader header;

	header.version = 1;
	header.scts = 8;
	header.flags = DRequest::Flag::KeepAlive | job.flag_multi_key_val();
	/* rp may exist but no result */
	header.cmd = (rp && (rp->numRows || rp->totalRows)) ?
			     DRequest::DTCResultSet :
			     DRequest::result_code;

	/* calculate version info */
	header.len[DRequest::Section::VersionInfo] =
		encoded_bytes_simple_section(job.versionInfo, DField::None);

	/* copy table definition */
	header.len[DRequest::Section::table_definition] =
		job.flag_table_definition() ? tdef->packed_definition().len : 0;

	/* no request info */
	header.len[DRequest::Section::RequestInfo] = 0;

	/* calculate result info */
	header.len[DRequest::Section::ResultInfo] =
		encoded_bytes_simple_section(job.resultInfo,
					     tdef->field_type(0));

	/* no update info */
	header.len[DRequest::Section::UpdateInfo] = 0;

	/* no condition info */
	header.len[DRequest::Section::ConditionInfo] = 0;

	/* no field set */
	header.len[DRequest::Section::FieldSet] = 0;

	/* copy result set */
	header.len[DRequest::Section::DTCResultSet] = lrp;

	bytes = encode_header(header);
	if (bytes > mtu) {
		/* clear result set */
		nrp = 0;
		lrp = 0;
		rb = NULL;
		rp = NULL;
		/* set message size error */
		job.set_error(
			-EMSGSIZE, "encode_result",
			"encoded result exceed the maximum network packet size");
		/* re-encode resultinfo */
		header.len[DRequest::Section::ResultInfo] =
			encoded_bytes_simple_section(job.resultInfo,
						     tdef->field_type(0));
		header.cmd = DRequest::result_code;
		header.len[DRequest::Section::DTCResultSet] = 0;
		/* FIXME: only work in LITTLE ENDIAN machine */
		bytes = encode_header(header);
	}

	//non-result packet len
	const int len = bytes - lrp;

	/* pool, exist and large enough, use. else free and malloc */
	int total_len =
		sizeof(BufferChain) + sizeof(struct iovec) * (nrp + 1) + len;
	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	} else if (buf &&
		   buf->totalBytes < total_len - (int)sizeof(BufferChain)) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(total_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = total_len - sizeof(BufferChain);
	}

	//发送实际数据集
	buf->nextBuffer = nrp ? rb : NULL;
	v = (struct iovec *)buf->data;
	char *p = buf->data + sizeof(struct iovec) * (nrp + 1);
	v->iov_base = p;
	v->iov_len = len;
	nv = nrp + 1;

	for (int i = 1; i <= nrp; i++, rb = rb->nextBuffer) {
		v[i].iov_base = rb->data + off;
		v[i].iov_len = rb->usedBytes - off;
		off = 0;
	}

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);
	p = encode_simple_section(p, job.versionInfo, DField::None);
	if (job.flag_table_definition())
		p = EncodeBinary(p, tdef->packed_definition());
	p = encode_simple_section(p, job.resultInfo, tdef->field_type(0));

	if (p - (char *)v->iov_base != len)
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			len);

	return 0;
}

int Packet::encode_result(DTCJobOperation &job, int mtu)
{
	return encode_result((DtcJob &)job, mtu, job.Timestamp());
}

void Packet::free_result_buff()
{
	BufferChain *resbuff = buf->nextBuffer;
	buf->nextBuffer = NULL;

	while (resbuff) {
		char *p = (char *)resbuff;
		resbuff = resbuff->nextBuffer;
		FREE(p);
	}
}

int Packet::Bytes(void)
{
	int sendbytes = 0;
	for (int i = 0; i < nv; i++) {
		sendbytes += v[i].iov_len;
	}
	return sendbytes;
}
