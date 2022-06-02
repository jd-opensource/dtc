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
#include <fcntl.h>
#include <stdio.h>
#include <sys/socket.h>
#include <new>

#include "version.h"
#include "packet.h"
#include "../table/table_def.h"
#include "../decode/decode.h"
#include "../task/task_request.h"

#include "../log/log.h"
//#include "mysql/field_types.h"

struct MetaSelections{
	const char* p_req_string;
	int i_select_type;
	const char* p_val;
};

enum enum_select_types {
	E_SELECT_NONE,
	E_SELECT_DTC_TABLES,
	E_SELECT_DTC_YAML,
	E_SELECT_TABLE_YAML
};

const MetaSelections meta_selections[] = {
	{"select dtctables" , E_SELECT_DTC_TABLES 	, NULL},
	{"select dtcyaml" 	, E_SELECT_DTC_YAML 	, "/etc/dtc/dtc.yaml"},
	{"select tableyaml" , E_SELECT_TABLE_YAML 	, "/etc/dtc/table.yaml"}
};

enum enum_field_types { MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
			MYSQL_TYPE_SHORT,  MYSQL_TYPE_LONG,
			MYSQL_TYPE_FLOAT,  MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_NULL,   MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_LONGLONG,MYSQL_TYPE_INT24,
			MYSQL_TYPE_DATE,   MYSQL_TYPE_TIME,
			MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
			MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT,
			MYSQL_TYPE_TIMESTAMP2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIME2,
                        MYSQL_TYPE_NEWDECIMAL=246,
			MYSQL_TYPE_ENUM=247,
			MYSQL_TYPE_SET=248,
			MYSQL_TYPE_TINY_BLOB=249,
			MYSQL_TYPE_MEDIUM_BLOB=250,
			MYSQL_TYPE_LONG_BLOB=251,
			MYSQL_TYPE_BLOB=252,
			MYSQL_TYPE_VAR_STRING=253,
			MYSQL_TYPE_STRING=254,
			MYSQL_TYPE_GEOMETRY=255

};

/* not yet pollized*/
int Packet::encode_detect(const DTCTableDefinition *tdef, int sn)
{
	DTC_HEADER_V1 header;

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

	bytes = encode_header_v1(header);
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
	DTC_HEADER_V1 header;

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

	bytes = encode_header_v1(header);
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
	DTC_HEADER_V1 header;

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

	bytes = encode_header_v1(header);
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
	DTC_HEADER_V1 header;

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

	bytes = encode_header_v1(header);
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

	DTC_HEADER_V1 header;

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

	bytes = encode_header_v1(header);
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
		bytes = encode_header_v1(header);
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

void encode_mysql_header(BufferChain *r, int len, uint8_t pkt_num)
{
	//Packet Lenght + Packet Number
	char t[3];
	int_conv_3(t, len);
	memcpy(r->data, t, 3);
	*(r->data + 3) = pkt_num;
}

int encode_my_fileds_info(BufferChain **bc, uint8_t& pkt_num, uint8_t fields_num)
{
	int packet_len = sizeof(BufferChain) + sizeof(MYSQL_HEADER_SIZE) +
			 sizeof(fields_num);

	*bc = (BufferChain *)MALLOC(packet_len);
	BufferChain *r = *bc;
	if (r == NULL) {
		return -ENOMEM;
	}
	r->totalBytes = packet_len - sizeof(BufferChain);
	encode_mysql_header(r, 1, pkt_num++);
	*(r->data + sizeof(MYSQL_HEADER_SIZE)) = fields_num;
	r->usedBytes = 5;
	r->nextBuffer = NULL;

	return 0;
}

struct my_result_set_field {
	std::string catalog;
	std::string database;
	std::string table;
	std::string original_table;
	std::string name;
	std::string original_name;
	uint16_t charset_number;
	uint32_t length;
	uchar type;
	uint16_t flags;
	uchar decimals;
	uint16_t reverse;
};

int encode_set_field(char *buf, my_result_set_field *sf)
{
	int len = 0;
	char *p = buf;

	len = sf->catalog.length();
	*p++ = (uint8_t)len;
	if (len > 0) {
		memcpy(p, sf->catalog.c_str(), len);
		p += len;
	}

	len = sf->database.length();
	*p++ = (uint8_t)len;
	if (len > 0) {
		memcpy(p, sf->database.c_str(), len);
		p += len;
	}

	len = sf->table.length();
	*p++ = (uint8_t)len;
	if (len > 0) {
		memcpy(p, sf->table.c_str(), len);
		p += len;
	}

	len = sf->original_table.length();
	*p++ = (uint8_t)len;
	if (len > 0) {
		memcpy(p, sf->original_table.c_str(), len);
		p += len;
	}

	len = sf->name.length();
	*p++ = (uint8_t)len;
	if (len > 0) {
		memcpy(p, sf->name.c_str(), len);
		p += len;
	}

	len = sf->original_name.length();
	*p++ = (uint8_t)len;
	if (len > 0) {
		memcpy(p, sf->original_name.c_str(), len);
		p += len;
	}

	//charset number
	*p++ = 0x0c;
	int2store_big_endian(p, sf->charset_number);
	p += sizeof(sf->charset_number);

	//length
	int4store_big_endian(p, sf->length);
	p += sizeof(sf->length);

	//type
	*p = sf->type;
	p += sizeof(sf->type);

	//flags
	int2store_big_endian(p, sf->flags);
	p += sizeof(sf->flags);

	//decimals
	*p = sf->decimals;
	p += sizeof(sf->decimals);

	//reverse
	*p = sf->reverse;
	p += sizeof(sf->reverse);

	return p - buf;
}

int calc_field_def(my_result_set_field *sf)
{
	int len = 0;

	len++;
	len += sf->catalog.length();

	len++;
	len += sf->database.length();

	len++;
	len += sf->table.length();

	len++;
	len += sf->original_table.length();

	len++;
	len += sf->name.length();

	len++;
	len += sf->original_name.length();

	//charset number
	len++; //0x0c
	len += sizeof(sf->charset_number);

	//length
	len += sizeof(sf->length);

	//type
	len += sizeof(sf->type);

	//flag
	len += sizeof(sf->flags);

	//decimals
	len += sizeof(sf->decimals);

	//reverse
	len += sizeof(sf->reverse);

	log4cplus_debug("sf len:%d", len);

	return len;
}

int build_field_type(int type)
{
	switch (type) {
	case DField::Signed:
		return MYSQL_TYPE_LONG;
	case DField::Unsigned:
		return MYSQL_TYPE_LONG;
	case DField::Float:
		return MYSQL_TYPE_FLOAT;
	case DField::String:
		return MYSQL_TYPE_VAR_STRING;
	case DField::Binary:
		return MYSQL_TYPE_VAR_STRING;
	}
}

uint16_t build_charset(int type)
{
	switch (type) {
	case MYSQL_TYPE_VAR_STRING:
		return 0xff; //utf8mb4
		break;
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_FLOAT:
	default:
		return 0x3f; //binary COLLATE binary
	}
}

uint16_t build_length(int type)
{
	switch (type) {
	case MYSQL_TYPE_VAR_STRING:
		return 200;
		break;
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_FLOAT:
	default:
		return 11;
	}
}

BufferChain *encode_field_def(DtcJob *job, BufferChain *bc, uint8_t& pkt_num)
{
	const DTCTableDefinition *tdef = job->table_definition();
	BufferChain *nbc = bc;
	BufferChain *r = NULL;
	std::vector<std::string> need = job->mr.get_need_array();

	for (int i = 0; i < need.size(); i++) {
		my_result_set_field sf;
		sf.type = build_field_type(
			job->field_type(job->field_id(need[i].c_str())));
		sf.charset_number = build_charset(sf.type);
		sf.database = "dtc";
		sf.length = build_length(sf.type);
		sf.catalog = "def";
		sf.table = job->table_name();
		sf.original_table = job->table_name();
		sf.name = need[i];
		sf.original_name = need[i];
		sf.decimals = 0x00;
		sf.flags = 0x0;
		sf.reverse = 0x0000;

		int packet_len = sizeof(BufferChain) + calc_field_def(&sf) +
				 sizeof(MYSQL_HEADER_SIZE);
		r = (BufferChain *)MALLOC(packet_len);
		if (r == NULL) {
			return NULL;
		}
		memset(r, 0, packet_len);
		r->totalBytes = packet_len - sizeof(BufferChain);

		int set_len = encode_set_field(
			r->data + sizeof(MYSQL_HEADER_SIZE), &sf);
		log4cplus_debug("set_len:%d", set_len);
		r->usedBytes = sizeof(MYSQL_HEADER_SIZE) + set_len;
		r->nextBuffer = NULL;
		encode_mysql_header(r, set_len, pkt_num++);

		nbc->nextBuffer = r;
		nbc = nbc->nextBuffer;
	}
	return nbc;
}

#pragma pack(1)
struct my_result_set_eof {
	uchar eof;
	uint16_t warning;
	uint16_t server_status;
	uint16_t reverse;
};
#pragma pack()

BufferChain *encode_eof(BufferChain *bc, uint8_t pkt_nr)
{
	BufferChain *nbc = bc;
	my_result_set_eof eof;
	eof.eof = 0xfe;
	eof.warning = 0;
	eof.server_status = 0x0022;
	eof.reverse = 0;

	int packet_len =
		sizeof(BufferChain) + sizeof(eof) + sizeof(MYSQL_HEADER_SIZE);
	BufferChain *r = (BufferChain *)MALLOC(packet_len);
	if (r == NULL) {
		return NULL;
	}
	r->totalBytes = packet_len - sizeof(BufferChain);

	memcpy(r->data + sizeof(MYSQL_HEADER_SIZE), &eof, sizeof(eof));
	r->usedBytes = sizeof(MYSQL_HEADER_SIZE) + sizeof(eof);
	r->nextBuffer = NULL;
	encode_mysql_header(r, sizeof(eof), pkt_nr);

	nbc->nextBuffer = r;
	nbc = nbc->nextBuffer;

	return nbc;
}

BufferChain *encode_row_data(DtcJob *job, BufferChain *bc, uint8_t &pkt_nr)
{
	ResultSet *pstResultSet = job->result;
	int count = 0;
	BufferChain *nbc = bc;
	std::vector<std::string> result_field = job->mr.get_need_array();
	const DTCTableDefinition *tdef = job->table_definition();

	if (pstResultSet == NULL)
		return NULL;

	for (int i = 0; i < pstResultSet->total_rows(); i++) {
		char buf[32] = { 0 };
		RowValue *pstRow = pstResultSet->_fetch_row();
		if (pstRow == NULL) {
			log4cplus_info("%s!", "call FetchRow func error");
			continue;
		}

		//calc current row len
		int row_len = 0;
		for (int j = 0; j < result_field.size(); j++) {
			int id = tdef->field_id(result_field[j].c_str());
			DTCValue* v;
			if (0 == id) {
				v = job->request_key();
			} else {
				v = pstRow->field_value(id);
			}
			int field_type = pstRow->field_type(id);
			switch (field_type) {
			case DField::Signed: {
				row_len++; //first byte for result len
				snprintf(buf, sizeof(buf), "%lld",
					 (long long)v->s64);
				row_len += strlen(buf);
				break;
			}
			case DField::Unsigned: {
				row_len++; //first byte for result len
				snprintf(buf, sizeof(buf), "%llu",
					 (unsigned long long)v->u64);
				row_len += strlen(buf);
				break;
			}
			case DField::Float: {
				row_len++; //first byte for result len
				snprintf(buf, sizeof(buf), "%f", v->flt);
				row_len += strlen(buf);
				break;
			}
			case DField::String:
			case DField::Binary: {
				row_len++;
				row_len += v->str.len;
				break;
			}
			default:
				break;
			}
		}

		//alloc new buffer to store row data.
		int packet_len = sizeof(BufferChain) +
				 sizeof(MYSQL_HEADER_SIZE) + row_len;
		BufferChain *nbuff = (BufferChain *)MALLOC(packet_len);
		if (nbuff == NULL) {
			return NULL;
		}
		nbuff->totalBytes = packet_len - sizeof(BufferChain);
		nbuff->usedBytes = sizeof(MYSQL_HEADER_SIZE) + row_len;
		nbuff->nextBuffer = NULL;

		char *r = nbuff->data;
		encode_mysql_header(nbuff, row_len, pkt_nr++);
		int offset = 0;
		offset += sizeof(MYSQL_HEADER_SIZE);

		//copy fields content
		for (int j = 0; j < result_field.size(); j++) {
			int id = tdef->field_id(result_field[j].c_str());
			DTCValue* v;
			if (0 == id) {
				v = job->request_key();
			} else {
				v = pstRow->field_value(id);
			}
			int field_type = pstRow->field_type(id);
			int num_len = 0;
			switch (field_type) {
			case DField::Signed: {
				snprintf(buf, sizeof(buf), "%lld",
					 (long long)v->s64);
				num_len = strlen(buf);
				*(r + offset) = (uint8_t)num_len;
				offset++;
				memcpy(r + offset, buf, num_len);
				offset += num_len;
				break;
			}
			case DField::Unsigned: {
				snprintf(buf, sizeof(buf), "%llu",
					 (unsigned long long)v->u64);
				num_len = strlen(buf);
				*(r + offset) = (uint8_t)strlen(buf);
				offset++;
				memcpy(r + offset, buf, num_len);
				offset += num_len;
				break;
			}
			case DField::Float: {
				snprintf(buf, sizeof(buf), "%f", v->flt);
				num_len = strlen(buf);
				*(r + offset) = (uint8_t)strlen(buf);
				offset++;
				memcpy(r + offset, buf, num_len);
				offset += num_len;
				break;
			}
			case DField::String: {
				*(r + offset) = (uint8_t)v->str.len;
				offset++;
				memcpy(r + offset, v->str.ptr, v->str.len);
				offset += v->str.len;
				break;
			}
			case DField::Binary: {
				*(r + offset) = (uint8_t)v->bin.len;
				offset++;
				memcpy(r + offset, v->bin.ptr, v->bin.len);
				offset += v->bin.len;
				break;
			}
			default:
				break;
			}
		}

		nbc->nextBuffer = nbuff;
		nbc = nbc->nextBuffer;
	}

	return nbc;
}

BufferChain *Packet::encode_mysql_ok(DtcJob *job)
{
	BufferChain *bc = NULL;
	BufferChain *pos = NULL;

	uint8_t buf[7] = {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00};
	int2store_big_endian(buf+ 1, job->resultInfo.affected_rows());

	uint8_t pkt_nr = job->mr.get_pkt_nr();
	pkt_nr++;


	int packet_len = sizeof(BufferChain) + sizeof(MYSQL_HEADER_SIZE) +
		sizeof(buf);

	bc = (BufferChain *)MALLOC(packet_len);
	BufferChain *r = bc;
	if (r == NULL) {
		return -ENOMEM;
	}
	r->totalBytes = packet_len - sizeof(BufferChain);
	encode_mysql_header(r, sizeof(buf), pkt_nr);
	memcpy(r->data + sizeof(MYSQL_HEADER_SIZE), buf, sizeof(buf));
	r->usedBytes = sizeof(buf) + sizeof(MYSQL_HEADER_SIZE);
	r->nextBuffer = NULL;

	return bc;
}

BufferChain *Packet::encode_mysql_protocol(DtcJob *job)
{
	BufferChain *bc = NULL;
	BufferChain *pos = NULL;

	uint8_t pkt_nr = job->mr.get_pkt_nr();
	pkt_nr++;

	int ret = encode_my_fileds_info(&bc, pkt_nr,
					job->mr.get_need_num_fields());
	if (ret < 0)
		return NULL;
	pos = encode_field_def(job, bc, pkt_nr);
	if (!pos)
		return NULL;
	//Different MYSQL Version.
	//pos = encode_eof(pos, ++pkt_nr);
	//if (!pos)
	//	return NULL;
	BufferChain *prow = encode_row_data(job, pos, pkt_nr);
	if (prow) {
		pos = prow;
	}
	pos = encode_eof(pos, pkt_nr);
	if (!pos)
		return NULL;

	return bc;
}

int net_send_ok(int affectedRow)
{
	uint8_t buf[100] = { 0x00, (uint8_t)affectedRow, 0x00, 0x02, 0x00, 0x00,
			     0x00 };
}

int is_desc_tables(DtcJob *job , char*& p_filepath)
{
	std::string sql = job->mr.get_sql();
	if (sql.empty()) {
		sql = std::string(job->mr.raw , job->mr.raw_len);
	}
	
	log4cplus_debug("req sql:%s" , sql.c_str());

	uint32_t ui_size = (sizeof(meta_selections) / sizeof(MetaSelections));
	for (int i = 0; i < ui_size; i++) {
		if (sql == string(meta_selections[i].p_req_string)) {
			p_filepath = meta_selections[i].p_val;
			return meta_selections[i].i_select_type; 
		}
	}

	return E_SELECT_NONE;
}

int Packet::desc_tables_result(DtcJob *job)
{
	log4cplus_debug("desc_tables_result entry.");
	const DTCTableDefinition *tdef = job->table_definition();
	DTC_HEADER_V2 header = { 0 };
	nv = 1;
	int content_len = strlen(tdef->field_name(0));
	int packet_len = sizeof(BufferChain) + sizeof(struct iovec) +
			 sizeof(header) + content_len + 2;

	header.version = 2;
	header.id = job->request_peerid();
	header.packet_len = packet_len;
	header.admin = CMD_KEY_DEFINE;

	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = packet_len - sizeof(BufferChain);
		buf->nextBuffer = NULL;
	} else if (buf &&
		   packet_len - (int)sizeof(BufferChain) > buf->totalBytes) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = packet_len - sizeof(BufferChain);
		buf->nextBuffer = NULL;
	}

	char *p = buf->data + sizeof(struct iovec);
	v = (struct iovec *)buf->data;
	v->iov_base = p;
	v->iov_len = sizeof(header) + content_len + 2;

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);
	*p = (uint8_t)tdef->field_type(0);
	p++;
	*p = (uint8_t)content_len;
	p++;
	memcpy(p, tdef->field_name(0), content_len);

	log4cplus_debug("desc_tables_result leave.");
	return 0;
}

int Packet::yaml_config_result(DtcJob *job , const char* p_filename)
{
	log4cplus_debug("yaml_config_result entry.");
	nv = 1;

	char* p_buf = NULL;
	int i_len = 0;
	int i_ret = load_table(p_filename , p_buf , i_len);
	if (p_buf != NULL) {
		log4cplus_debug("p_filename:%s , buflen:%d" , p_filename , i_len);
	}
    
	if (i_ret != 0) { return -EFAULT; }

	int send_len = sizeof(DTC_HEADER_V2) + i_len;
	int packet_len = sizeof(BufferChain) + sizeof(iovec) + send_len;
	
	DTC_HEADER_V2 header = { 0 };
	header.version = 2;
	header.id = job->request_peerid();
	header.packet_len = send_len;
	header.admin = CMD_KEY_DEFINE;

	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = packet_len - sizeof(BufferChain);
		buf->nextBuffer = NULL;
	} else if (buf &&
		   packet_len - (int)sizeof(BufferChain) > buf->totalBytes) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = packet_len - sizeof(BufferChain);
		buf->nextBuffer = NULL;
	}

	char *p = buf->data + sizeof(struct iovec);
	v = (struct iovec *)buf->data;
	v->iov_base = p;
	v->iov_len = send_len;

	memcpy(p, &header, sizeof(header));
	p += sizeof(header);

	memcpy(p, p_buf, i_len);
	FREE_CLEAR(p_buf);

	log4cplus_debug("yaml_config_result leave.");
	return 0;
};

int Packet::load_table(const char* p_filename, char*& file , int& i_length)
{
	int fd = -1;

	if (!p_filename 
	|| p_filename[0] == '\0' 
	|| (fd = open(p_filename, O_RDONLY)) < 0) {
		log4cplus_error("open config file error");
		return -1;
	}

	lseek(fd, 0L, SEEK_SET);
	i_length = lseek(fd, 0L, SEEK_END);
	lseek(fd, 0L, SEEK_SET);
	// Attention: memory init here ,need release outside
	file = (char *)MALLOC(i_length + 1);
	int readlen = read(fd, file, i_length);
	if (readlen < 0 || readlen == 0)
		return -1;
	file[i_length] = '\0';
	close(fd);
	i_length++; // add finish flag length
	log4cplus_debug("read file to buf, len: %d", i_length);
	return 0;
};

int Packet::encode_result_v2(DtcJob &job, int mtu, uint32_t ts)
{
	log4cplus_debug("encode_result_v2 entry.");
	const DTCTableDefinition *tdef = job.table_definition();

	char* p_file_path = NULL;
	switch (is_desc_tables(&job , p_file_path))
	{
	case E_SELECT_DTC_TABLES:
	    {
			return desc_tables_result(&job);
		}
		break;
	case E_SELECT_DTC_YAML:
		{
			return yaml_config_result(&job , p_file_path);
		}
		break;
	case E_SELECT_TABLE_YAML:
		{
			return yaml_config_result(&job , p_file_path);
		}
		break;
	case E_SELECT_NONE:
	default:
		break;
	}

	// if (is_desc_tables(&job)) {
	// 	return desc_tables_result(&job);
	// }

	// rp指向返回数据集
	ResultPacket *rp =
		job.result_code() >= 0 ? job.get_result_packet() : NULL;
	log4cplus_info("result code:%d" , job.result_code());
	BufferChain *rb = NULL;
	int nrp = 0, lrp = 0, off = 0;
	bool bok = false;
	if (mtu <= 0) {
		mtu = MAXPACKETSIZE;
	}

	/* rp may exist but no result */
	if (rp && (rp->numRows || rp->totalRows)) {
		//rb指向数据结果集缓冲区起始位置
		log4cplus_info("line:%d" ,__LINE__);
		rb = rp->bc;
		if (rb)
			rb->Count(nrp, lrp);
		off = 5 - encoded_bytes_length(rp->numRows);
		encode_length(rb->data + off, rp->numRows);
		lrp -= off;
		job.resultInfo.set_total_rows(rp->totalRows);
	} else {
		log4cplus_info("line:%d" ,__LINE__);
		nrp = 1;
		bok = true;
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
	job.versionInfo.set_serial_nr(job.request_peerid() + 1);

	if (job.result_key() == NULL && job.request_key() != NULL)
		job.set_result_key(*job.request_key());

	//转换内容包
	int err = job.decode_result_set(rb->data + off, lrp);
	if (err) {
		log4cplus_debug("decode result null: %d", err);
	} else {
		log4cplus_debug("decode_result_set success");
	}

	DTC_HEADER_V2 dtc_header = { 0 };
	dtc_header.version = 2;
	dtc_header.id = job.request_peerid();
	log4cplus_info("dtc_header.id:%d , job.request_serial():%d" , dtc_header.id , job.request_peerid());
	dtc_header.packet_len = 0;
	dtc_header.admin = CMD_NOP;

	if(bok == false)
	{
		nrp = 1 /*fields count info*/ +
			job.mr.get_need_array().size() /*fields def*/ + 0 /*eof*/ +
			(job.result ? job.result->total_rows() : 0) /*row data*/ +
			1 /*eof*/;
	}

	log4cplus_info("line:%d" ,__LINE__);
	/* pool, exist and large enough, use. else free and malloc */
	int first_packet_len = sizeof(BufferChain) +
			       sizeof(struct iovec) * (nrp + 1) +
			       sizeof(dtc_header);
	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(first_packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = first_packet_len - sizeof(BufferChain);
		buf->nextBuffer = NULL;
	} else if (buf && first_packet_len - (int)sizeof(BufferChain) >
				  buf->totalBytes) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(first_packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = first_packet_len - sizeof(BufferChain);
		buf->nextBuffer = NULL;
	}
	//设置要发送的第一个包
	char *p = buf->data + sizeof(struct iovec) * (nrp + 1);
	v = (struct iovec *)buf->data;
	v->iov_base = p;
	v->iov_len = sizeof(dtc_header);
	nv = nrp + 1;
	buf->usedBytes = sizeof(struct iovec) * (nrp + 1) + sizeof(dtc_header);

	//修改第一个包的内容
	memcpy(p, &dtc_header, sizeof(dtc_header));
	p += sizeof(dtc_header);
	if (p - (char *)v->iov_base != sizeof(dtc_header))
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			sizeof(dtc_header));

	rb = NULL;
	if(bok == true)
		rb = encode_mysql_ok(&job);
	else
		rb = encode_mysql_protocol(&job);
	if (!rb)
		return -3;

	buf->nextBuffer = rb;
	for (int i = 1; i <= nrp; i++, rb = rb->nextBuffer) {
		v[i].iov_base = rb->data;
		v[i].iov_len = rb->usedBytes;
	}

	log4cplus_debug("encode_result_v2 leave.");
	return 0;
}

int Packet::encode_result(DTCJobOperation &job, int mtu)
{
	if (1 == job.get_pac_version()) {
		return encode_result((DtcJob &)job, mtu, job.Timestamp());
	} else if (2 == job.get_pac_version()) {
		return encode_result_v2((DtcJob &)job, mtu, job.Timestamp());
	} else {
		log4cplus_error("illegal packet version");
		return -1;
	}
}

void Packet::free_result_buff()
{
	if (!buf)
		return;

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
