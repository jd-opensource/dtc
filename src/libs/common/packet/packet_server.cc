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

const char *req_string = "select dtctables";

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

int encode_my_fileds_info(char *p, int *len, uint8_t pkt_num,
			  uint8_t fields_num)
{
	//Packet Lenght + Packet Number + Number of fields
	char *offset = p;
	sprintf(offset, "%d", sizeof(fields_num));
	*len += 3;
	offset += 3;

	sprintf(offset, "%d", pkt_num);
	*len += 1;
	offset += 1;

	sprintf(offset, "%d", fields_num);
	*len += 1;
	offset += 1;
}

struct my_result_set_field {
	std::string catalog;
	std::string database;
	std::string table;
	std::string original_table;
	std::string name;
	std::string original_name;
	uint16_t charset_number;
	int32_t length;
	char type;
	uint16_t flags;
	char decimals;
};

int encode_set_field(char *p, my_result_set_field sf)
{
	int len = 0;
	char *start = p;

	len = sf.catalog.length();
	p++;
	sprintf(p, "%d", len);
	if (len > 0) {
		memcpy(p, sf.catalog.c_str(), sf.catalog.length());
	}

	len = sf.database.length();
	sprintf(p, "%d", len);
	p++;
	if (len > 0) {
		memcpy(p, sf.database.c_str(), sf.database.length());
	}

	len = sf.table.length();
	sprintf(p, "%d", len);
	p++;
	if (len > 0) {
		memcpy(p, sf.table.c_str(), sf.table.length());
	}

	len = sf.original_table.length();
	sprintf(p, "%d", len);
	p++;
	if (len > 0) {
		memcpy(p, sf.original_table.c_str(),
		       sf.original_table.length());
	}

	len = sf.name.length();
	sprintf(p, "%d", len);
	p++;
	if (len > 0) {
		memcpy(p, sf.name.c_str(), sf.name.length());
	}

	len = sf.original_name.length();
	sprintf(p, "%d", len);
	p++;
	if (len > 0) {
		memcpy(p, sf.original_name.c_str(), sf.original_name.length());
	}

	//charset number
	p += 2;

	//length
	p += 4;

	//type
	p++;

	return p - start;
}

void encode_field(DtcJob &job, char *p, int *my_len, uint8_t pkt_num)
{
	const DTCTableDefinition *tdef = job.table_definition();

	for (int i = 0; i < job.num_fields(); i++) {
		my_result_set_field sf = { 0 };
		sf.charset_number = 0xff;
		sf.type = job.field_type(i);
		sf.catalog = job.field_name(i);
		sf.table = job.table_name();

		int set_len = encode_set_field(p + 4, sf);

		//Packet Lenght + Packet Number
		sprintf(p, "%d", set_len);
		*my_len += 3;
		p += 3;

		sprintf(p, "%d", pkt_num++);
		*my_len += 1;
		p += 1;

		*my_len += set_len;
		p += set_len;
	}
}

struct my_result_set_eof {
	char eof = 0xfe;
	uint16_t warning;
	uint16_t server_status;
};

int encode_eof(char *my_result, int *my_len, int pkt_nr)
{
	char *p = my_result + *my_len;
	my_result_set_eof eof;

	eof.warning = 0;
	eof.server_status = 0;
}

int encode_row_data(ResultSet *rp, char *my_result, int *my_len, int pkt_nr)
{
	ResultSet *pstResultSet = rp;
	char *p = my_result + *my_len;

	for (int i = 0; i < pstResultSet->total_rows(); i++) {
		RowValue *pstRow = pstResultSet->_fetch_row();
		if (pstRow == NULL) {
			log4cplus_info("%s!", "call FetchRow func error");
			continue;
		}

		for (int j = 0; j < pstResultSet->num_fields(); j++) {
			DTCValue *v = pstRow->field_value(j);

			//Packet Lenght + Packet Number
			sprintf(p, "%d", v->str.len + 1);
			*my_len += 3;
			p += 3;

			sprintf(p, "%d", pkt_nr++);
			*my_len += 1;
			p += 1;

			memcpy(p, v->str.ptr, v->str.len);
			*my_len += v->str.len;
			p += v->str.len;
		}
	}
}

int Packet::encode_mysql_protocol(ResultSet *rp, char *my_result, int *my_len,
				  DtcJob &job)
{
	my_result = new char[MAXPACKETSIZE];
	*my_len = 0;
	int pkt_nr = job.mr.get_pkt_nr();
	log4cplus_debug("***11111111111111");

	encode_my_fileds_info(my_result, my_len, ++pkt_nr,
			      job.mr.get_need_num_fields());
	log4cplus_debug("***2222222222222");
	//encode_field(job, my_result, my_len, ++pkt_nr);
	//encode_eof();
	log4cplus_debug("***3333333");
	//encode_row_data(rp, my_result, my_len, ++pkt_nr);
	log4cplus_debug("***4444444444444");
	//encode_eof(my_result, my_len, ++pkt_nr);
	log4cplus_debug("***5555555555555");
}

int net_send_ok(int affectedRow)
{
	uint8_t buf[100] = { 0x00, (uint8_t)affectedRow, 0x00, 0x02, 0x00, 0x00,
			     0x00 };
}

bool is_desc_tables(DtcJob *job)
{
	std::string sql = job->mr.get_sql();
	if (sql == string(req_string))
		return true;

	return false;
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
	header.id = job->request_serial();
	header.packet_len = packet_len;
	header.admin = CMD_KEY_DEFINE;

	if (buf == NULL) {
		buf = (BufferChain *)MALLOC(packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = packet_len - sizeof(BufferChain);
	} else if (buf &&
		   packet_len - (int)sizeof(BufferChain) > buf->totalBytes) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = packet_len - sizeof(BufferChain);
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
}

int Packet::encode_result_v2(DtcJob &job, int mtu, uint32_t ts)
{
	log4cplus_debug("encode_result_v2 entry.");

	if (is_desc_tables(&job)) {
		return desc_tables_result(&job);
	}

	// rp指向返回数据集
	ResultPacket *rp =
		job.result_code() >= 0 ? job.get_result_packet() : NULL;
	BufferChain *rb = NULL;
	int nrp = 1, lrp = 0, off = 0;
	log4cplus_debug("111111111111");
	if (mtu <= 0) {
		log4cplus_debug("222222222");
		mtu = MAXPACKETSIZE;
	}
	log4cplus_debug("333333333333333");
	/* rp may exist but no result */
	if (rp && (rp->numRows || rp->totalRows)) {
		log4cplus_debug("4444444444444");
		//rb指向数据结果集缓冲区起始位置
		rb = rp->bc;
		if (rb)
			rb->Count(nrp, lrp);
		off = 5 - encoded_bytes_length(rp->numRows);
		encode_length(rb->data + off, rp->numRows);
		lrp -= off;
		job.resultInfo.set_total_rows(rp->totalRows);
	} else {
		log4cplus_debug("55555555555555555:%d %d", job.result_code(),
				is_desc_tables(&job));
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
	log4cplus_debug("66666666666666666666");
	if (ts) {
		job.resultInfo.set_time_info(ts);
	}
	job.versionInfo.set_serial_nr(job.request_serial() + 1);

	if (job.result_key() == NULL && job.request_key() != NULL)
		job.set_result_key(*job.request_key());
	log4cplus_debug("7777777777777777777:%d %d %p %d", job.mr.get_pkt_nr(),
			job.request_serial(), &job.mr.pkt_nr, job.mr.pkt_nr);

	DTC_HEADER_V2 dtc_header = { 0 };
	dtc_header.version = 2;
	dtc_header.id = job.request_serial();
	dtc_header.packet_len = 0;
	dtc_header.admin = CMD_NOP;

	log4cplus_debug("8888888888888888888888");

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
	} else if (buf && first_packet_len - (int)sizeof(BufferChain) >
				  buf->totalBytes) {
		FREE_IF(buf);
		buf = (BufferChain *)MALLOC(first_packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = first_packet_len - sizeof(BufferChain);
	}
	log4cplus_debug("999999999999999999999999");
	//设置要发送的第一个包
	char *p = buf->data + sizeof(struct iovec) * (nrp + 1);
	v = (struct iovec *)buf->data;
	v->iov_base = p;
	v->iov_len = sizeof(dtc_header);
	nv = nrp + 1;

	//修改第一个包的内容
	memcpy(p, &dtc_header, sizeof(dtc_header));
	p += sizeof(dtc_header);
	log4cplus_debug("AAAAAAAAAAAAAAAAAAAAAAA");
	if (p - (char *)v->iov_base != sizeof(dtc_header))
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			sizeof(dtc_header));
	log4cplus_debug("BBBBBBBBBBBBBBB");
	//转换内容包
	//job.decode_result_set(rb->data, lrp);
	char *my_result = NULL;
	int my_len = 0;
	encode_mysql_protocol(job.result, my_result, &my_len, job);
	log4cplus_debug("CCCCCCCCCCCCCCCCCCCCCCCCCC");

	log4cplus_debug("CCCCCCCCCCCCCCCCCCCCCCCCCC-CCCCCCc:%d %p %d", nrp, rb,
			off);
	log4cplus_debug("444444444:%p", rb->data);
	for (int i = 1; i <= nrp; i++, rb = rb->nextBuffer) {
		v[i].iov_base = my_result;
		log4cplus_debug("11111111111");
		v[i].iov_len = my_len;
		log4cplus_debug("22222222222");
		off = 0;
	}

	log4cplus_debug("encode_result_v2 leave.");
	return 0;
}

int Packet::encode_result(DTCJobOperation &job, int mtu)
{
	//return encode_result((DtcJob &)job, mtu, job.Timestamp());
	return encode_result_v2((DtcJob &)job, mtu, job.Timestamp());
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
