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
#include <unistd.h>
#include <errno.h>
#include <endian.h>
#include <byteswap.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

#include "table/table_def_manager.h"
#include "task_base.h"
#include "../decode/decode.h"
#include "protocol.h"
#include "dtc_error_code.h"
#include "../log/log.h"
#include "algorithm/md5.h"
#include "../my/my_request.h"

#include "../../hsql/include/SQLParser.h"
#include "../../hsql/include/SQLParserResult.h"
#include "../../hsql/include/util/sqlhelper.h"

int DtcJob::check_packet_size(const char *buf, int len)
{
	const DTC_HEADER_V1 *header = (DTC_HEADER_V1 *)buf;
	if (len < (int)sizeof(DTC_HEADER_V1))
		return 0;

	if (header->version != 1) { // version not supported
		return -1;
	}

	if (header->scts != DRequest::Section::Total) { // tags# mismatch
		return -2;
	}

	int i;
	int64_t n;
	for (i = 0, n = 0; i < DRequest::Section::Total; i++) {
#if __BYTE_ORDER == __BIG_ENDIAN
		n += bswap_32(header->len[i]);
#else
		n += header->len[i];
#endif
	}

	if (n > MAXPACKETSIZE) { // oversize
		return -4;
	}

	return (int)n + sizeof(DTC_HEADER_V1);
}

void DtcJob::decode_stream(SimpleReceiver &receiver)
{
	int rv;

	switch (stage) {
	default:
		break;
	case DecodeStageFatalError:
	case DecodeStageDataError:
	case DecodeStageDone:
		return;

	case DecodeStageIdle:
		receiver.init();
	case DecodeStageWaitHeader:
		rv = receiver.fill();

		if (rv == -1) {
			if (errno != EAGAIN && errno != EINTR &&
			    errno != EINPROGRESS)
				stage = DecodeStageFatalError;
			return;
		}

		if (rv == 0) {
			errno = role == TaskRoleServer &&
						stage == DecodeStageIdle ?
					0 :
					ECONNRESET;
			stage = DecodeStageFatalError;
			return;
		}

		stage = DecodeStageWaitHeader;

		if (receiver.remain() > 0) {
			return;
		}

		if ((rv = decode_header_v1(receiver.header(),
					   &receiver.header())) < 0) {
			return;
		}

		char *buf = packetbuf.Allocate(rv, role);
		receiver.set(buf, rv);
		if (buf == NULL) {
			set_error(-ENOMEM, "decoder", "Insufficient Memory");
			stage = DecodeStageDiscard;
		}
	}

	if (stage == DecodeStageDiscard) {
		rv = receiver.discard();
		if (rv == -1) {
			if (errno != EAGAIN && errno != EINTR) {
				stage = DecodeStageFatalError;
			}

			return;
		}

		if (rv == 0) {
			stage = DecodeStageFatalError;
			return;
		}

		stage = DecodeStageDataError;
		return;
	}

	rv = receiver.fill();

	if (rv == -1) {
		if (errno != EAGAIN && errno != EINTR) {
			stage = DecodeStageFatalError;
		}

		return;
	}

	if (rv == 0) {
		stage = DecodeStageFatalError;
		return;
	}

	if (receiver.remain() <= 0) {
		decode_request_v1(receiver.header(), receiver.c_str());
	}

	return;
}

int8_t DtcJob::select_version(char *packetIn, int packetLen)
{
	int8_t ver = 0;
	log4cplus_debug("select version entry.");

	switch (stage) {
	default:
		break;
	case DecodeStageFatalError:
	case DecodeStageDataError:
	case DecodeStageDone:
		log4cplus_error("stage: %d", stage);
		return -1;
	}

	if (packetLen <= 1) {
		log4cplus_error("packet len: %d", packetLen);
		stage = DecodeStageFatalError;
		return -2;
	}

	ver = (int8_t)(packetIn[0]);
	log4cplus_debug("incoming dtc packet version:%d", ver);
	if (ver != 1 && ver != 2) {
		log4cplus_error("dtc packet version error:%d, pkt len:%d", ver,
				packetLen);
		stage = DecodeStageFatalError;
		return -3;
	}

	return ver;
}

// Decode data from packet
//     type 0: clone packet
//     type 1: eat(keep&free) packet
//     type 2: use external packet
void DtcJob::decode_packet_v2(char *packetIn, int packetLen, int type)
{
	DTC_HEADER_V2 *header;
	char *p = packetIn;

	switch (stage) {
	default:
		break;
	case DecodeStageFatalError:
	case DecodeStageDataError:
	case DecodeStageDone:
		return;
	}

	if (packetLen < (int)sizeof(DTC_HEADER_V2)) {
		stage = DecodeStageFatalError;
		return;
	}

	//Parse DTC v2 protocol.
	header = (DTC_HEADER_V2 *)p;

	if (header->version != 2) {
		stage = DecodeStageDataError;
		return;
	}

	serialNr = header->id;

	//offset DTC Header.
	p = p + sizeof(DTC_HEADER_V2);

	mr.set_packet_info(p, packetLen - sizeof(DTC_HEADER_V2));
	if (!mr.load_sql()) {
		log4cplus_error("load sql error");
		stage = DecodeStageDataError;
		return;
	}

	decode_request_v2(&mr);

	return;
}

// Decode data from packet
//     type 0: clone packet
//     type 1: eat(keep&free) packet
//     type 2: use external packet
void DtcJob::decode_packet_v1(char *packetIn, int packetLen, int type)
{
	DTC_HEADER_V1 header;
#if __BYTE_ORDER == __BIG_ENDIAN
	DTC_HEADER_V1 out[1];
#else
	DTC_HEADER_V1 *const out = &header;
#endif

	switch (stage) {
	default:
		break;
	case DecodeStageFatalError:
	case DecodeStageDataError:
	case DecodeStageDone:
		return;
	}

	if (packetLen < (int)sizeof(DTC_HEADER_V1)) {
		stage = DecodeStageFatalError;
		return;
	}

	memcpy((char *)&header, packetIn, sizeof(header));

	int rv = decode_header_v1(header, out);
	if (rv < 0) {
		return;
	}

	if ((int)(sizeof(DTC_HEADER_V1) + rv) > packetLen) {
		stage = DecodeStageFatalError;
		return;
	}

	char *buf = (char *)packetIn + sizeof(DTC_HEADER_V1);
	switch (type) {
	default:
	case 0:
		// clone packet buffer
		buf = packetbuf.Clone(buf, rv, role);
		if (buf == NULL) {
			set_error(-ENOMEM, "decoder", "Insufficient Memory");
			stage = DecodeStageDataError;
			return;
		}
		break;

	case 1:
		packetbuf.Push(packetIn);
		break;
	case 2:
		break;
	}

	decode_request_v1(*out, buf);
	return;
}

int DtcJob::decode_header_v1(const DTC_HEADER_V1 &header, DTC_HEADER_V1 *out)
{
	if (header.version != 1) { // version not supported
		stage = DecodeStageFatalError;
		log4cplus_debug("version incorrect: %d", header.version);

		return -1;
	}

	if (header.scts != DRequest::Section::Total) { // tags# mismatch
		stage = DecodeStageFatalError;
		log4cplus_debug("session count incorrect: %d", header.scts);

		return -2;
	}

	int i;
	int64_t n;
	for (i = 0, n = 0; i < DRequest::Section::Total; i++) {
#if __BYTE_ORDER == __BIG_ENDIAN
		const unsigned int v = bswap_32(header.len[i]);
		if (out)
			out->len[i] = v;
#else
		const unsigned int v = header.len[i];
#endif
		n += v;
	}

	if (n > MAXPACKETSIZE) { // oversize
		stage = DecodeStageFatalError;
		log4cplus_debug("package size error: %ld", (long)n);

		return -4;
	}

	if (header.cmd == DRequest::result_code ||
	    header.cmd == DRequest::DTCResultSet) {
		replyCode = header.cmd;
		replyFlags = header.flags;
	} else {
		requestCode = header.cmd;
		requestFlags = header.flags;
		requestType = cmd2type[requestCode];
	}

	stage = DecodeStageWaitData;

	return (int)n;
}

int DtcJob::validate_section(DTC_HEADER_V1 &header)
{
	int i;
	int m;
	for (i = 0, m = 0; i < DRequest::Section::Total; i++) {
		if (header.len[i])
			m |= 1 << i;
	}

	if (header.cmd > 17 || !(validcmds[role] & (1 << header.cmd))) {
		set_error(-EC_BAD_COMMAND, "decoder", "Invalid Command");
		return -1;
	}

	if ((m & validsections[header.cmd][0]) !=
	    validsections[header.cmd][0]) {
		set_error(-EC_MISSING_SECTION, "decoder", "Missing Section");
		return -1;
	}

	if ((m & ~validsections[header.cmd][1]) != 0) {
		log4cplus_error("m[%x] valid[%x]", m,
				validsections[header.cmd][1]);
		set_error(-EC_EXTRA_SECTION, "decoder", "Extra Section");
		return -1;
	}

	return 0;
}

void DtcJob::decode_request_v2(MyRequest *mr)
{
	char *p = mr->get_packet_ptr();

	//1.versionInfo
	this->versionInfo.set_serial_nr(mr->get_pkt_nr());

	//2.requestInfo
	DTCValue key;
	if (mr->get_key(&key)) {
		requestInfo.set_key(key);
	}

	//limit
	if (mr->get_limit_count()) {
		requestInfo.set_limit_start(mr->get_limit_start());
		requestInfo.set_limit_count(mr->get_limit_count());
	}

	//3.fieldList(Need)
	if (mr->get_need_num_fields() > 0) {
		FieldSetByName fs;
		std::vector<std::string> need = mr->get_need_array();
		if (need.size() != mr->get_need_num_fields()) {
			log4cplus_error("need field num error:%d, %d",
					need.size(), mr->get_need_num_fields());
			return;
		}
		for (int i = 0; i < need.size(); i++) {
			fs.add_field(need[i].c_str(), i);
		}
		if (!fs.Solved())
			fs.Resolve(TableDefinitionManager::instance()
					   ->get_cur_table_def(),
				   0);

		fieldList = new DTCFieldSet(fs.num_fields());
		fieldList->Copy(fs); // never failed
	}

	//4.conditionInfo(where)
	if (mr->get_condition_num_fields() > 0) {
		hsql::SelectStatement *stmt = mr->get_result()->getStatement(0);
		hsql::Expr *where = stmt->whereClause;

		int size = where->exprList->size();
		if (size <= 1) {
			log4cplus_error("condition num error: %d", size);
			return;
		}

		FieldValueByName ci;
		for (int i = 0; i < size; i++) {
			ci.add_value(
				where->exprList->at(i)->getName(), DField::Set,
				where->exprList->at(i)
					->type /*DField::Unsigned*/,
				DTCValue::Make(
					where->exprList->at(i)->expr2->ival));
		}

		conditionInfo = new DTCFieldValue(size);
		int err = conditionInfo->Copy(
			ci, 0,
			TableDefinitionManager::instance()->get_cur_table_def());
		if (err < 0) {
			log4cplus_error("decode condition info error: %d", err);
			return;
		}
	}

	//5.updateInfo Set(update) / values(insert into)
	if (mr->get_update_num_fields() > 0) {
		DTCFieldValue *updateInfo;
	}
}

void DtcJob::decode_request_v1(DTC_HEADER_V1 &header, char *p)
{
#if !CLIENTAPI
	if (DRequest::ReloadConfig == requestCode &&
	    TaskTypeHelperReloadConfig == requestType) {
		log4cplus_error("TaskTypeHelperReloadConfig == requestType");
		stage = DecodeStageDone;
		return;
	}
#endif
	int id = 0;
	int err = 0;

#define ERR_RET(ret_msg, fmt, args...)                                         \
	do {                                                                   \
		set_error(err, "decoder", ret_msg);                            \
		log4cplus_debug(fmt, ##args);                                  \
		goto error;                                                    \
	} while (0)

	//VersionInfo
	if (header.len[id]) {
		/* decode version info */
		err = decode_simple_section(p, header.len[id], versionInfo,
					    DField::None);
		if (err)
			ERR_RET("decode version info error",
				"decode version info error: %d", err);

		/* backup serialNr */
		serialNr = versionInfo.serial_nr();

		if (header.cmd == DRequest::TYPE_SYSTEM_COMMAND) {
			set_table_definition(
				hotbackupTableDef); // 管理命令，换成管理表定义
			log4cplus_debug("hb table ptr: %p, name: %s",
					hotbackupTableDef,
					hotbackupTableDef->table_name());
		}

		if (role == TaskRoleServer) {
			/* local storage no need to check table, because it always set it to "@HOT_BACKUP", checking tablename */
			if (requestCode != DRequest::Replicate &&
			    !is_same_table(versionInfo.table_name())) {
				err = -EC_TABLE_MISMATCH;
				requestFlags |=
					DRequest::Flag::NeedTableDefinition;

				ERR_RET("table mismatch",
					"table mismatch: %d, client[%.*s], server[%s]",
					err, versionInfo.table_name().len,
					versionInfo.table_name().ptr,
					table_name());
			}

			/* check table hash */
			if (requestCode != DRequest::Replicate &&
			    versionInfo.table_hash().len > 0 &&
			    !hash_equal(versionInfo.table_hash())) {
#if !CLIENTAPI

				DTCTableDefinition *oldDef =
					TableDefinitionManager::instance()
						->get_old_table_def();
				if (oldDef &&
				    oldDef->hash_equal(
					    versionInfo.table_hash())) {
					requestFlags |= DRequest::Flag::
						NeedTableDefinition;
					log4cplus_error(
						"mismatch new table, but match old table, so notify update tabledefinition! pid [%d]",
						getpid());
				} else {
					err = -EC_CHECKSUM_MISMATCH;
					requestFlags |= DRequest::Flag::
						NeedTableDefinition;

					ERR_RET("table mismatch",
						"table mismatch: %d", err);
				}

#else

				err = -EC_CHECKSUM_MISMATCH;
				requestFlags |=
					DRequest::Flag::NeedTableDefinition;

				ERR_RET("table mismatch", "table mismatch: %d",
					err);

#endif
			}
			/* checking keytype */
			/* local storage no need to check table, because it always set it to "unsigned int", not the
         	* acutal key type */
			if (requestCode != DRequest::Replicate) {
				const unsigned int t = versionInfo.key_type();
				const int rt = key_type();
				if (!(requestFlags &
				      DRequest::Flag::MultiKeyValue) &&
				    (t >= DField::TotalType ||
				     !validktype[t][rt])) {
					err = -EC_BAD_KEY_TYPE;
					requestFlags |= DRequest::Flag::
						NeedTableDefinition;

					ERR_RET("key type incorrect",
						"key type incorrect: %d", err);
				}
			}
		}

	} //end of version info

	if (requestCode != DRequest::Replicate &&
	    validate_section(header) < 0) {
		stage = DecodeStageDataError;
		return;
	}

	if (header.cmd == DRequest::TYPE_PASS && role == TaskRoleServer) {
		stage = DecodeStageDataError;
		return;
	}

	//table_definition
	p += header.len[id++];

	// only client use remote table
	if (header.len[id] && allow_remote_table()) {
		/* decode table definition */
		DTCTableDefinition *tdef;
		try {
			tdef = new DTCTableDefinition;
		} catch (int e) {
			err = e;
			goto error;
		}
		int rv = tdef->Unpack(p, header.len[id]);
		if (rv != 0) {
			delete tdef;

			ERR_RET("unpack table info error",
				"unpack table info error: %d", rv);
		}
		DTCTableDefinition *old = table_definition();
		DEC_DELETE(old);
		set_table_definition(tdef);
		tdef->increase();
		if (!(header.flags & DRequest::Flag::admin_table))
			mark_has_remote_table();
	}

	//RequestInfo
	p += header.len[id++];

	if (header.len[id]) {
		/* decode request info */
		if (requestFlags & DRequest::Flag::MultiKeyValue)
			err = decode_simple_section(
				p, header.len[id], requestInfo, DField::Binary);
		else
			err = decode_simple_section(p, header.len[id],
						    requestInfo,
						    table_definition() ?
							    field_type(0) :
							    DField::None);

		if (err)
			ERR_RET("decode request info error",
				"decode request info error: %d", err);

		/* key present */
		key = requestInfo.key();

		if (header.cmd == DRequest::TYPE_SYSTEM_COMMAND &&
		    requestInfo.admin_code() ==
			    DRequest::SystemCommand::RegisterHB) {
			if (versionInfo.data_table_hash().len <= 0 ||
			    !dataTableDef->hash_equal(
				    versionInfo.data_table_hash())) {
				err = -EC_CHECKSUM_MISMATCH;
				requestFlags |=
					DRequest::Flag::NeedTableDefinition;

				ERR_RET("table mismatch", "table mismatch: %d",
					err);
			}
		}
	}

	//ResultInfo
	p += header.len[id++];

	if (header.len[id]) {
		/* decode result code */
		err = decode_simple_section(p, header.len[id], resultInfo,
					    table_definition() ? field_type(0) :
								 DField::None);

		if (err)
			ERR_RET("decode result info error",
				"decode result info error: %d", err);
		rkey = resultInfo.key();
	}

	//UpdateInfo
	p += header.len[id++];

	if (header.len[id]) {
		/* decode updateInfo */
		const int mode = requestCode == DRequest::Update ? 2 : 1;
		err = decode_field_value(p, header.len[id], mode);

		if (err)
			ERR_RET("decode update info error",
				"decode update info error: %d", err);
	}

	//ConditionInfo
	p += header.len[id++];

	if (header.len[id]) {
		/* decode conditionInfo */
		err = decode_field_value(p, header.len[id], 0);

		if (err)
			ERR_RET("decode condition error",
				"decode condition error: %d", err);
	}

	//FieldSet
	p += header.len[id++];

	if (header.len[id]) {
		/* decode fieldset */
		err = decode_field_set(p, header.len[id]);

		if (err)
			ERR_RET("decode field set error",
				"decode field set error: %d", err);
	}

	//DTCResultSet
	p += header.len[id++];

	if (header.len[id]) {
		/* decode resultset */
		err = decode_result_set(p, header.len[id]);

		if (err)
			ERR_RET("decode result set error",
				"decode result set error: %d", err);
	}

	stage = DecodeStageDone;
	return;

#undef ERR_RET

error:
	set_error(err, "decoder", NULL);
	stage = DecodeStageDataError;
}

int DtcJob::decode_field_set(char *d, int l)
{
	uint8_t mask[32];
	FIELD_ZERO(mask);

	DTCBinary bin = { l, d };
	if (!bin)
		return -EC_BAD_SECTION_LENGTH;

	const int num = *bin++;
	if (num == 0)
		return 0;
	int realnum = 0;
	uint8_t idtab[num];

	/* old: buf -> local -> id -> idtab -> job */
	/* new: buf -> local -> idtab -> job */
	for (int i = 0; i < num; i++) {
		int nd = 0;
		int rv = decode_field_id(bin, idtab[realnum],
					 table_definition(), nd);
		if (rv != 0)
			return rv;
		if (FIELD_ISSET(idtab[realnum], mask))
			continue;
		FIELD_SET(idtab[realnum], mask);
		realnum++;
		if (nd)
			requestFlags |= DRequest::Flag::NeedTableDefinition;
	}

	if (!!bin.len)
		return -EC_EXTRA_SECTION_DATA;

	/* allocate max field in field set, first byte indicate real field num */
	if (!fieldList) {
		try {
			fieldList = new DTCFieldSet(idtab, realnum,
						    num_fields() + 1);
		} catch (int err) {
			return -ENOMEM;
		}
	} else {
		if (fieldList->max_fields() < num_fields() + 1) {
			fieldList->Realloc(num_fields() + 1);
		}
		if (fieldList->Set(idtab, realnum) < 0) {
			log4cplus_warning("fieldList not exist in pool");
			return -EC_TASKPOOL;
		}
	}

	return 0;
}

int DtcJob::decode_field_value(char *d, int l, int mode)
{
	DTCBinary bin = { l, d };
	if (!bin)
		return -EC_BAD_SECTION_LENGTH;

	const int num = *bin++;
	if (num == 0)
		return 0;

	/* conditionInfo&updateInfo at largest size in pool, numFields indicate real field */
	DTCFieldValue *fv;
	if (mode == 0 && conditionInfo) {
		fv = conditionInfo;
	} else if (mode != 0 && updateInfo) {
		fv = updateInfo;
	} else {
		try {
			fv = new DTCFieldValue(num);
		} catch (int err) {
			return err;
		}
		if (mode == 0)
			conditionInfo = fv;
		else
			updateInfo = fv;
	}

	if (fv->max_fields() < num) {
		fv->Realloc(num);
	}

	int err;
	int keymask = 0;
	for (int i = 0; i < num; i++) {
		uint8_t id;
		DTCValue val;
		uint8_t op;
		uint8_t vt;

		if (!bin) {
			err = -EC_BAD_SECTION_LENGTH;
			goto bad;
		}

		op = *bin++;
		vt = op & 0xF;
		op >>= 4;

		int nd = 0;
		/* buf -> local -> id */
		err = decode_field_id(bin, id, table_definition(), nd);
		if (err != 0)
			goto bad;
		err = -EC_BAD_INVALID_FIELD;
		if (id > num_fields())
			goto bad;

		const int ft = field_type(id);

		if (vt >= DField::TotalType) {
			err = -EC_BAD_VALUE_TYPE;
			goto bad;
		}

		err = -EC_BAD_OPERATOR;

		if (mode == 0) {
			if (op >= DField::TotalComparison ||
			    validcomps[ft][op] == 0)
				goto bad;
			if (id < key_fields()) {
				if (op != DField::EQ) {
					err = -EC_BAD_MULTIKEY;
					goto bad;
				}
			} else
				clear_all_rows();

			if (validxtype[DField::Set][ft][vt] == 0)
				goto bad;
		} else if (mode == 1) {
			if (op != DField::Set)
				goto bad;
			if (validxtype[op][ft][vt] == 0)
				goto bad;
		} else {
			if (table_definition()->is_read_only(id)) {
				err = -EC_READONLY_FIELD;
				goto bad;
			}
			if (op >= DField::TotalOperation ||
			    validxtype[op][ft][vt] == 0)
				goto bad;
		}

		/* avoid one more copy of DTCValue*/
		/* int(len, value):  buf -> local; buf -> local -> tag */
		/* str(len, value):  buf -> local -> tag; buf -> tag */
		//err = decode_data_value(bin, val, vt);
		err = decode_data_value(bin, *fv->next_field_value(), vt);
		if (err != 0)
			goto bad;
		//fv->add_value(id, op, vt, val);
		fv->add_value_no_val(id, op, vt);
		if (nd)
			requestFlags |= DRequest::Flag::NeedTableDefinition;
		if (id < key_fields()) {
			if ((keymask & (1 << id)) != 0) {
				err = -EC_BAD_MULTIKEY;
				goto bad;
			}
			keymask |= 1 << id;
		} else {
			fv->update_type_mask(
				table_definition()->field_flags(id));
		}
	}

	if (!!bin) {
		err = -EC_EXTRA_SECTION_DATA;
		goto bad;
	}

	return 0;
bad:
	/* free when distructed instread free here*/
	//delete fv;
	return err;
}

int DtcJob::decode_result_set(char *d, int l)
{
	uint32_t nrows;
	int num;

	uint8_t mask[32];
	FIELD_ZERO(mask);

	DTCBinary bin = { l, d };

	int err = decode_length(bin, nrows);
	if (err)
		return err;

	if (!bin)
		return -EC_BAD_SECTION_LENGTH;

	num = *bin++;

	/* buf -> id */
	//check duplicate or bad field id
	//	int haskey = 0;
	if (num > 0) {
		if (bin < num)
			return -EC_BAD_SECTION_LENGTH;

		for (int i = 0; i < num; i++) {
			int t = bin[i];
			//			if (t == 0)
			//				haskey = 1;
			if (t == 255)
				return -EC_BAD_FIELD_ID;
			if (FIELD_ISSET(t, mask))
				return -EC_DUPLICATE_FIELD;
			FIELD_SET(t, mask);
		}
	}

	/* result's fieldset at largest size */
	/* field ids: buf -> result */
	if (!result) {
		try {
			result = new ResultSet((uint8_t *)bin.ptr, num,
					       num_fields() + 1,
					       table_definition());
		} catch (int err) {
			return err;
		}
	} else {
		if (result->field_set_max_fields() < num_fields() + 1)
			result->realloc_field_set(num_fields() + 1);
		result->Set((uint8_t *)bin.ptr, num);
	}

	bin += num;
	result->set_value_data(nrows, bin);
	return 0;
}

int ResultSet::decode_row(void)
{
	if (err)
		return err;
	if (rowno == numRows)
		return -EC_NO_MORE_DATA;
	for (int i = 0; i < num_fields(); i++) {
		const int id = field_id(i);
		err = decode_data_value(curr, row[id], row.field_type(id));
		if (err)
			return err;
	}
	rowno++;
	return 0;
}

int packet_body_len_v1(DTC_HEADER_V1 &header)
{
	int pktbodylen = 0;
	for (int i = 0; i < DRequest::Section::Total; i++) {
#if __BYTE_ORDER == __BIG_ENDIAN
		const unsigned int v = bswap_32(header.len[i]);
#else
		const unsigned int v = header.len[i];
#endif
		pktbodylen += v;
	}
	return pktbodylen;
}
