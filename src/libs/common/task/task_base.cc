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
#include <unistd.h>
#include <errno.h>
#include <endian.h>
#include <byteswap.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

#include "table/table_def_manager.h"
#include "agent/agent_client.h"
#include "task_base.h"
#include "../decode/decode.h"
#include "protocol.h"
#include "dtc_error_code.h"
#include "../log/log.h"
#include "algorithm/md5.h"
#include "../my/my_request.h"
#include "../my/my_command.h"

#include "../../hsql/include/SQLParser.h"
#include "../../hsql/include/SQLParserResult.h"
#include "../../hsql/include/util/sqlhelper.h"
#include "../../hsql/include/sql/Expr.h"

using namespace hsql;

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

int8_t DtcJob::select_version(const char *packetIn, int packetLen)
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
	if(ver == 1 && packetLen != 5)
	{
		log4cplus_debug("request ver:%d", ver);
		return 1;
	}
	else if(ver == 2 && packetLen != 6)
	{
		log4cplus_debug("request ver:%d", ver);
		return 2;
	}
	else 
	{
		log4cplus_debug("request ver:%d", 0);
		return 0;
	}

	log4cplus_debug("request ver: error");
	return -3;
}

void DtcJob::decode_mysql_packet(const char *packetIn, int packetLen, int type)
{
	if(_client_owner == NULL)
	{
		log4cplus_error("get client owner null error.");
		return ;
	}

	char* p = const_cast<char*>(packetIn);
	p += 3;
	this->mr.pkt_nr = (uint8_t)(*p); // mysql sequence id
	log4cplus_debug("pkt_nr:%d", this->mr.pkt_nr);

	if(_client_owner->get_login_stage() == CONN_STAGE_LOGGING_IN)
	{
		//send ok resp.
		_client_owner->set_login_stage(CONN_STAGE_LOGGED_IN);
	}
	else if(_client_owner->get_login_stage() == CONN_STAGE_LOGGED_IN)
	{
		switch (stage) {
		default:
			break;
		case DecodeStageFatalError:
		case DecodeStageDataError:
		case DecodeStageDone:
			return;
		}

		enum enum_server_command cmd = (enum enum_server_command)*(reinterpret_cast<const uchar*>(packetIn+sizeof(MYSQL_HEADER_SIZE)));
		if (cmd == COM_PING) 
		{
			log4cplus_debug("cmd PING.");
			return;
		}

		mr.set_packet_info(const_cast<char*>(packetIn), packetLen);

		struct timeval tv1, tv2;
		gettimeofday(&tv1, NULL);

		if (!mr.load_sql()) {
			log4cplus_error("load sql error");
			stage = DecodeStageDataError;
			return;
		}
		gettimeofday(&tv2, NULL);
		log4cplus_debug("load sql used time:%d us", tv2.tv_usec- tv1.tv_usec);

		int ret = decode_request_v2(&mr);
		if(ret < 0)
		{
			log4cplus_error("decode request error: %d", ret);
			mr.set_mr_invalid();
			mr.set_mr_msg("dtc syntax error: unexpected identifier.");
			stage = DecodeStageDataError;
			return;
		}

		stage = DecodeStageDone;

	}

	return ;
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

	peerid = header->id;
	int dbname_len = header->dbname_len;

	//offset DTC Header.
	p = p + sizeof(DTC_HEADER_V2);

	mr.set_packet_info(p + dbname_len, packetLen - sizeof(DTC_HEADER_V2) - dbname_len);

	struct timeval tv1, tv2;
	gettimeofday(&tv1, NULL);

	if (!mr.load_sql()) {
		log4cplus_error("load sql error");
		stage = DecodeStageDataError;
		return;
	}
	gettimeofday(&tv2, NULL);
	log4cplus_debug("load sql used time:%d us", tv2.tv_usec- tv1.tv_usec);

	int ret = decode_request_v2(&mr);
	if(ret < 0)
	{
		log4cplus_error("decode request error: %d", ret);
		mr.set_mr_invalid();
		mr.set_mr_msg("dtc syntax error: unexpected identifier.");
		stage = DecodeStageDataError;
		return;
	}

	stage = DecodeStageDone;

	return;
}

// Decode data from packet
//     type 0: clone packet
//     type 1: eat(keep&free) packet
//     type 2: use external packet
void DtcJob::decode_packet_v1(char *packetIn, int packetLen, int type)
{
	log4cplus_debug("decode_packet_v1 entry.");
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
	log4cplus_debug("decode_packet_v1 leave.");
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

int DtcJob::build_field_type_r(int sql_type, char *field_name)
{
	const DTCTableDefinition *tdef = this->table_definition();
	int t_type = tdef->field_type(tdef->field_id(field_name));

	return t_type;
}

bool hsql_convert_value_int(hsql::Expr* input, int* out)
{
    if(input->isType(kExprLiteralInt))
    {
        *out = input->ival;
        return true;
    }
    else if(input->isType(kExprLiteralString))
    {
        char *endptr = NULL;
        long result = strtol(input->name, &endptr, 10);
        if(endptr == input->name)
            return false;
        if(strlen(endptr) > 0)
            return false;
        *out = result;
        return true;    
    }
    else if(input->isType(kExprLiteralFloat))
    {
        *out = static_cast<int>(input->fval);
        return true;
    }

    return false;
}

std::string hsql_convert_value_string(hsql::Expr* input)
{
    if(input->isType(kExprLiteralInt))
    {
        return to_string(input->ival);
    }
    else if(input->isType(kExprLiteralString))
    {
        return input->name;
    }
    else if(input->isType(kExprLiteralFloat))
    {
        return to_string(input->fval);
    }

    return "";
}

bool hsql_convert_value_float(hsql::Expr* input, double* out)
{
    if(input->isType(kExprLiteralFloat))
    {
        *out = input->fval;
        return true;
    }
    else if(input->isType(kExprLiteralString))
    {
        char *endptr = NULL;
        double result = strtod(input->name, &endptr);
        if(endptr == input->name)
            return false;
        if(strlen(endptr) > 0)
            return false;
        *out = result;
        return true;    
    }
    else if(input->isType(kExprLiteralInt))
    {
        *out = static_cast<double>(input->ival);
        return true;
    }

    return false;
}

int build_condition_fields(Expr *expr, Expr *parent,
			   std::vector<hsql::Expr *> *vec_expr)
{
	int count = 0;
	if (!expr)
		return 0;
	switch (expr->type) {
	case kExprColumnRef:
		if (parent) {
			count++;
			vec_expr->push_back(parent);
		}

		break;
	case kExprOperator:
		if ((expr->opType >= kOpEquals &&
		     expr->opType <= kOpGreaterEq) ||
		    expr->opType == kOpAnd) {
			count += build_condition_fields(expr->expr, expr,
							vec_expr);

			if (expr->expr2)
				count += build_condition_fields(expr->expr2,
								NULL, vec_expr);
		} else {
			return 0;
		}
	default:
		break;
	}

	return count;
}

int get_compare_symbol(uint8_t opType)
{
	if(opType == kOpEquals)
		return DField::EQ;
	else if(opType == kOpNotEquals)
		return DField::NE;
	else if(opType == kOpLess)
		return DField::LT;
	else if(opType == kOpLessEq)
		return DField::LE;
	else if(opType == kOpGreater)
		return DField::GT;		
	else if(opType == kOpGreaterEq)
		return DField::GE;
	else
		return DField::EQ; // default case							
}

int DtcJob::decode_request_v2(MyRequest *mr)
{
	char *p = mr->get_packet_ptr();

	//1.versionInfo
	this->versionInfo.set_serial_nr(mr->get_pkt_nr());

	char* p_table_name = mr->get_table_name();
	if(NULL != p_table_name) {
		this->versionInfo.set_table_name(p_table_name);
	}
	this->versionInfo.set_key_type(table_definition()->key_type());

	//2.requestInfo
	static DTCValue key;
	if (mr->get_key(&key, const_cast<char*>(table_definition()->key_name()))) {
		requestInfo.set_key(key);
		set_request_key(&key);
	}
	else
		return -1;
	log4cplus_debug("key type:%d %d", mr->get_request_type(), key.s64);
	set_request_code(mr->get_request_type());

	if (requestCode == DRequest::result_code ||
	    requestCode == DRequest::DTCResultSet) {
		replyCode = requestCode;
		// replyFlags = header.flags;
	} else {
		// requestFlags = header.flags;
		requestType = cmd2type[requestCode];
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
			return -2;
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
	std::vector<hsql::Expr *> exprList;

	hsql::StatementType type = mr->get_result()->getStatement(0)->type();
	hsql::Expr *where = NULL;

	if (type != hsql::StatementType::kStmtInsert) {
		if (type == hsql::StatementType::kStmtUpdate) {
			hsql::UpdateStatement *stmt =
				const_cast<hsql::UpdateStatement*>(
					static_cast<const hsql::UpdateStatement*>(mr->get_result()->getStatement(0)));
			where = stmt->where;
		} else if (type == hsql::StatementType::kStmtDelete) {
			hsql::DeleteStatement *stmt =
				const_cast<hsql::DeleteStatement*>(
					static_cast<const hsql::DeleteStatement*>(mr->get_result()->getStatement(0)));
			where = stmt->expr;

			if (stmt->schema != NULL) {
				mr->m_sql.replace(stmt->st_start_idx ,
					 stmt->st_end_idx - stmt->st_start_idx,
					stmt->tableName);
				log4cplus_info("delete replace sql to :%s" , mr->m_sql.c_str());
			}
		} else if (type == hsql::StatementType::kStmtSelect) {
			hsql::SelectStatement *stmt =
				const_cast<hsql::SelectStatement*>(
					static_cast<const hsql::SelectStatement*>(mr->get_result()->getStatement(0)));
			where = stmt->whereClause;
		} else {
			log4cplus_error("StatementType error: %d", type);
			return -3;
		}

		int cnts = build_condition_fields(where, NULL, &exprList);
		if (cnts != exprList.size()) {
			log4cplus_error("build_condition_fields error: %d %d",
					cnts, exprList.size());
			return -4;
		}
		log4cplus_debug("condition num: %d", cnts);

		if (exprList.size() > 1) {
			int temp = 0;
			for (int i = 0; i < exprList.size(); i++) {
				if (strcmp(exprList.at(i)->expr->getName(),
					   table_definition()->key_name()) ==
				    0) { //is key
					temp--;
					continue;
				}

				if (strcmp(exprList.at(i)->expr->getName(),
					   "WITHOUT@@") ==
				    0) { //is special flag.
					temp--;
					continue;
				}				

				int rtype = build_field_type_r(
					exprList.at(i)
						->expr2
						->type /*DField::Unsigned*/,
					const_cast<char*>(exprList.at(i)->expr->getName()));
				if (rtype == -1) {
					temp--;
					continue;
				}

				log4cplus_debug("fields name: %s, optype: %d", exprList.at(i)->expr->getName(), exprList.at(i)->opType);
				if (DField::Signed == rtype ||
				    DField::Unsigned == rtype) {
					ci.add_value(exprList.at(i)->expr->getName(),
						     get_compare_symbol(exprList.at(i)->opType), DField::Signed,
						     DTCValue::Make(
							     exprList.at(i)->expr2->ival));
				} else if (DField::Float == rtype) {
					ci.add_value(exprList.at(i)->expr->getName(),
						     get_compare_symbol(exprList.at(i)->opType), rtype,
						     DTCValue::Make(
							    exprList.at(i)->expr2->fval));
				} else if (DField::String == rtype ||
					   DField::Binary == rtype) {
					ci.add_value(exprList.at(i)->expr->getName(),
						     get_compare_symbol(exprList.at(i)->opType), rtype,
						     DTCValue::Make(
							    exprList.at(i)->expr2->name));
				}
			}
			ci.Resolve(TableDefinitionManager::instance()
					   ->get_cur_table_def(),
				   0);

			if (exprList.size() + temp > 0) {
				conditionInfo = new DTCFieldValue(
					exprList.size() + temp);
				int err = conditionInfo->Copy(
					ci, 0,
					TableDefinitionManager::instance()
						->get_cur_table_def());
				if (err < 0) {
					log4cplus_error(
						"decode condition info error: %d",
						err);
					return -10;
				}
				clear_all_rows();
			}
		}
	}

	//5.updateInfo Set(update) / values(insert into)
	if (mr->get_update_num_fields() > 0) {
		int count = 0;
		int t = mr->get_result()->getStatement(0)->type();
		if (hsql::StatementType::kStmtUpdate == t) {
			hsql::UpdateStatement *stmt =
				const_cast<hsql::UpdateStatement*>(
					static_cast<const hsql::UpdateStatement*>(mr->get_result()->getStatement(0)));
			
			if (stmt->table->schema != NULL) {
				mr->m_sql.replace(stmt->table->st_start_idx ,
					 stmt->table->st_end_idx - stmt->table->st_start_idx,
					stmt->table->name);
				log4cplus_info(" update replace sql to :%s" , mr->m_sql.c_str());
			}

			count = stmt->updates->size();
			for (int i = 0; i < count; i++) {
				if(strcmp(table_definition()->key_name(), stmt->updates->at(i)->column) == 0)
					return -13;

				int rtype = build_field_type_r(
					stmt->updates->at(i)->value->type,
					stmt->updates->at(i)->column);
				if (rtype == -1) {
					return -11;
				}

				if (DField::Signed == rtype ||
				    DField::Unsigned == rtype) {
					int v = 0;
					if(!hsql_convert_value_int(stmt->updates->at(i)->value, &v))
						return -12;
					ui.add_value(stmt->updates->at(i)->column,
						     DField::Set, rtype,
						     DTCValue::Make(v));
				} else if (DField::Float == rtype) {
					double v = 0;
					if(!hsql_convert_value_float(stmt->updates->at(i)->value, &v))
						return -12;
					ui.add_value(stmt->updates->at(i)->column,
						     DField::Set, rtype,
						     DTCValue::Make(v));
				} else if (DField::String == rtype ||
					   DField::Binary == rtype) {
					std::string v = hsql_convert_value_string(stmt->updates->at(i)->value);
					ui.add_value(stmt->updates->at(i)->column,
						     DField::Set, rtype,
						     DTCValue::Make(v.c_str()));
				}
			}

		} else if (hsql::StatementType::kStmtInsert == t) {
			hsql::InsertStatement *stmt =
				const_cast<hsql::InsertStatement*>(
					static_cast<const hsql::InsertStatement*>(mr->get_result()->getStatement(0)));
			if (stmt->schema != NULL) {
				mr->m_sql.replace(stmt->st_start_idx ,
					 stmt->st_end_idx - stmt->st_start_idx,
					stmt->tableName);
				log4cplus_info("insert replace sql to :%s" , mr->m_sql.c_str());
			}

			if(stmt->columns == NULL)
			{
				if(stmt->values->size() != table_definition()->num_fields())
				{
					log4cplus_error("all fields mode of insert, check num fields failed: %d %d", stmt->values->size(), table_definition()->num_fields());
					return -5;
				}
				count = stmt->values->size();
				for (int i = 0; i < count; i++) {
					char* field_name = const_cast<char*>(table_definition()->field_name(i));
					int rtype = build_field_type_r(
						stmt->values->at(i)->type,
						field_name);
					if (rtype == -1) {
						log4cplus_error("build field type_r error, type: %d, field name: %s", stmt->values->at(i)->type, field_name);
						return -6;
					}

					if (DField::Signed == rtype ||
						DField::Unsigned == rtype) {
						int v = 0;
						if(!hsql_convert_value_int(stmt->values->at(i), &v))
							return -12;
						ui.add_value(field_name,
								DField::Set, rtype,
								DTCValue::Make(v));
					} else if (DField::Float == rtype) {
						double v = 0;
						if(!hsql_convert_value_float(stmt->values->at(i), &v))
							return -12;
						ui.add_value(field_name,
								DField::Set, rtype,
								DTCValue::Make(v));
					} else if (DField::String == rtype ||
						DField::Binary == rtype) {
						log4cplus_debug(
							"DTCValue key: %s, value: %s",
							field_name,
							stmt->values->at(i)->name);
						std::string v = hsql_convert_value_string(stmt->values->at(i));
						ui.add_value(field_name,
								DField::Set, rtype,
								DTCValue::Make(v.c_str()));
					}
				}
			}
			else
			{
				count = stmt->columns->size();
				if(count > 0)
				{
					for (int i = 0; i < count; i++) {
						int rtype = build_field_type_r(
							stmt->values->at(i)->type,
							stmt->columns->at(i));
						if (rtype == -1) {
							log4cplus_error("build field type_r error, type: %d, field name: %s", stmt->values->at(i)->type, stmt->columns->at(i));
							return -7;
						}

						if (DField::Signed == rtype ||
							DField::Unsigned == rtype) {
							int v = 0;
							if(!hsql_convert_value_int(stmt->values->at(i), &v))
								return -12;
							ui.add_value(stmt->columns->at(i),
									DField::Set, rtype,
									DTCValue::Make(v));
						} else if (DField::Float == rtype) {
							double v = 0;
							if(!hsql_convert_value_float(stmt->values->at(i), &v))
								return -12;							
							ui.add_value(stmt->columns->at(i),
									DField::Set, rtype,
									DTCValue::Make(v));
						} else if (DField::String == rtype ||
							DField::Binary == rtype) {
							log4cplus_debug(
								"DTCValue key: %s, value: %s",
								stmt->columns->at(i),
								stmt->values->at(i)->name);
							std::string v = hsql_convert_value_string(stmt->values->at(i));
							ui.add_value(stmt->columns->at(i),
									DField::Set, rtype,
									DTCValue::Make(v.c_str()));
						}
					}
				}
			}

		}

		if (count > 0) {
			ui.Resolve(TableDefinitionManager::instance()
					   ->get_cur_table_def(),
				   0);

			updateInfo = new DTCFieldValue(count);
			int err = updateInfo->Copy(
				ui, 1,
				TableDefinitionManager::instance()
					->get_cur_table_def());
			if (err < 0) {
				log4cplus_error("decode update info error: %d",
						err);
				return -8;
			}
		}
	}

	return 0;
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
				hotbackupTableDef); // Management command, switch to management table definition
			log4cplus_debug("hb table ptr: %p, name: %s",
					hotbackupTableDef,
					hotbackupTableDef->table_name());
		}

		if (role == TaskRoleServer) {
#if 0
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
#endif			

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
