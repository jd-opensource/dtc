/*
* Copyright [2022] JD.com, Inc.
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
#include "../log/log.h"
#include "my_request.h"
#include "my_command.h"

using namespace hsql;

bool MyRequest::do_mysql_protocol_parse()
{
	char *p = this->raw;

	if (p == NULL || this->raw_len < MYSQL_HEADER_SIZE) {
		log4cplus_error("receive size small than package header.");
		return false;
	}

	int input_packet_length = uint_trans_3(p);
	log4cplus_debug("uint_trans_3:0x%x 0x%x 0x%x, len:%d", p[0], p[1], p[2],
			input_packet_length);
	p += 3;
	this->pkt_nr = (uint8_t)(*p); // mysql sequence id
	p++;
	log4cplus_debug("pkt_nr:%d, packet len:%d", this->pkt_nr,
			input_packet_length);

	if (sizeof(MYSQL_HEADER_SIZE) + input_packet_length > raw_len) {
		log4cplus_error(
			"mysql header len %d is different with actual len %d.",
			input_packet_length, raw_len);
		return false;
	}

	enum enum_server_command cmd = (enum enum_server_command)(uchar)p[0];
	if (cmd != COM_QUERY) {
		log4cplus_error("cmd type error:%d", cmd);
		return false;
	}

	input_packet_length --;
	p++;

	if (*p == 0x0) {
		p++;
		input_packet_length--;
	}

	if (*p == 0x01) {
		p++;
		input_packet_length--;
	}
	m_sql.assign(p, input_packet_length);
	log4cplus_debug("sql: \"%s\"", m_sql.c_str());

	return true;
}

bool MyRequest::load_sql()
{
	log4cplus_debug("load_sql entry.");
	if (!check_packet_info())
		return false;

	if (!do_mysql_protocol_parse())
		return false;

	if ((m_sql.find("insert into") != string::npos ||
	     m_sql.find("INSERT INTO") != string::npos) &&
	    (m_sql.find(" where ") != string::npos ||
	     m_sql.find(" WHERE ") != string::npos)) {
		m_sql = m_sql.substr(0, m_sql.find(" where "));
		m_sql = m_sql.substr(0, m_sql.find(" WHERE "));
	}

	log4cplus_debug("sql: %s", m_sql.c_str());
	hsql::SQLParser::parse(m_sql, &m_result);
	if (m_result.isValid()) {
		log4cplus_debug("load_sql success.");
		return true;
	} else {
		log4cplus_error("%s (Line %d:%d)", m_result.errorMsg(),
				m_result.errorLine(), m_result.errorColumn());
		return false;
	}

	//check statement size.
	if (m_result.size() != 1)
		return false;

	return false;
}

bool MyRequest::check_packet_info()
{
	if (this->raw == NULL || this->raw_len <= 0) {
		log4cplus_error(
			"check packet info error:%p %d，set packet info first please",
			this->raw, this->raw_len);
		return false;
	} else
		return true;
}

bool MyRequest::get_key(DTCValue *key, char *key_name)
{
	hsql::Expr *where = NULL;
	int t = m_result.getStatement(0)->type();

	if (hsql::StatementType::kStmtInsert == t) {
		hsql::InsertStatement *stmt = get_result()->getStatement(0);
		for (int i = 0; i < stmt->columns->size(); i++) {
			if (strcmp(stmt->columns->at(i), key_name) == 0) {

				switch (stmt->values->at(i)->type) {
				case hsql::ExprType::kExprLiteralInt:
					*key = DTCValue::Make(
						stmt->values->at(i)->ival);
					return true;
				case hsql::ExprType::kExprLiteralFloat:
					*key = DTCValue::Make(
						stmt->values->at(i)->fval);
					return true;
				case hsql::ExprType::kExprLiteralString:
					*key = DTCValue::Make(
						stmt->values->at(i)->name);
					return true;
				default:
					return false;
				}
			}
		}
	} else {
		if (hsql::StatementType::kStmtUpdate == t) {
			hsql::UpdateStatement *stmt =
				get_result()->getStatement(0);
			where = stmt->where;
		} else if (hsql::StatementType::kStmtSelect == t) {
			hsql::SelectStatement *stmt =
				get_result()->getStatement(0);
			where = stmt->whereClause;
		} else if (hsql::StatementType::kStmtDelete == t) {
			hsql::DeleteStatement *stmt =
				get_result()->getStatement(0);
			where = stmt->expr;
		}

		if (!where)
			return false;

		if (where->type == kExprOperator && where->opType == kOpAnd) {
			where = where->expr;
		}

		if (where->type == kExprOperator &&
		    where->expr->type == kExprColumnRef) {
			if (strcmp(where->expr->name, key_name) == 0) {
				if (where->expr2) {
					switch (where->expr2->type) {
						case hsql::ExprType::kExprLiteralInt:
							*key = DTCValue::Make(
								where->expr2->ival);
							return true;
						case hsql::ExprType::kExprLiteralFloat:
							*key = DTCValue::Make(
								where->expr2->fval);
							return true;
						case hsql::ExprType::kExprLiteralString:
							*key = DTCValue::Make(
								where->expr2->name);
							return true;
						default:
							return false;
					}
				}
				return false;
			}
		}
	}

	return false;
}

uint32_t MyRequest::get_limit_start()
{
	return 0;
}

uint32_t MyRequest::get_limit_count()
{
	return 0;
}

uint32_t MyRequest::get_need_num_fields()
{
	int t = m_result.getStatement(0)->type();
	if (t != hsql::StatementType::kStmtSelect) {
		return 0;
	}
	hsql::SelectStatement *stmt = get_result()->getStatement(0);
	std::vector<hsql::Expr *> *selectList = stmt->selectList;
	log4cplus_debug("select size:%d", selectList->size());
	return selectList->size();
}

uint32_t MyRequest::get_update_num_fields()
{
	int t = m_result.getStatement(0)->type();
	if (hsql::StatementType::kStmtUpdate == t) {
		hsql::UpdateStatement *stmt = get_result()->getStatement(0);
		return stmt->updates->size();
	} else if (hsql::StatementType::kStmtInsert == t) {
		hsql::InsertStatement *stmt = get_result()->getStatement(0);
		return stmt->columns->size();
	}

	return 0;
}

std::vector<std::string> MyRequest::get_need_array()
{
	std::vector<std::string> need;
	int t = m_result.getStatement(0)->type();
	if (t != hsql::StatementType::kStmtSelect) {
		return need;
	}

	hsql::SelectStatement *stmt = get_result()->getStatement(0);
	std::vector<hsql::Expr *> *selectList = stmt->selectList;
	for (int i = 0; i < stmt->selectList->size(); i++) {
		need.push_back(stmt->selectList->at(i)->getName());
	}

	return need;
}

char* MyRequest::get_table_name()
{
	if (m_result.size() < 1)
		return NULL;

	int t = m_result.getStatement(0)->type();

	if (hsql::StatementType::kStmtInsert == t) {
		hsql::InsertStatement *stmt = get_result()->getStatement(0);
		if(stmt && stmt->tableName)
			return stmt->tableName;
	} else {
		if (hsql::StatementType::kStmtUpdate == t) {
			hsql::UpdateStatement *stmt =
				get_result()->getStatement(0);
			if(stmt && stmt->table)
				return stmt->table->name;
		} else if (hsql::StatementType::kStmtSelect == t) {
			hsql::SelectStatement *stmt =
				get_result()->getStatement(0);
			if(stmt && stmt->fromTable)
				return stmt->fromTable->name;
		} else if (hsql::StatementType::kStmtDelete == t) {
			hsql::DeleteStatement *stmt =
				get_result()->getStatement(0);
			if(stmt)
				return stmt->tableName;
		}
	}
	return NULL;
}