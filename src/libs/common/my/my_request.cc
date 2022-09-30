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
#include "../config/config.h"

using namespace hsql;

extern DTCConfig *g_dtc_config;

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
	int count = 0;
	if (*p == 0x0) {
		p++;
		input_packet_length--;
		count++;
	}

	if (*p == 0x01) {
		p++;
		input_packet_length--;
		count++;
	}
	if(count == 2)
	{
		log4cplus_debug("new version query request.");
		eof_packet_new = true;
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
			"check packet info error:%p %d, set packet info first please",
			this->raw, this->raw_len);
		return false;
	} else
		return true;
}

hsql::Expr* find_node(hsql::Expr* node, char* key_name)
{
	if(!node)
		return NULL;

	if (node->type == kExprOperator && node->opType == kOpAnd) 
	{
		hsql::Expr* t1 = find_node(node->expr, key_name);
		if(t1)
			return t1;
		hsql::Expr* t2 = find_node(node->expr2, key_name);
		if(t2)
			return t2;
	}
	else if(node->type == kExprOperator && node->opType == kOpEquals)
	{
		if(strcmp(node->expr->name, key_name) == 0)
		{
			return node->expr2;
		}
	}

	return NULL;
}

bool MyRequest::get_key(DTCValue *key, char *key_name)
{
	hsql::Expr *where = NULL;
	int t = m_result.getStatement(0)->type();

	if (hsql::StatementType::kStmtInsert == t) {
		hsql::InsertStatement *stmt = get_result()->getStatement(0);
		if(stmt->columns == NULL)
		{
			int i = 0;
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
		else
		{
			for (int i = 0; i < stmt->columns->size(); i++) 
			{
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

		hsql::Expr* node = find_node(where, key_name);
		if(node)
		{
			switch (node->type) 
			{
				case hsql::ExprType::kExprLiteralInt:
					*key = DTCValue::Make(
						node->ival);
					return true;
				case hsql::ExprType::kExprLiteralFloat:
					*key = DTCValue::Make(
						node->fval);
					return true;
				case hsql::ExprType::kExprLiteralString:
					*key = DTCValue::Make(
						node->name);
					return true;
				default:
					return false;
			}
		}
	}

	return false;
}

uint32_t MyRequest::get_limit_start()
{
	int t = m_result.getStatement(0)->type();
	if (t != hsql::StatementType::kStmtSelect) {
		return 0;
	}
	hsql::SelectStatement *stmt = get_result()->getStatement(0);
	LimitDescription* limit = stmt->limit;
	if(limit)
	{
		if(limit->offset)
		{
			int val = limit->offset->ival;
			log4cplus_debug("limit- offset: %d", val);
			if(val >= 0)
				return val;
		}
	}

	return 0;
}

uint32_t MyRequest::get_limit_count()
{
	int t = m_result.getStatement(0)->type();
	if (t != hsql::StatementType::kStmtSelect) {
		return 0;
	}
	hsql::SelectStatement *stmt = get_result()->getStatement(0);
	LimitDescription* limit = stmt->limit;
	if(limit)
	{
		if(limit->limit)
		{
			int val = limit->limit->ival;
			log4cplus_debug("limit- limit: %d", val);
			if(val >= 0)
				return val;
		}
	}

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
	if(selectList->size() == 1 && (*selectList)[0]->type == kExprStar)
		return g_dtc_config->get_config_node()["primary"]["cache"]["field"].size();
	else
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
		return stmt->values->size();
	}

	return 0;
}

std::vector<std::string> MyRequest::get_need_array()
{
	std::vector<std::string> need;
	int t = m_result.getStatement(0)->type();
	if (t != hsql::StatementType::kStmtSelect) {
		log4cplus_error("need array type: %d", t);
		return need;
	}

	hsql::SelectStatement *stmt = get_result()->getStatement(0);
	std::vector<hsql::Expr *> *selectList = stmt->selectList;

	if(selectList->size() == 1 && (*selectList)[0]->type == kExprStar)
	{
		int num = g_dtc_config->get_config_node()["primary"]["cache"]["field"].size();
		for(int i = 0; i < num; i++)
		{
			need.push_back(g_dtc_config->get_config_node()["primary"]["cache"]["field"][i]["name"].as<std::string>());
		}	
	}
	else
	{
		for (int i = 0; i < stmt->selectList->size(); i++) {
			need.push_back(stmt->selectList->at(i)->getName());
		}
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