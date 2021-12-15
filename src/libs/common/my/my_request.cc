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

bool MyRequest::do_mysql_protocol_parse()
{
	char *p = this->raw;

	if (p == NULL || this->raw_len < MYSQL_HEADER_SIZE) {
		log4cplus_error("receive size small than package header.");
		return false;
	}

	int input_packet_length = uint3korr(p);
	log4cplus_debug("uint3korr:0x%x 0x%x 0x%x, len:%d", p[0], p[1], p[2],
			input_packet_length);
	p += 3;
	this->pkt_nr = (uint8_t)(*p); // mysql sequence id
	p++;
	log4cplus_debug("pkt_nr:%d, packet len:%d", this->pkt_nr,
			input_packet_length);

	if (sizeof(MYSQL_HEADER_SIZE) + input_packet_length > raw_len) {
		log4cplus_error(
			"the len %d at mysql header is different with len %d actually.",
			input_packet_length, raw_len);
		return false;
	}

	enum enum_server_command cmd = (enum enum_server_command)(uchar)p[0];
	if (cmd != COM_QUERY) {
		log4cplus_error("cmd type error:%d", cmd);
		return false;
	}

	input_packet_length -= 2;

	p += 2;

	if (*p == 0x01) {
		p++;
		input_packet_length--;
	}
	m_sql.assign(p, input_packet_length);
	log4cplus_debug("sql:%s", m_sql.c_str());

	return true;
}

bool MyRequest::load_sql()
{
	if (!check_packet_info())
		return false;

	if (!do_mysql_protocol_parse())
		return false;

	/*hsql::SQLParser::parse(m_sql, &m_result);
	if (result.isValid())
		return true;
	else {
		log4cplus_error("%s (Line %d:%d)", result.errorMsg(),
				result.errorLine(), result.errorColumn());
		return false;
	}*/

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