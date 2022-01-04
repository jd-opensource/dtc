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
#ifndef __MY_REQUEST_H__
#define __MY_REQUEST_H__
#include "my_comm.h"
#include <string>
#include "../../common/value.h"

#include "../../hsql/include/SQLParser.h"
#include "../../hsql/include/util/sqlhelper.h"

class MyRequest {
    public:
	MyRequest() : raw(NULL), raw_len(0), pkt_nr(0)
	{
	}

	void set_packet_info(char *packet, int packet_len)
	{
		this->raw = packet;
		this->raw_len = packet_len;
	}

	int get_packet_len()
	{
		return raw_len;
	}

	char *get_packet_ptr()
	{
		return raw;
	}

	// parse mysql protocol, get sql string.
	bool do_mysql_protocol_parse();
	bool load_sql();
	bool check_packet_info();

	//sequence id
	void set_pkt_nr(uint64_t pkt_nr)
	{
		this->pkt_nr = pkt_nr;
	}

	uint64_t get_pkt_nr()
	{
		return this->pkt_nr;
	}

	hsql::SQLParserResult *get_result()
	{
		return &m_result;
	}

	std::string get_sql()
	{
		return m_sql;
	}

	bool get_key(DTCValue *key);
	uint32_t get_limit_start();
	uint32_t get_limit_count();
	uint32_t get_need_num_fields();
	std::vector<std::string> get_need_array();

	uint32_t get_condition_num_fields();
	uint32_t get_update_num_fields();

    public:
	char *raw;
	int raw_len;
	std::string m_sql;
	hsql::SQLParserResult m_result;
	uint64_t pkt_nr;
};

#endif