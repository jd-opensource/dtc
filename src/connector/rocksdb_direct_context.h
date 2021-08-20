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
* 
*/
#ifndef __ROCKSDB_DIRECT_CONTEXT_H__
#define __ROCKSDB_DIRECT_CONTEXT_H__

#if 1

#include <stdint.h>
#include <string>
#include <vector>
#include <deque>
#include <assert.h>
#include <string.h>

static const uint16_t sgMagicNum = 12345; // global magic number

// operator must be matched with DTC with the same order
enum class ConditionOperation : uint8_t {
	EQ, // ==   0
	NE, // !=   1
	LT, // <    2
	LE, // <=   3
	GT, // >    4
	GE // >=    5
};

bool operator==(const ConditionOperation lc, const ConditionOperation rc)
{
	return (int)lc == (int)rc;
}

#pragma pack(push, 1)
struct QueryCond {
	uint8_t s_field_index;
	uint8_t s_cond_opr; // ConditionOperation
	std::string s_cond_value;

    private:
	int binary_size()
	{
		static int fix_header_len = sizeof(s_field_index) +
					    sizeof(s_cond_opr) +
					    sizeof(int) /* value len */;
		return fix_header_len + s_cond_value.length();
	}

	void serialize_to(char *&data)
	{
		*(uint8_t *)data = s_field_index;
		data += sizeof(uint8_t);

		*(uint8_t *)data = s_cond_opr;
		data += sizeof(uint8_t);

		*(int *)data = s_cond_value.length();
		data += sizeof(int);

		memmove((void *)data, (void *)s_cond_value.c_str(),
			s_cond_value.length());
		data += s_cond_value.length();
	}

	void serialize_from(const char *data, int &condLen)
	{
		const char *beg_pos = data;

		s_field_index = *(uint8_t *)data;
		data += sizeof(uint8_t);

		s_cond_opr = *(uint8_t *)data;
		data += sizeof(uint8_t);

		int len = *(int *)data;
		data += sizeof(int);

		s_cond_value.assign(data, len);
		condLen = data - beg_pos + len;
	}

	friend class DirectRequestContext;
};

struct LimitCond {
	int s_limit_start = -1;
	int s_limit_step = -1;

	void reset()
	{
		s_limit_start = -1, s_limit_step = -1;
	}
};

struct DirectRequestContext {
	uint16_t s_magic_num;
	uint64_t s_sequence_id;
	std::vector<QueryCond> s_field_conds;
	std::vector<std::pair<int, bool /* asc or not*/> > s_orderby_fields;
	LimitCond s_limit_cond;

	void reset()
	{
		s_magic_num = 0;
		s_sequence_id = 0;
		s_field_conds.clear();
		s_limit_cond.reset();
	}

	// binary format size for transporting in
	int binary_size()
	{
		static int fix_header_len = sizeof(s_magic_num) +
					    sizeof(s_sequence_id) +
					    sizeof(uint8_t) * 2;

		int len = fix_header_len;
		for (size_t idx = 0; idx < s_field_conds.size(); idx++) {
			len += s_field_conds[idx].binary_size();
		}

		for (size_t idx = 0; idx < s_orderby_fields.size(); idx++) {
			len += (sizeof(int) + sizeof(bool));
		}
		len += sizeof(LimitCond);

		return len;
	}

	// before call this function, should call 'binary_size' to evaluate the size of the struct
	void serialize_to(char *data, int len)
	{
		*(uint16_t *)data = s_magic_num;
		data += sizeof(uint16_t);

		*(uint64_t *)data = s_sequence_id;
		data += sizeof(uint64_t);

		*(uint8_t *)data = s_field_conds.size();
		data += sizeof(uint8_t);
		for (size_t idx = 0; idx < s_field_conds.size(); idx++) {
			s_field_conds[idx].serialize_to(data);
		}

		*(uint8_t *)data = s_orderby_fields.size();
		data += sizeof(uint8_t);
		for (size_t idx = 0; idx < s_orderby_fields.size(); idx++) {
			*(int *)data = s_orderby_fields[idx].first;
			data += sizeof(int);

			*(bool *)data = s_orderby_fields[idx].second;
			data += sizeof(bool);
		}

		memmove((void *)data, (void *)&s_limit_cond, sizeof(LimitCond));
		data += sizeof(LimitCond);
	}

	void serialize_from(const char *data, int data_len)
	{
		s_magic_num = *(uint16_t *)data;
		data += sizeof(uint16_t);
		data_len -= sizeof(uint16_t);

		s_sequence_id = *(uint64_t *)data;
		data += sizeof(uint64_t);
		data_len -= sizeof(uint64_t);

		uint8_t condNum = *(uint8_t *)data;
		data += sizeof(uint8_t);
		data_len -= sizeof(uint8_t);

		QueryCond cond;
		int condLen = 0;
		for (uint8_t idx = 0; idx < condNum; idx++) {
			cond.serialize_from(data, condLen);
			data += condLen;
			data_len -= condLen;

			s_field_conds.push_back(cond);
		}

		std::pair<int, bool> orPair;
		uint8_t orderNum = *(uint8_t *)data;
		data += sizeof(uint8_t);
		data_len -= sizeof(uint8_t);
		for (uint8_t idx = 0; idx < orderNum; idx++) {
			orPair.first = *(int *)data;
			data += sizeof(int);
			data_len -= sizeof(int);

			orPair.second = *(bool *)data;
			data += sizeof(bool);
			data_len -= sizeof(bool);

			s_orderby_fields.push_back(orPair);
		}

		memmove((void *)&s_limit_cond, (void *)data, sizeof(LimitCond));
		data_len -= sizeof(LimitCond);

		assert(data_len == 0);
	}
};

struct DirectResponseContext {
	uint16_t s_magic_num;
	uint64_t s_sequence_id;
	int16_t s_row_nums; // number of matched rows or errno
	std::deque<std::string> s_row_values;

	void serialize_to(std::string &data)
	{
		static int header_len =
			sizeof(uint16_t) + sizeof(uint64_t) + sizeof(int16_t);

		data.clear();
		data = std::move(std::string((char *)this, header_len));

		for (size_t idx = 0; idx < s_row_nums; idx++) {
			data.append(s_row_values.front());
			s_row_values.pop_front();
		}
	}

	void free()
	{
		s_magic_num = 0;
		s_sequence_id = 0;
		s_row_nums = -1;
		s_row_values.clear();
	}
};
#pragma pack(pop)

#endif

#endif // __ROCKSDB_DIRECT_CONTEXT_H__
