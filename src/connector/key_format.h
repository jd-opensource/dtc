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
#ifndef __KEY_FORMAT_H__
#define __KEY_FORMAT_H__

#include <string>
#include <sstream>
#include <stdint.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <string.h>
#include <map>
#include <vector>

#include "table/table_def.h"

class key_format {
    public:
	static std::string
	do_encode(const std::map<uint8_t, DTCValue *> &fieldValues,
		  const DTCTableDefinition *table_def,
		  uint64_t &caseSensitiveFreeLen);
	static std::string
	do_encode(const std::vector<std::string> &fieldValues,
		  const DTCTableDefinition *table_def,
		  uint64_t &caseSensitiveFreeLen);
	static void do_decode(const std::string &src,
			      std::vector<std::string> &fieldValues,
			      const DTCTableDefinition *table_def);

	static std::string
	do_encode(const std::vector<std::string> &fieldValues,
		  const std::vector<int> &fieldTypes);

	static void do_decode(const std::string &src,
			      const std::vector<int> &fieldTypes,
			      std::vector<std::string> &fieldValues);

	static void decode_primary_key(const std::string &src, int key_type,
				       std::string &pKey);

	static int get_format_key(const std::string &src, int field_type,
				  std::string &key);

	static int get_field_len(const char *src, int field_type);

	static int Compare(const std::string &ls, const std::string &rs,
			   const std::vector<int> &fieldTypes);

	// private:
	static std::string encode_bytes(const std::string &src);
	static std::string encode_bytes(int64_t src);
	static std::string encode_bytes(uint64_t src);
	static std::string encode_bytes(double src);
	static void DecodeBytes(const std::string &src, int64_t &dst);
	static void DecodeBytes(const std::string &src, std::string &dst);
	static void DecodeBytes(const std::string &src, uint64_t &dst);
	static void DecodeBytes(const std::string &src, double &dst);
};

#endif
