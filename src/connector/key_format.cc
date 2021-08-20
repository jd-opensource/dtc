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
#include <iostream>
#include "key_format.h"
#include "protocol.h"
#include "log/log.h"

#include <utility>

#define SEGMENT_SIZE 8

const std::string SEG_SYMBOL = "|";

const char ENCODER_MARKER = 127;
const uint64_t signMask = 0x8000000000000000;

uint64_t encode_into_cmp_uint(int64_t src)
{
	return uint64_t(src) ^ signMask;
}

uint64_t htonll(uint64_t val)
{
	return (((uint64_t)htonl(val)) << 32) + htonl(val >> 32);
}

uint64_t ntohll(uint64_t val)
{
	return (((uint64_t)ntohl(val)) << 32) + ntohl(val >> 32);
}

std::string
key_format::do_encode(const std::map<uint8_t, DTCValue *> &fieldValues,
		      const DTCTableDefinition *table_def,
		      uint64_t &caseSensitiveFreeLen)
{
	caseSensitiveFreeLen = 0;

	const uint8_t *uniq_fields = table_def->uniq_fields_list();
	std::string temp_field;
	std::string rockdb_key;
	for (uint8_t i = 0; i < table_def->uniq_fields(); i++) {
		std::map<uint8_t, DTCValue *>::const_iterator key_value =
			fieldValues.find(uniq_fields[i]);
		if (key_value != fieldValues.end()) {
			switch (table_def->field_type(uniq_fields[i])) {
			case DField::Signed:
				rockdb_key.append(encode_bytes(
					(int64_t)key_value->second->s64));
				// rockdb_key.append(SEG_SYMBOL);
				break;
			case DField::Unsigned:
				rockdb_key.append(encode_bytes(
					(uint64_t)key_value->second->u64));
				// rockdb_key.append(SEG_SYMBOL);
				break;
			case DField::Float:
				rockdb_key.append(encode_bytes(
					(double)key_value->second->flt));
				// rockdb_key.append(SEG_SYMBOL);
				break;
			case DField::Binary:
				rockdb_key.append(encode_bytes(std::string(
					key_value->second->bin.ptr,
					key_value->second->bin.len)));
				// rockdb_key.append(SEG_SYMBOL);
				break;
			case DField::String:
				temp_field = std::move(encode_bytes(std::string(
					key_value->second->str.ptr,
					key_value->second->str.len)));
				caseSensitiveFreeLen += temp_field.length();
				rockdb_key.append(std::move(temp_field));
				// rockdb_key.append(SEG_SYMBOL);
				// caseSensitiveFreeLen += 1;
				break;
			}
		}
	}
	return rockdb_key;
}

std::string key_format::do_encode(const std::vector<std::string> &fieldValues,
				  const DTCTableDefinition *table_def,
				  uint64_t &caseSensitiveFreeLen)
{
	caseSensitiveFreeLen = 0;

	const uint8_t *uniq_fields = table_def->uniq_fields_list();
	std::string temp_field;
	std::string rockdb_key;
	for (uint8_t i = 0; i < table_def->uniq_fields(); i++) {
		switch (table_def->field_type(uniq_fields[i])) {
		case DField::Signed:

			rockdb_key.append(encode_bytes((int64_t)strtoll(
				fieldValues[i].c_str(), NULL, 10)));
			// rockdb_key.append(SEG_SYMBOL);
			break;
		case DField::Unsigned:
			rockdb_key.append(encode_bytes((uint64_t)strtoull(
				fieldValues[i].c_str(), NULL, 10)));
			// rockdb_key.append(SEG_SYMBOL);
			break;
		case DField::Float:
			rockdb_key.append(encode_bytes(
				strtod(fieldValues[i].c_str(), NULL)));
			// rockdb_key.append(SEG_SYMBOL);
			break;
		case DField::Binary:
			rockdb_key.append(encode_bytes(fieldValues[i]));
			// rockdb_key.append(SEG_SYMBOL);
		case DField::String:
			temp_field = std::move(encode_bytes(fieldValues[i]));
			caseSensitiveFreeLen += temp_field.length();
			rockdb_key.append(std::move(temp_field));
			// rockdb_key.append(SEG_SYMBOL);
			// caseSensitiveFreeLen += 1;
			break;
		}
	}
	return rockdb_key;
}

std::string key_format::do_encode(const std::vector<std::string> &fieldValues,
				  const std::vector<int> &fieldTypes)
{
	std::string temp_field;
	std::string rockdb_key;
	log4cplus_info("fieldTypes size:%d", fieldTypes.size());
	for (size_t i = 0; i < fieldTypes.size(); i++) {
		switch (fieldTypes[i]) {
		case DField::Signed:

			rockdb_key.append(encode_bytes((int64_t)strtoll(
				fieldValues[i].c_str(), NULL, 10)));
			// rockdb_key.append(SEG_SYMBOL);
			break;
		case DField::Unsigned:
			rockdb_key.append(encode_bytes((uint64_t)strtoull(
				fieldValues[i].c_str(), NULL, 10)));
			// rockdb_key.append(SEG_SYMBOL);
			break;
		case DField::Float:
			rockdb_key.append(encode_bytes(
				strtod(fieldValues[i].c_str(), NULL)));
			// rockdb_key.append(SEG_SYMBOL);
			break;
		case DField::Binary:
		case DField::String:
			rockdb_key.append(encode_bytes(fieldValues[i]));
			// rockdb_key.append(SEG_SYMBOL);
			break;
		}
	}
	return rockdb_key;
}

void key_format::do_decode(const std::string &src,
			   const std::vector<int> &fieldTypes,
			   std::vector<std::string> &fieldValues)
{
	fieldValues.clear();

	size_t pos = 0;
	for (size_t i = 0; i < fieldTypes.size(); i++) {
		std::string value;
		switch (fieldTypes[i]) {
		case DField::Signed:
			int64_t s64;
			DecodeBytes(src.substr(pos, 8), s64);
			pos += 8;
			// value = new DTCValue(s64);
			value = std::to_string(s64);
			break;
		case DField::Unsigned:
			uint64_t u64;
			DecodeBytes(src.substr(pos, 8), u64);
			pos += 8;
			// value = new DTCValue(u64);
			value = std::to_string(u64);
			break;
		case DField::Float:
			double d64;
			DecodeBytes(src.substr(pos, 8), d64);
			pos += 8;
			// value = new DTCValue(d64);
			value = std::to_string(d64);
			break;
		case DField::Binary:
		case DField::String:
			size_t begin_pos = pos;
			pos += SEGMENT_SIZE;
			// std::string str;
			// for (; src[ pos - 1] == ENCODER_MARKER &&  src[pos] != SEG_SYMBOL[0] && pos < src.length() ;  pos += SEGMENT_SIZE) {
			// }
			for (; src[pos - 1] == ENCODER_MARKER;
			     pos += SEGMENT_SIZE) {
			}
			// value = new DTCValue(str.c_str(), str.length());
			DecodeBytes(src.substr(begin_pos, pos - begin_pos),
				    value);
			// pos++;
			// value = std::move(str);
			break;
		}
		// fieldValues[uniq_fields[i]] = value;
		fieldValues.push_back(value);
	}
}

void key_format::decode_primary_key(const std::string &src, int key_type,
				    std::string &pKey)
{
	switch (key_type) {
	default:
		log4cplus_error("unsupport data type! type:%d", key_type);
		break;
	case DField::Signed:
		int64_t s64;
		DecodeBytes(src.substr(0, 8), s64);
		pKey = std::to_string(s64);
		break;
	case DField::Unsigned:
		uint64_t u64;
		DecodeBytes(src.substr(0, 8), u64);
		pKey = std::to_string(u64);
		break;
	case DField::Float:
		double d64;
		DecodeBytes(src.substr(0, 8), d64);
		pKey = std::to_string(d64);
		break;
	case DField::Binary:
	case DField::String:
		size_t pos = 0;
		pos += SEGMENT_SIZE;
		for (; src[pos - 1] == ENCODER_MARKER; pos += SEGMENT_SIZE) {
		}
		DecodeBytes(src.substr(0, pos), pKey);
		break;
	}

	return;
}

int key_format::get_field_len(const char *src, int field_type)
{
	int ret = -1;
	switch (field_type) {
	default:
		log4cplus_error("unsupport data type! type:%d", field_type);
		break;
	case DField::Signed:
	case DField::Unsigned:
	case DField::Float:
		ret = 8;
		break;
	case DField::Binary:
	case DField::String:
		size_t pos = 0;
		pos += SEGMENT_SIZE;
		for (; src[pos - 1] == ENCODER_MARKER; pos += SEGMENT_SIZE) {
		}
		ret = pos;
		break;
	}

	return ret;
}

// get the first field in the row with encode format
int key_format::get_format_key(const std::string &src, int field_type,
			       std::string &key)
{
	int ret = 0;
	switch (field_type) {
	default:
		ret = -1;
		log4cplus_error("unsupport data type! type:%d", field_type);
		break;
	case DField::Signed:
	case DField::Unsigned:
	case DField::Float:
		key = src.substr(0, 8);
		break;
	case DField::Binary:
	case DField::String:
		size_t pos = SEGMENT_SIZE;
		for (; src[pos - 1] == ENCODER_MARKER; pos += SEGMENT_SIZE) {
		}
		key = src.substr(0, pos);
		break;
	}

	return ret;
}

// compare all the field one by one with its explicit type
int key_format::Compare(const std::string &ls, const std::string &rs,
			const std::vector<int> &fieldTypes)
{
	int ret, type, lFieldLen, rFieldLen, compLen;
	char *lHead = (char *)ls.data();
	char *rHead = (char *)rs.data();
	for (size_t idx = 0; idx < fieldTypes.size(); idx++) {
		type = fieldTypes[idx];
		switch (type) {
		default:
			ret = -2;
			log4cplus_error("unsupport data type! type:%d", type);
			break;
		case DField::Signed:
		case DField::Unsigned:
		case DField::Float:
			lFieldLen = rFieldLen = 8;
			break;
		case DField::Binary:
		case DField::String:
			lFieldLen = get_field_len(lHead, type);
			rFieldLen = get_field_len(rHead, type);
			break;
		}

		compLen = lFieldLen > rFieldLen ? rFieldLen : lFieldLen;
		if (type == DField::String) {
			// the case insensitive
			int my_strn_case_cmp(const char *, const char *,
					     size_t);
			ret = my_strn_case_cmp(lHead, rHead, compLen);
		} else {
			// case sensitive compare
			ret = memcmp((void *)lHead, (void *)rHead, compLen);
		}

		if (ret != 0)
			return ret;
		else if (lFieldLen != rFieldLen)
			return lFieldLen < rFieldLen ? -1 : 1;

		// equal in the current field
		lHead += compLen;
		rHead += compLen;
	}

	return 0;
}

void key_format::do_decode(const std::string &src,
			   std::vector<std::string> &fieldValues,
			   const DTCTableDefinition *table_def)
{
	fieldValues.clear();
	const uint8_t *uniq_fields = table_def->uniq_fields_list();

	size_t pos = 0;
	for (uint8_t i = 0; i < table_def->uniq_fields(); i++) {
		// DTCValue *value = NULL;
		std::string value;
		switch (table_def->field_type(uniq_fields[i])) {
		case DField::Signed:
			int64_t s64;
			DecodeBytes(src.substr(pos, 8), s64);
			pos += 8;
			// value = new DTCValue(s64);
			value = std::to_string(s64);
			break;
		case DField::Unsigned:
			uint64_t u64;
			DecodeBytes(src.substr(pos, 8), u64);
			pos += 8;
			// value = new DTCValue(u64);
			value = std::to_string(u64);
			break;
		case DField::Float:
			double d64;
			DecodeBytes(src.substr(pos, 8), d64);
			pos += 8;
			// value = new DTCValue(d64);
			value = std::to_string(d64);
			break;
		case DField::Binary:
		case DField::String:
			size_t begin_pos = pos;
			pos += SEGMENT_SIZE;
			for (; src[pos - 1] == ENCODER_MARKER;
			     pos += SEGMENT_SIZE) {
			}
			// value = new DTCValue(str.c_str(), str.length());
			DecodeBytes(src.substr(begin_pos, pos - begin_pos),
				    value);
			break;
		}
		// fieldValues[uniq_fields[i]] = value;
		fieldValues.push_back(value);
	}
}

std::string key_format::encode_bytes(const std::string &src)
{
	unsigned char padding_bytes;
	size_t left_length = src.length();
	size_t pos = 0;
	std::stringstream oss_dst;
	while (true) {
		unsigned char copy_len = SEGMENT_SIZE - 1 < left_length ?
						 SEGMENT_SIZE - 1 :
						 left_length;
		padding_bytes = SEGMENT_SIZE - 1 - copy_len;
		oss_dst << src.substr(pos, copy_len);
		pos += copy_len;
		left_length -= copy_len;

		if (padding_bytes) {
			oss_dst << std::string(padding_bytes, '\0');
			oss_dst << (char)(ENCODER_MARKER - padding_bytes);
			break;
		} else {
			oss_dst << ENCODER_MARKER;
		}
	}
	return oss_dst.str();
}

std::string key_format::encode_bytes(int64_t src)
{
	uint64_t host_bytes = encode_into_cmp_uint(src);
	uint64_t net_bytes = htonll(host_bytes);
	char dst_bytes[8];
	memcpy(dst_bytes, &net_bytes, sizeof(uint64_t));
	std::string dst = std::string(8, '\0');
	for (size_t i = 0; i < dst.length(); i++) {
		dst[i] = dst_bytes[i];
	}
	return dst;
}

std::string key_format::encode_bytes(double src)
{
	uint64_t u;
	memcpy(&u, &src, sizeof(double));
	if (src >= 0) {
		u |= signMask;
	} else {
		u = ~u;
	}

	return encode_bytes(u);
}

std::string key_format::encode_bytes(uint64_t src)
{
	uint64_t net_bytes = htonll(src);
	char dst_bytes[8];
	memcpy(dst_bytes, &net_bytes, sizeof(uint64_t));
	std::string dst = std::string(8, '\0');
	for (size_t i = 0; i < dst.length(); i++) {
		dst[i] = dst_bytes[i];
	}
	return dst;
}

void key_format::DecodeBytes(const std::string &src, int64_t &dst)
{
	uint64_t net_bytes;
	memcpy(&net_bytes, src.c_str(), sizeof(uint64_t));
	uint64_t host_bytes = ntohll(net_bytes);
	dst = int64_t(host_bytes ^ signMask);
}

void key_format::DecodeBytes(const std::string &src, std::string &dst)
{
	if (src.length() == 0) {
		dst = "";
	}
	std::stringstream oss_dst;
	for (size_t i = 0; i < src.length(); i += SEGMENT_SIZE) {
		char padding_bytes = ENCODER_MARKER - src[i + 7];
		oss_dst << src.substr(i, SEGMENT_SIZE - 1 - padding_bytes);
	}
	dst = oss_dst.str();
}

void key_format::DecodeBytes(const std::string &src, uint64_t &dst)
{
	uint64_t net_bytes;
	memcpy(&net_bytes, src.c_str(), sizeof(uint64_t));
	dst = ntohll(net_bytes);
}

void key_format::DecodeBytes(const std::string &src, double &dst)
{
	uint64_t u;
	DecodeBytes(src, u);

	if ((u & signMask) > 0) {
		u &= (~signMask);
	} else {
		u = ~u;
	}
	memcpy(&dst, &u, sizeof(dst));
}
