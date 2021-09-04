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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <bitset>
#include <map>
#include <string>
#include <algorithm>

#include "db_process_rocks.h"
#include <protocol.h>
#include <log/log.h>

#include "proc_title.h"
#include "table/table_def_manager.h"

#include "buffer/buffer_pond.h"
#include <daemon/daemon.h>
#include "mysql_error.h"
#include <sstream>
#include <fstream>

// #define DEBUG_INFO
#define PRINT_STAT
#define BITS_OF_BYTE 8 /* bits of one byte */
#define MAX_REPLICATE_LEN (1UL << 20)
static const std::string gReplicatePrefixKey = "__ROCKS_REPLICAE_PREFIX_KEY__";
CommKeyComparator gInternalComparator;

RocksdbProcess::RocksdbProcess(RocksDBConn *conn)
{
	db_connect = conn;

	strncpy(name, "helper", 6);
	title_prefix_size = 0;
	proc_timeout = 0;

	table_define = NULL;
	m_compound_key_field_nums = -1;
	m_extra_value_field_nums = -1;

	m_no_bitmap_key = true;

	m_prev_migrate_key = "";
	m_current_migrate_key = "";
	m_uncommited_mig_id = -1;

	m_order_by_unit = NULL;
}

RocksdbProcess::~RocksdbProcess()
{
}

int RocksdbProcess::check_table()
{
	// no table concept in rocksdb
	return (0);
}

void RocksdbProcess::init_ping_timeout(void)
{
	// only for frame adapt
	return;
}

void RocksdbProcess::use_matched_rows(void)
{
	// only for frame adapt, no actual meanings
	return;
}

int RocksdbProcess::do_init(int group_id, const DbConfig *Config,
			    DTCTableDefinition *tdef, int slave)
{
	int ret;

	self_group_id = group_id;
	table_define = tdef;

	std::vector<int> dtcFieldIndex;
	int totalFields = table_define->num_fields();
	for (int i = 0; i <= totalFields; i++) {
		//bug fix volatile不在db中
		if (table_define->is_volatile(i))
			continue;
		dtcFieldIndex.push_back(i);
	}

	totalFields = dtcFieldIndex.size();
	if (totalFields <= 0) {
		log4cplus_error("field can not be empty!");
		return -1;
	}

	m_compound_key_field_nums = table_define->uniq_fields();
	if (m_compound_key_field_nums <= 0) {
		log4cplus_error("not found unique constraint in any field!");
		return -1;
	}
	m_extra_value_field_nums = totalFields - m_compound_key_field_nums;
	log4cplus_info("total fields:%d, uniqKeyNum:%d, valueNum:%d",
		       totalFields, m_compound_key_field_nums,
		       m_extra_value_field_nums);

	// create map relationship
	uint8_t keyIndex;
	uint8_t *uniqFields = table_define->uniq_fields_list();
	for (int idx = 0; idx < m_compound_key_field_nums; idx++) {
		keyIndex = *(uniqFields + idx);
		dtcFieldIndex[keyIndex] = -1;
		m_field_index_mapping.push_back(keyIndex);
	}

	if (dtcFieldIndex.size() <= 0) {
		log4cplus_error("no value field!");
		return -1;
	}

	// classify the unique keys into two types: Integer fixed len and elastic string type,
	// no need to do collecting if the key is binary
	mKeyfield_types.resize(m_compound_key_field_nums);

	{
		// shrink string keys or integer keys into the head of the array
		int field_type;
		// int moveHeadIdx = -1;
		for (size_t idx = 0; idx < m_field_index_mapping.size();
		     idx++) {
			field_type = table_define->field_type(
				m_field_index_mapping[idx]);
			mKeyfield_types[idx] = field_type;
			log4cplus_info("fieldId:%d, field_type:%d",
				       m_field_index_mapping[idx], field_type);
			switch (field_type) {
			case DField::Signed:
			case DField::Unsigned:
			case DField::Float:
			case DField::Binary:
				break;

			case DField::String: {
				m_no_bitmap_key = false;
				break;
			}

			default:
				log4cplus_error(
					"unexpected field type! type:%d",
					field_type);
				return -1;
			};
		}
	}

	// remove key from vector
	auto start = std::remove_if(dtcFieldIndex.begin(), dtcFieldIndex.end(),
				    [](const int idx) { return idx == -1; });
	dtcFieldIndex.erase(start, dtcFieldIndex.end());

	// append value maps
	m_field_index_mapping.insert(m_field_index_mapping.end(),
				     dtcFieldIndex.begin(),
				     dtcFieldIndex.end());

	{
		m_reverse_field_index_mapping.resize(
			m_field_index_mapping.size());
		for (size_t idx1 = 0; idx1 < m_field_index_mapping.size();
		     idx1++) {
			m_reverse_field_index_mapping
				[m_field_index_mapping[idx1]] = idx1;
		}
	}

	// init replication tag key
	ret = get_replicate_end_key();

	std::stringstream ss;
	ss << "rocks helper meta info, keysize:" << m_compound_key_field_nums
	   << " valuesize:" << m_extra_value_field_nums << " rocksdb fields:[";
	for (size_t idx = 0; idx < m_field_index_mapping.size(); idx++) {
		log4cplus_info("%d, type:%d", m_field_index_mapping[idx],
			       idx < m_compound_key_field_nums ?
				       mKeyfield_types[idx] :
				       -1);
		if (idx == 0)
			ss << m_field_index_mapping[idx];
		else
			ss << ", " << m_field_index_mapping[idx];
	}
	ss << "]";
	log4cplus_info("%s", ss.str().c_str());

	return ret;
}

int RocksdbProcess::get_replicate_end_key()
{
	return 0;
	std::string value;
	std::string fullKey = gReplicatePrefixKey;
	int ret = db_connect->get_entry(fullKey, value,
					RocksDBConn::COLUMN_META_DATA);
	if (ret < 0 && ret != -RocksDBConn::ERROR_KEY_NOT_FOUND) {
		log4cplus_error("query replicate end key failed! ret:%d", ret);
		return -1;
	} else {
		m_repl_end_key = value;
	}

	return 0;
}

inline int RocksdbProcess::value_add_to_str(const DTCValue *additionValue,
					    int ifield_type,
					    std::string &baseValue)
{
	log4cplus_debug("value_add_to_str ifield_type[%d]", ifield_type);

	if (additionValue == NULL) {
		log4cplus_error("value can not be null!");
		return -1;
	}

	switch (ifield_type) {
	case DField::Signed: {
		long long va = strtoll(baseValue.c_str(), NULL, 10);
		va += (long long)additionValue->s64;
		baseValue = std::to_string(va);
		break;
	}

	case DField::Unsigned: {
		unsigned long long va = strtoull(baseValue.c_str(), NULL, 10);
		va += (unsigned long long)additionValue->u64;
		baseValue = std::to_string(va);
		break;
	}

	case DField::Float: {
		double va = strtod(baseValue.c_str(), NULL);
		va += additionValue->flt;
		baseValue = std::to_string(va);
		break;
	}

	case DField::String:
	case DField::Binary:
		log4cplus_error("string type can not do add operation!");
		break;

	default:
		log4cplus_error("unexpected field type! type:%d", ifield_type);
		return -1;
	};

	return 0;
}

inline int RocksdbProcess::value_to_str(const DTCValue *Value, int fieldId,
					std::string &strValue)
{
	const DTCValue *defaultValue;
	bool valueNull = false;

	if (Value == NULL) {
		log4cplus_info("value is null, use user default value!");
		defaultValue = table_define->default_value(fieldId);
		valueNull = true;
	}

	int ifield_type = table_define->field_type(fieldId);
	{
		switch (ifield_type) {
		case DField::Signed: {
			int64_t val;
			if (valueNull)
				val = defaultValue->s64;
			else
				val = Value->s64;
			strValue = std::move(std::to_string((long long)val));
			break;
		}

		case DField::Unsigned: {
			uint64_t val;
			if (valueNull)
				val = defaultValue->u64;
			else
				val = Value->u64;
			strValue = std::move(
				std::to_string((unsigned long long)val));
			break;
		}

		case DField::Float: {
			double val;
			if (valueNull)
				val = defaultValue->flt;
			else
				val = Value->flt;
			strValue = std::move(std::to_string(val));
			break;
		}

		case DField::String:
		case DField::Binary: {
			// value whether be "" or NULL ????
			// in current, regard NULL as empty string, not support NULL attribute here
			if (valueNull)
				strValue = std::move(
					std::string(defaultValue->str.ptr,
						    defaultValue->str.len));
			else {
				if (Value->str.is_empty()) {
					log4cplus_info("empty str value!");
					strValue = "";
					return 0;
				}

				strValue = std::move(std::string(
					Value->str.ptr, Value->str.len));
				/*if ( mkey_type == DField::String )
            {
          // case insensitive
          std::transform(strValue.begin(), strValue.end(), strValue.begin(), ::tolower);
          }*/
			}
			break;
		}
		default:
			log4cplus_error("unexpected field type! type:%d",
					ifield_type);
			return -1;
		};
	}

	return 0;
}

inline int RocksdbProcess::set_default_value(int field_type, DTCValue &Value)
{
	switch (field_type) {
	case DField::Signed:
		Value.s64 = 0;
		break;

	case DField::Unsigned:
		Value.u64 = 0;
		break;

	case DField::Float:
		Value.flt = 0.0;
		break;

	case DField::String:
		Value.str.len = 0;
		Value.str.ptr = 0;
		break;

	case DField::Binary:
		Value.bin.len = 0;
		Value.bin.ptr = 0;
		break;

	default:
		Value.s64 = 0;
	};

	return (0);
}

inline int RocksdbProcess::str_to_value(const std::string &Str, int field_type,
					DTCValue &Value)
{
	if (Str == NULL) {
		log4cplus_debug(
			"Str is NULL, field_type: %d. Check mysql table definition.",
			field_type);
		set_default_value(field_type, Value);
		return (0);
	}

	switch (field_type) {
	case DField::Signed:
		errno = 0;
		Value.s64 = strtoll(Str.c_str(), NULL, 10);
		if (errno != 0)
			return (-1);
		break;

	case DField::Unsigned:
		errno = 0;
		Value.u64 = strtoull(Str.c_str(), NULL, 10);
		if (errno != 0)
			return (-1);
		break;

	case DField::Float:
		errno = 0;
		Value.flt = strtod(Str.c_str(), NULL);
		if (errno != 0)
			return (-1);
		break;

	case DField::String:
		Value.str.len = Str.length();
		Value.str.ptr = const_cast<char *>(
			Str.data()); // 不重新new，要等这个value使用完后释放内存(如果Str是动态分配的)
		break;

	case DField::Binary:
		Value.bin.len = Str.length();
		Value.bin.ptr = const_cast<char *>(Str.data());
		break;

	default:
		log4cplus_error("type[%d] invalid.", field_type);
		return -1;
	}

	return 0;
}

int RocksdbProcess::condition_filter(const std::string &rocksValue, int fieldid,
				     int field_type,
				     const DTCFieldValue *condition)
{
	if (condition == NULL)
		return 0;

	bool matched;
	// find out the condition value
	for (int idx = 0; idx < condition->num_fields(); idx++) {
		if (table_define->is_volatile(idx)) {
			log4cplus_error("volatile field, idx:%d", idx);
			return -1;
		}

		int fId = condition->field_id(idx);
		if (fId != fieldid)
			continue;

		// DTC support query condition
		/* enum {
      EQ = 0,
      NE = 1,
      LT = 2,
      LE = 3,
      GT = 4,
      GE = 5,
      TotalComparison
    }; */
		int comparator = condition->field_operation(idx);
		const DTCValue *condValue = condition->field_value(idx);

		switch (field_type) {
		case DField::Signed:
			// matched = is_matched_template(strtoll(rocksValue.c_str(), NULL, 10), comparator, condValue.s64);
			matched = is_matched<int64_t>(
				strtoll(rocksValue.c_str(), NULL, 10),
				comparator, condValue->s64);
			if (!matched) {
				log4cplus_info(
					"not match the condition, lv:%s, rv:%lld, com:%d",
					rocksValue.c_str(),
					(long long)condValue->s64, comparator);
				return 1;
			}
			break;

		case DField::Unsigned:
			// matched = is_matched_template(strtoull(rocksValue.c_str(), NULL, 10), comparator, condValue.u64);
			matched = is_matched<uint64_t>(
				strtoull(rocksValue.c_str(), NULL, 10),
				comparator, condValue->u64);
			if (!matched) {
				log4cplus_info(
					"not match the condition, lv:%s, rv:%llu, com:%d",
					rocksValue.c_str(),
					(unsigned long long)condValue->u64,
					comparator);
				return 1;
			}
			break;

		case DField::Float:
			// matched = is_matched_template(strtod(rocksValue.c_str(), NULL, 10), comparator, condValue.flt);
			matched = is_matched<double>(
				strtod(rocksValue.c_str(), NULL), comparator,
				condValue->flt);
			if (!matched) {
				log4cplus_info(
					"not match the condition, lv:%s, rv:%lf, com:%d",
					rocksValue.c_str(), condValue->flt,
					comparator);
				return 1;
			}
			break;

		case DField::String:
			matched = is_matched(rocksValue.c_str(), comparator,
					     condValue->str.ptr,
					     (int)rocksValue.length(),
					     condValue->str.len, false);
			if (!matched) {
				log4cplus_info(
					"not match the condition, lv:%s, rv:%s, com:%d",
					rocksValue.c_str(),
					std::string(condValue->str.ptr,
						    condValue->str.len)
						.c_str(),
					comparator);
				return 1;
			}
		case DField::Binary:
			// matched = is_matched_template(rocksValue.c_str(), comparator, condValue.str.ptr, (int)rocksValue.length(), condValue.str.len);
			matched = is_matched(rocksValue.c_str(), comparator,
					     condValue->bin.ptr,
					     (int)rocksValue.length(),
					     condValue->bin.len, true);
			if (!matched) {
				log4cplus_info(
					"not match the condition, lv:%s, rv:%s, com:%d",
					rocksValue.c_str(),
					std::string(condValue->bin.ptr,
						    condValue->bin.len)
						.c_str(),
					comparator);
				return 1;
			}
			break;

		default:
			log4cplus_error("field[%d] type[%d] invalid.", fieldid,
					field_type);
			return -1;
		}
	}

	return 0;
}

int RocksdbProcess::condition_filter(const std::string &rocksValue,
				     const std::string &condValue,
				     int field_type, int comparator)
{
	bool matched;
	switch (field_type) {
	case DField::Signed:
		matched = is_matched<int64_t>(
			strtoll(rocksValue.c_str(), NULL, 10), comparator,
			strtoll(condValue.c_str(), NULL, 10));
		if (!matched) {
			log4cplus_info(
				"not match the condition, lv:%s, rv:%s, com:%d",
				rocksValue.c_str(), condValue.c_str(),
				comparator);
			return 1;
		}
		break;

	case DField::Unsigned:
		matched = is_matched<uint64_t>(
			strtoull(rocksValue.c_str(), NULL, 10), comparator,
			strtoull(condValue.c_str(), NULL, 10));
		if (!matched) {
			log4cplus_info(
				"not match the condition, lv:%s, rv:%s, com:%d",
				rocksValue.c_str(), condValue.c_str(),
				comparator);
			return 1;
		}
		break;

	case DField::Float:
		matched = is_matched<double>(strtod(rocksValue.c_str(), NULL),
					     comparator,
					     strtod(condValue.c_str(), NULL));
		if (!matched) {
			log4cplus_info(
				"not match the condition, lv:%s, rv:%s, com:%d",
				rocksValue.c_str(), condValue.c_str(),
				comparator);
			return 1;
		}
		break;

	case DField::String:
	case DField::Binary:
		matched =
			is_matched(rocksValue.c_str(), comparator,
				   condValue.c_str(), (int)rocksValue.length(),
				   (int)condValue.length(), false);
		if (!matched) {
			log4cplus_info(
				"not match the condition, lv:%s, rv:%s, com:%d",
				rocksValue.c_str(), condValue.c_str(),
				comparator);
			return 1;
		}
		break;

	default:
		log4cplus_error("invalid field type[%d].", field_type);
		return -1;
	}

	return 0;
}

template <class... Args> bool RocksdbProcess::is_matched_template(Args... len)
{
	return is_matched(len...);
}

template <class T>
bool RocksdbProcess::is_matched(const T lv, int comparator, const T rv)
{
	/* enum {
     EQ = 0,
     NE = 1,
     LT = 2,
     LE = 3,
     GT = 4,
     GE = 5,
     TotalComparison
     }; */
	switch (comparator) {
	case 0:
		return lv == rv;
	case 1:
		return lv != rv;
	case 2:
		return lv < rv;
	case 3:
		return lv <= rv;
	case 4:
		return lv > rv;
	case 5:
		return lv >= rv;
	default:
		log4cplus_error("unsupport comparator:%d", comparator);
	}

	return false;
}
template bool RocksdbProcess::is_matched<int64_t>(const int64_t lv, int comp,
						  const int64_t rv);
template bool RocksdbProcess::is_matched<uint64_t>(const uint64_t lv, int comp,
						   const uint64_t rv);
template bool RocksdbProcess::is_matched<double>(const double lv, int comp,
						 const double rv);

//template<>
bool RocksdbProcess::is_matched(const char *lv, int comparator, const char *rv,
				int lLen, int rLen, bool caseSensitive)
{
	/* enum {
     EQ = 0,
     NE = 1,
     LT = 2,
     LE = 3,
     GT = 4,
     GE = 5,
     TotalComparison
     }; */
	int ret;
	int min_len = lLen <= rLen ? lLen : rLen;
	switch (comparator) {
	case 0:
		if (caseSensitive)
			return lLen == rLen && !strncmp(lv, rv, min_len);
		return lLen == rLen && !strncasecmp(lv, rv, min_len);
	case 1:
		if (lLen != rLen)
			return true;
		if (caseSensitive)
			return strncmp(lv, rv, min_len);
		return strncasecmp(lv, rv, min_len);
	case 2:
		if (caseSensitive)
			ret = strncmp(lv, rv, min_len);
		else
			ret = strncasecmp(lv, rv, min_len);
		return ret < 0 || (ret == 0 && lLen < rLen);
	case 3:
		if (caseSensitive)
			ret = strncmp(lv, rv, min_len);
		else
			ret = strncasecmp(lv, rv, min_len);
		return ret < 0 || (ret == 0 && lLen <= rLen);
	case 4:
		if (caseSensitive)
			ret = strncmp(lv, rv, min_len);
		else
			ret = strncasecmp(lv, rv, min_len);
		return ret > 0 || (ret == 0 && lLen > rLen);
	case 5:
		if (caseSensitive)
			ret = strncmp(lv, rv, min_len);
		else
			ret = strncasecmp(lv, rv, min_len);
		return ret > 0 || (ret == 0 && lLen >= rLen);
	default:
		log4cplus_error("unsupport comparator:%d", comparator);
	}

	return false;
}

int RocksdbProcess::save_row(const std::string &compoundKey,
			     const std::string &compoundValue, bool countOnly,
			     int &totalRows, DtcJob *Job)
{
	if (m_compound_key_field_nums + m_extra_value_field_nums <= 0) {
		log4cplus_error("no fields in the table! key:%s");
		return (-1);
	}

	int ret;
	// decode the compoundKey and check whether it is matched
	std::vector<std::string> keys;
	key_format::do_decode(compoundKey, mKeyfield_types, keys);
	if (keys.size() != m_compound_key_field_nums) {
		// unmatched row
		log4cplus_error(
			"unmatched row, fullKey:%s, keyNum:%lu, definitionFieldNum:%d",
			compoundKey.c_str(), keys.size(),
			m_compound_key_field_nums);
		return -1;
	}

	if (countOnly) {
		totalRows++;
		return 0;
	}

	// decode key bitmap
	int bitmapLen = 0;
	decodeBitmapKeys(compoundValue, keys, bitmapLen);

	//DBConn.Row[0]是key的值，table_define->Field[0]也是key，
	//因此从1开始。而结果Row是从0开始的(不包括key)
	RowValue *row = new RowValue(table_define);
	const DTCFieldValue *Condition = Job->request_condition();
	std::string fieldValue;
	char *valueHead = const_cast<char *>(compoundValue.data()) + bitmapLen;
	for (size_t idx = 0; idx < m_field_index_mapping.size(); idx++) {
		int fieldId = m_field_index_mapping[idx];
		if (idx < m_compound_key_field_nums) {
			fieldValue = keys[idx];
		} else {
			ret = get_value_by_id(valueHead, fieldId, fieldValue);
			if (ret != 0) {
				log4cplus_error(
					"parse field value failed! compoundValue:%s",
					compoundValue.c_str());
				delete row;
				return -1;
			}
		}
		log4cplus_info("save row, fieldId:%d, val:%s", fieldId,
			       fieldValue.data());

		// do condition filter
		ret = condition_filter(fieldValue, fieldId,
				       table_define->field_type(fieldId),
				       Condition);
		if (ret < 0) {
			delete row;
			log4cplus_error(
				"string[%s] conver to value[%d] error: %d",
				fieldValue.c_str(),
				table_define->field_type(fieldId), ret);
			return (-2);
		} else if (ret == 1) {
			// condition is not matched
			delete row;
			return 0;
		}

		// fill the value
		ret = str_to_value(fieldValue,
				   table_define->field_type(fieldId),
				   (*row)[fieldId]);
		if (ret < 0) {
			delete row;
			log4cplus_error(
				"string[%s] conver to value[%d] error: %d",
				fieldValue.c_str(),
				table_define->field_type(fieldId), ret);
			return (-2);
		}
	}

	// Job->update_key(row);
	ret = Job->append_row(row);

	delete row;

	if (ret < 0) {
		log4cplus_error("append row to job failed!");
		return (-3);
	}

	// totalRows++;

	return (0);
}

int RocksdbProcess::save_direct_row(const std::string &prefixKey,
				    const std::string &compoundKey,
				    const std::string &compoundValue,
				    DirectRequestContext *request_context,
				    DirectResponseContext *response_context,
				    int &totalRows)
{
	if (m_compound_key_field_nums + m_extra_value_field_nums <= 0) {
		log4cplus_error("no fields in the table! key:%s",
				prefixKey.c_str());
		return (-1);
	}

	int ret;
	// decode the compoundKey and check whether it is matched
	std::vector<std::string> keys;
	key_format::do_decode(compoundKey, mKeyfield_types, keys);
	if (keys.size() != m_compound_key_field_nums) {
		// unmatched row
		log4cplus_error(
			"unmatched row, key:%s, fullKey:%s, keyNum:%lu, definitionFieldNum:%d",
			prefixKey.c_str(), compoundKey.c_str(), keys.size(),
			m_compound_key_field_nums);
		return -1;
	}

	// decode key bitmap
	int bitmapLen = 0;
	decodeBitmapKeys(compoundValue, keys, bitmapLen);

	std::string realValue = compoundValue.substr(bitmapLen);

	std::vector<std::string> values;
	split_values(realValue, values);
	assert(values.size() == m_extra_value_field_nums);

	int fieldId, rocksFId;
	std::string fieldValue;
	std::vector<QueryCond> &condFields = request_context->s_field_conds;
	for (size_t idx = 0; idx < condFields.size(); idx++) {
		fieldId = condFields[idx].s_field_index;
		rocksFId = translate_field_idx(fieldId);
		if (rocksFId < m_compound_key_field_nums) {
			fieldValue = keys[rocksFId];
		} else {
			fieldValue =
				values[rocksFId - m_compound_key_field_nums];
		}

		// do condition filter
		ret = condition_filter(fieldValue, condFields[idx].s_cond_value,
				       table_define->field_type(fieldId),
				       condFields[idx].s_cond_opr);
		if (ret < 0) {
			log4cplus_error("condition filt failed! key:%s",
					prefixKey.c_str());
			return -1;
		} else if (ret == 1) {
			// condition is not matched
			return 0;
		}
	}

	// deal with order by syntax
	if (m_order_by_unit || request_context->s_orderby_fields.size() > 0) {
		if (!m_order_by_unit) {
			// build order by unit
			int heapSize =
				request_context->s_limit_cond.s_limit_start >=
							0 &&
						request_context->s_limit_cond
								.s_limit_step >
							0 ?
					request_context->s_limit_cond
							.s_limit_start +
						request_context->s_limit_cond
							.s_limit_step :
					50;
			m_order_by_unit = new RocksdbOrderByUnit(
				table_define, heapSize,
				m_reverse_field_index_mapping,
				request_context->s_orderby_fields);
			assert(m_order_by_unit);
		}

		struct OrderByUnitElement element;
		element.m_rocks_keys.swap(keys);
		element.m_rocks_values.swap(values);
		m_order_by_unit->add_row(element);
		return 0;
	}

	// limit condition control
	ret = 0;
	if (request_context->s_limit_cond.s_limit_start >= 0 &&
	    request_context->s_limit_cond.s_limit_step > 0) {
		if (totalRows < request_context->s_limit_cond.s_limit_start) {
			// not reach to the range of limitation
			totalRows++;
			return 0;
		}

		// leaving from the range of limitaion
		if (response_context->s_row_values.size() ==
		    request_context->s_limit_cond.s_limit_step - 1)
			ret = 1;
	}

	// build row
	build_direct_row(keys, values, response_context);
	totalRows++;

	return ret;
}

void RocksdbProcess::build_direct_row(const std::vector<std::string> &keys,
				      const std::vector<std::string> &values,
				      DirectResponseContext *response_context)
{
	int rocksFId;
	std::string row, fieldValue;
	for (size_t idx1 = 0; idx1 < m_reverse_field_index_mapping.size();
	     idx1++) {
		rocksFId = m_reverse_field_index_mapping[idx1];
		if (rocksFId < m_compound_key_field_nums) {
			fieldValue = keys[rocksFId];
		} else {
			fieldValue =
				values[rocksFId - m_compound_key_field_nums];
		}

		int data_len = fieldValue.length();
		row.append(std::string((char *)&data_len, 4)).append(fieldValue);
	}

	response_context->s_row_values.push_front(row);
	return;
}

int RocksdbProcess::update_row(const std::string &prefixKey,
			       const std::string &compoundKey,
			       const std::string &compoundValue, DtcJob *Job,
			       std::string &newKey, std::string &newValue)
{
	if (m_compound_key_field_nums + m_extra_value_field_nums <= 0) {
		log4cplus_error("no fields in the table!");
		return (-1);
	}

	int ret;
	// decode the compoundKey and check whether it is matched
	std::vector<std::string> keys;
	key_format::do_decode(compoundKey, mKeyfield_types, keys);

	if (keys.size() != m_compound_key_field_nums) {
		// unmatched row
		log4cplus_error(
			"unmatched row, key:%s, fullKey:%s, keyNum:%lu, definitionFieldNum:%d",
			prefixKey.c_str(), compoundKey.c_str(), keys.size(),
			m_compound_key_field_nums);
		return -1;
	}

	bool upKey = false, upVal = false;
	const DTCFieldValue *updateInfo = Job->request_operation();
	whether_update_key(updateInfo, upKey, upVal);

	int keyBitmapLen = 0;
	if (upKey) {
		// recover keys for the next update
		decodeBitmapKeys(compoundValue, keys, keyBitmapLen);
	} else {
		// no need to create bitmap and compound key again
		keyBitmapLen = get_key_bitmap_len(compoundValue);
		assert(keyBitmapLen >= 0);
	}

	std::string bitmapKey = compoundValue.substr(0, keyBitmapLen);
	std::string realValue = compoundValue.substr(keyBitmapLen);

	std::string fieldValue;
	const DTCFieldValue *Condition = Job->request_condition();
	char *valueHead = (char *)realValue.data();
	for (size_t idx = 1; idx < m_field_index_mapping.size(); idx++) {
		int dtcfid = m_field_index_mapping[idx];
		if (idx < m_compound_key_field_nums) {
			fieldValue = keys[idx];
		} else {
			ret = get_value_by_id(valueHead, dtcfid, fieldValue);
			if (ret != 0) {
				log4cplus_error(
					"parse field value failed! compoundValue:%s, key:%s",
					realValue.c_str(), prefixKey.c_str());
				return -1;
			}
		}

		// do condition filter
		ret = condition_filter(fieldValue, dtcfid,
				       table_define->field_type(dtcfid),
				       Condition);
		if (ret < 0) {
			log4cplus_error(
				"string[%s] conver to value[%d] error: %d, %m",
				fieldValue.c_str(),
				table_define->field_type(dtcfid), ret);
			return (-2);
		} else if (ret == 1) {
			// condition is not matched
			return 1;
		}
	}

	// update value
	std::vector<std::string> values;
	if (upVal) {
		// translate the plane raw value to individual field
		split_values(realValue, values);
	}

	for (int i = 0; i < updateInfo->num_fields(); i++) {
		const int fid = updateInfo->field_id(i);

		if (table_define->is_volatile(fid))
			continue;

		int rocksFId = translate_field_idx(fid);
		assert(rocksFId >= 0 &&
		       rocksFId < m_compound_key_field_nums +
					  m_extra_value_field_nums);

		// if not update the value field, the rocksfid must be less than 'm_compound_key_field_nums', so it would not touch
		// the container of 'values'
		std::string &va =
			rocksFId < m_compound_key_field_nums ?
				keys[rocksFId] :
				values[rocksFId - m_compound_key_field_nums];

		switch (updateInfo->field_operation(i)) {
		case DField::Set:
			value_to_str(updateInfo->field_value(i), fid, va);
			break;

		case DField::Add:
			value_add_to_str(updateInfo->field_value(i),
					 updateInfo->field_type(i), va);
			break;

		default:
			log4cplus_error("unsupport operation, opr:%d",
					updateInfo->field_operation(i));
			return -1;
		};
	}

	if (upKey) {
		bitmapKey.clear();
		encode_bitmap_keys(keys, bitmapKey);

		newKey =
			std::move(key_format::do_encode(keys, mKeyfield_types));
	} else
		newKey = compoundKey;

	if (upVal)
		shrink_value(values, newValue);
	else
		newValue = std::move(realValue);

	newValue = std::move(bitmapKey.append(newValue));

	return 0;
}

// query value of the key
int RocksdbProcess::process_select(DtcJob *Job)
{
	log4cplus_info("come into process select!");

#ifdef PRINT_STAT
	mSTime = GET_TIMESTAMP();
#endif

	int ret, i;
	int haslimit = !Job->count_only() && (Job->requestInfo.limit_start() ||
					      Job->requestInfo.limit_count());

	// create resultWriter
	ret = Job->prepare_result_no_limit();
	if (ret != 0) {
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
			       "job prepare-result error");
		log4cplus_error("job prepare-result error: %d, %m", ret);
		return (-2);
	}

	// prefix key
	//std::string prefixKey;
	std::vector<std::string> prefixKey(1);
	ret = value_to_str(Job->request_key(), 0, prefixKey[0]);
	if (ret != 0 || prefixKey[0].empty()) {
		log4cplus_error("dtc primary key can not be empty!");
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
			       "get dtc primary key failed!");
		return -1;
	}
	std::vector<int> preType(1);
	preType[0] = mKeyfield_types[0];

	if (mKeyfield_types[0] == DField::String)
		std::transform(prefixKey[0].begin(), prefixKey[0].end(),
			       prefixKey[0].begin(), ::tolower);

	// encode the key to rocksdb format
	std::string fullKey =
		std::move(key_format::do_encode(prefixKey, preType));
	if (fullKey.empty()) {
		log4cplus_error("encode primary key failed! key:%s",
				prefixKey[0].c_str());
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
			       "encode primary key failed!");
		return -1;
	}

	std::string encodePreKey = fullKey;

	std::string value;
	RocksDBConn::RocksItr_t rocksItr;
	ret = db_connect->retrieve_start(fullKey, value, rocksItr, true);
	if (ret < 0) {
		log4cplus_error("query rocksdb failed! key:%s, ret:%d",
				prefixKey[0].c_str(), ret);
		Job->set_error(ret, __FUNCTION__,
			       "retrieve primary key failed!");
		db_connect->retrieve_end(rocksItr);
		return -1;
	} else if (ret == 1) {
		// not found the key
		Job->set_total_rows(0);
		log4cplus_info("no matched key:%s", prefixKey[0].c_str());
		db_connect->retrieve_end(rocksItr);
		return 0;
	}

	log4cplus_info("begin to iterate key:%s", prefixKey[0].c_str());

	// iterate the matched prefix key and find out the real one from start to end
	int totalRows = 0;
	bool countOnly = Job->count_only();
	while (true) {
		ret = key_matched(encodePreKey, fullKey);
		if (ret != 0) {
			// prefix key not matched, reach to the end
			break;
		}

		// save row
		ret = save_row(fullKey, value, countOnly, totalRows, Job);
		if (ret < 0) {
			// ignore the incorrect key and keep going
			log4cplus_error("save row failed! key:%s",
					prefixKey[0].c_str());
		}

		// move iterator to the next key
		ret = db_connect->next_entry(rocksItr, fullKey, value);
		if (ret < 0) {
			log4cplus_error("iterate rocksdb failed! key:%s",
					prefixKey[0].c_str());
			Job->set_error(ret, __FUNCTION__,
				       "iterate rocksdb failed!");
			db_connect->retrieve_end(rocksItr);
			return -1;
		} else if (ret == 1) {
			// reach to the storage end
			break;
		}

		// has remaining value in rocksdb
	}

	if (Job->count_only()) {
		Job->set_total_rows(totalRows);
	}

	//bug fixed确认客户端带Limit限制
	if (haslimit) { // 获取总行数
		Job->set_total_rows(totalRows, 1);
	}

	db_connect->retrieve_end(rocksItr);

#ifdef PRINT_STAT
	mETime = GET_TIMESTAMP();
	insert_stat(OperationType::OperationQuery, mETime - mSTime);
#endif

	return (0);
}

int RocksdbProcess::process_insert(DtcJob *Job)
{
	log4cplus_info("come into process insert!!!");

#ifdef PRINT_STAT
	mSTime = GET_TIMESTAMP();
#endif

	int ret;

	set_title("INSERT...");

	int totalFields = table_define->num_fields();

	// initialize the fixed empty string vector
	std::bitset<256> filledMap;
	std::vector<std::string> keys(m_compound_key_field_nums);
	std::vector<std::string> values(m_extra_value_field_nums);

	ret = value_to_str(Job->request_key(), 0, keys[0]);
	if (ret != 0 || keys[0].empty()) {
		log4cplus_error("dtc primary key can not be empty!");
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
			       "get dtc primary key failed!");
		return -1;
	} else {
		filledMap[0] = 1;
	}
	log4cplus_info("insert key:%s", keys[0].c_str());

	if (Job->request_operation()) {
		int fid, rocksFId;
		const DTCFieldValue *updateInfo = Job->request_operation();
		for (int i = 0; i < updateInfo->num_fields(); ++i) {
			fid = updateInfo->field_id(i);
			if (table_define->is_volatile(fid))
				continue;

			rocksFId = translate_field_idx(fid);
			assert(rocksFId >= 0 &&
			       rocksFId < m_compound_key_field_nums +
						  m_extra_value_field_nums);

			if (fid == 0)
				continue;

			std::string &va =
				rocksFId < m_compound_key_field_nums ?
					keys[rocksFId] :
					values[rocksFId -
					       m_compound_key_field_nums];
			ret = value_to_str(updateInfo->field_value(i), fid, va);
			assert(ret == 0);

			filledMap[fid] = 1;
		}
	}

	// set default value
	for (int i = 1; i <= totalFields; ++i) {
		int rocksFId;
		if (table_define->is_volatile(i))
			continue;

		if (filledMap[i])
			continue;

		rocksFId = translate_field_idx(i);
		assert(rocksFId >= 0 &&
		       rocksFId < m_compound_key_field_nums +
					  m_extra_value_field_nums);

		std::string &va =
			rocksFId < m_compound_key_field_nums ?
				keys[rocksFId] :
				values[rocksFId - m_compound_key_field_nums];
		ret = value_to_str(table_define->default_value(i), i, va);
		assert(ret == 0);
	}

#ifdef DEBUG_INFO
	std::stringstream ss;
	ss << "insert row:[";
	for (size_t idx = 0; idx < m_compound_key_field_nums; idx++) {
		ss << keys[idx];
		if (idx != m_compound_key_field_nums - 1)
			ss << ",";
	}
	ss << "]";
	log4cplus_error("%s", ss.str().c_str());
#endif

	// convert string type 'key' into lower case and build case letter bitmap
	std::string keyBitmaps;
	encode_bitmap_keys(keys, keyBitmaps);

	std::string rocksKey, rocksValue;
	rocksKey = std::move(key_format::do_encode(keys, mKeyfield_types));

	// value use plane style to organize, no need to encode
	ret = shrink_value(values, rocksValue);
	if (ret != 0) {
		std::string rkey;
		value_to_str(Job->request_key(), 0, rkey);
		log4cplus_error("shrink value failed, key:%s", rkey.c_str());

		Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
			       "shrink buff failed!");
		return -1;
	}

	// add key bitmaps to the rocksdb value field
	keyBitmaps.append(rocksValue);

	log4cplus_info("save kv, key:%s, value:%s", rocksKey.c_str(),
		       rocksValue.c_str());
	ret = db_connect->insert_entry(rocksKey, keyBitmaps, true);
	if (ret != 0) {
		std::string rkey;
		value_to_str(Job->request_key(), 0, rkey);
		if (ret != -ER_DUP_ENTRY) {
			Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
				       "insert key failed!");
			log4cplus_error("insert key failed, ret:%d, key:%s",
					ret, rkey.c_str());
		} else {
			Job->set_error_dup(ret, __FUNCTION__,
					   "insert entry failed!");
			log4cplus_error("insert duplicate key : %s",
					rkey.c_str());
		}

		return (-1);
	}

	Job->resultInfo.set_affected_rows(1);

#ifdef PRINT_STAT
	mETime = GET_TIMESTAMP();
	insert_stat(OperationType::OperationInsert, mETime - mSTime);
#endif

	return (0);
}

// update the exist rows matched the condition
int RocksdbProcess::process_update(DtcJob *Job)
{
	log4cplus_info("come into rocks update");

#ifdef PRINT_STAT
	mSTime = GET_TIMESTAMP();
#endif

	int ret, affectRows = 0;

	// prefix key
	std::string prefixKey;
	ret = value_to_str(Job->request_key(), 0, prefixKey);
	if (ret != 0) {
		log4cplus_error("get dtc primary key failed! key");
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__, "get key failed!");
		return -1;
	}
	log4cplus_info("update key:%s", prefixKey.c_str());

	if (Job->request_operation() == NULL) {
		log4cplus_info("request operation info is null!");
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
			       "update field not found");
		return (-1);
	}

	if (Job->request_operation()->has_type_commit() == 0) {
		// pure volatile fields update, always succeed
		log4cplus_info("update pure volatile fields!");
		Job->resultInfo.set_affected_rows(affectRows);
		return (0);
	}

	const DTCFieldValue *updateInfo = Job->request_operation();
	if (updateInfo == NULL) {
		// no need to do update
		log4cplus_info("request update info is null!");
		Job->resultInfo.set_affected_rows(affectRows);
		return (0);
	}

	set_title("UPDATE...");

	bool updateKey = false, updateValue = false;
	whether_update_key(updateInfo, updateKey, updateValue);
	if (!updateKey && !updateValue) {
		// no need to do update
		log4cplus_info("no change for the row!");
		Job->resultInfo.set_affected_rows(affectRows);
		return (0);
	}

	if (mKeyfield_types[0] == DField::String)
		std::transform(prefixKey.begin(), prefixKey.end(),
			       prefixKey.begin(), ::tolower);

	// encode the key to rocksdb format
	std::string fullKey = std::move(key_format::encode_bytes(prefixKey));
	std::string encodePreKey = fullKey;

	std::string compoundValue;
	RocksDBConn::RocksItr_t rocksItr;
	ret = db_connect->retrieve_start(fullKey, compoundValue, rocksItr,
					 true);
	if (ret < 0) {
		log4cplus_info("retrieve rocksdb failed, key:%s",
			       prefixKey.c_str());
		Job->set_error_dup(ret, __FUNCTION__,
				   "retrieve rocksdb failed!");
		db_connect->retrieve_end(rocksItr);
		return -1;
	} else if (ret == 1) {
		// not found the key
		log4cplus_info("no matched key:%s", prefixKey.c_str());
		Job->resultInfo.set_affected_rows(affectRows);
		db_connect->retrieve_end(rocksItr);
		return 0;
	}

	// retrieve the range keys one by one
	std::set<std::string> deletedKeys;
	std::map<std::string, std::string> newEntries;
	if (updateKey) {
		// Will update the row, so we need to keep the whole row in the memory for checking
		// the unique constraints
		// Due to the limitaion of the memory, we can not hold all rows in the memory sometimes,
		// so use the LRU to evit the oldest row
		std::set<std::string>
			originKeys; // keys located in rocksdb those before doing update
		while (true) {
			// find if the key has been update before, if yes, should rollback the previously update
			auto itr = newEntries.find(fullKey);
			if (itr != newEntries.end()) {
				log4cplus_info("duplicated entry, key:%s",
					       fullKey.c_str());
				// report alarm
				Job->set_error_dup(-ER_DUP_ENTRY, __FUNCTION__,
						   "duplicate key!");
				db_connect->retrieve_end(rocksItr);
				return -1;
			}

			ret = key_matched(encodePreKey, fullKey);
			if (ret != 0) {
				// prefix key not matched, reach to the end
				break;
			}

			// update row
			std::string newKey, newValue;
			ret = update_row(prefixKey, fullKey, compoundValue, Job,
					 newKey, newValue);
			if (ret < 0) {
				// ignore the incorrect key and keep going
				log4cplus_error(
					"save row failed! key:%s, compoundValue:%s",
					fullKey.c_str(), compoundValue.c_str());
			} else if (ret == 1) {
				// key matched, but condition mismatched, keep on retrieve
				originKeys.insert(fullKey);
			} else {
				{
					ret = rocks_key_matched(fullKey,
								newKey);
					if (ret == 0) {
						log4cplus_info(
							"duplicated entry, newkey:%s",
							newKey.c_str());
						Job->set_error_dup(
							-ER_DUP_ENTRY,
							__FUNCTION__,
							"duplicate key!");
						db_connect->retrieve_end(
							rocksItr);
						return -1;
					}
				}

				if (originKeys.find(newKey) ==
					    originKeys.end() &&
				    newEntries.find(newKey) ==
					    newEntries.end() &&
				    deletedKeys.find(newKey) ==
					    deletedKeys.end()) {
					// there are so many duplcate string save in the different containers, this will
					// waste too much space, we can optimize it in the future
					affectRows++;
					deletedKeys.insert(fullKey);
					newEntries[newKey] = newValue;
				} else {
					// duplicate key, ignore it
					log4cplus_info(
						"duplicated entry, newkey:%s",
						newKey.c_str());
					Job->set_error_dup(-ER_DUP_ENTRY,
							   __FUNCTION__,
							   "duplicate key!");
					db_connect->retrieve_end(rocksItr);
					return -1;
				}
			}

			// move iterator to the next key
			ret = db_connect->next_entry(rocksItr, fullKey,
						     compoundValue);
			if (ret < 0) {
				log4cplus_error(
					"retrieve compoundValue failed, key:%s",
					prefixKey.c_str());
				Job->set_error_dup(ret, __FUNCTION__,
						   "get next entry failed!");
				db_connect->retrieve_end(rocksItr);
				return -1;
			} else if (ret == 1) {
				// no remaining rows in the storage
				break;
			}

			// has remaining compoundValue in rocksdb
		}
	} else {
		// do not break the unique key constraints, no need to hold the entire row in memory
		// iterate the matched prefix key and find out the real one from start to end
		while (true) {
			ret = key_matched(encodePreKey, fullKey);
			if (ret != 0) {
				// prefix key not matched, reach to the end
				break;
			}

			// update row
			std::string newKey, newValue;
			ret = update_row(prefixKey, fullKey, compoundValue, Job,
					 newKey, newValue);
			if (ret < 0) {
				// ignore the incorrect key and keep going
				log4cplus_error(
					"save row failed! key:%s, compoundValue:%s",
					fullKey.c_str(), compoundValue.c_str());
			} else if (ret == 1) {
				// key matched, but condition mismatched, keep on retrieve
			} else {
				affectRows++;
				newEntries[fullKey] = newValue;
			}

			// move iterator to the next key
			ret = db_connect->next_entry(rocksItr, fullKey,
						     compoundValue);
			if (ret < 0) {
				log4cplus_error(
					"retrieve compoundValue failed, key:%s",
					prefixKey.c_str());
				Job->set_error_dup(ret, __FUNCTION__,
						   "get next entry failed!");
				db_connect->retrieve_end(rocksItr);
				return -1;
			} else if (ret == 1) {
				// reach to the storage end
				break;
			}

			// has remaining compoundValue in rocksdb
		}
	}

#ifdef DEBUG_INFO
	std::vector<std::string> keys;
	std::stringstream ss;

	ss << "delete keys:[";
	auto itr = deletedKeys.begin();
	while (itr != deletedKeys.end()) {
		keys.clear();
		key_format::do_decode(*itr, mKeyfield_types, keys);

		ss << "(";
		for (size_t idx = 0; idx < keys.size(); idx++) {
			ss << keys[idx];
			if (idx != keys.size() - 1)
				ss << ",";
		}
		ss << ") ";

		itr++;
	}
	ss << "]";

	ss << "new keys:[";
	auto itrM = newEntries.begin();
	while (itrM != newEntries.end()) {
		keys.clear();
		key_format::do_decode(itrM->first, mKeyfield_types, keys);

		ss << "(";
		for (size_t idx = 0; idx < keys.size(); idx++) {
			ss << keys[idx];
			if (idx != keys.size() - 1)
				ss << ",";
		}
		ss << ") ";

		itrM++;
	}
	ss << "]";

	log4cplus_error("%s", ss.str().c_str());
#endif

	// do atomic update
	ret = db_connect->batch_update(deletedKeys, newEntries, true);
	if (ret != 0) {
		log4cplus_error("batch update rocksdb failed! key:%s",
				prefixKey.c_str());
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__, "update failed!");
		return -1;
	}

	db_connect->retrieve_end(rocksItr);

	Job->resultInfo.set_affected_rows(affectRows);

#ifdef PRINT_STAT
	mETime = GET_TIMESTAMP();
	insert_stat(OperationType::OperationUpdate, mETime - mSTime);
#endif

	return (0);
}

int RocksdbProcess::whether_update_key(const DTCFieldValue *UpdateInfo,
				       bool &updateKey, bool &updateValue)
{
	int fieldId;
	updateKey = false;
	updateValue = false;
	for (int i = 0; i < UpdateInfo->num_fields(); i++) {
		const int fid = UpdateInfo->field_id(i);

		if (table_define->is_volatile(fid))
			continue;

		assert(fid < m_field_index_mapping.size());

		for (size_t idx = 0; idx < m_field_index_mapping.size();
		     idx++) {
			if (fid == m_field_index_mapping[idx]) {
				if (idx < m_compound_key_field_nums)
					updateKey = true;
				else
					updateValue = true;

				break;
			}
		}
	}

	return 0;
}

int RocksdbProcess::process_delete(DtcJob *Job)
{
	int ret, affectRows = 0;

	// prefix key
	std::string prefixKey;
	ret = value_to_str(Job->request_key(), 0, prefixKey);
	if (ret != 0) {
		log4cplus_error("get dtc primary key failed! key");
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__, "get key failed!");
		return -1;
	}

	if (mKeyfield_types[0] == DField::String)
		std::transform(prefixKey.begin(), prefixKey.end(),
			       prefixKey.begin(), ::tolower);

	// encode the key to rocksdb format
	std::string fullKey = std::move(key_format::encode_bytes(prefixKey));
	std::string encodePreKey = fullKey;

	std::string compoundValue;
	RocksDBConn::RocksItr_t rocksItr;
	ret = db_connect->retrieve_start(fullKey, compoundValue, rocksItr,
					 true);
	if (ret < 0) {
		log4cplus_error("retrieve rocksdb failed! key:%s",
				fullKey.c_str());
		Job->set_error_dup(ret, __FUNCTION__, "retrieve failed!");
		db_connect->retrieve_end(rocksItr);
		return -1;
	} else if (ret == 1) {
		// not found the key
		Job->resultInfo.set_affected_rows(affectRows);
		db_connect->retrieve_end(rocksItr);
		log4cplus_info("no matched key:%s", prefixKey.c_str());
		return 0;
	}

	// iterate the matched prefix key and find out the real one from start to end
	bool condMatch = true;
	int bitmapLen = 0;
	DTCFieldValue *condition;
	std::set<std::string> deleteKeys;
	while (true) {
		// check whether the key is in the range of the prefix of the 'fullKey'
		ret = key_matched(encodePreKey, fullKey);
		if (ret != 0) {
			// prefix key not matched, reach to the end
			break;
		}

		// decode the compoundKey and check whether it is matched
		std::string realValue;
		std::vector<std::string> keys;
		std::vector<std::string> values;
		key_format::do_decode(fullKey, mKeyfield_types, keys);
		assert(keys.size() > 0);

		ret = prefixKey.compare(keys[0]);
		// if ( ret != 0 ) goto NEXT_ENTRY;
		if (ret != 0)
			break;

		if (keys.size() != m_compound_key_field_nums) {
			// unmatched row
			log4cplus_error(
				"unmatched row, fullKey:%s, keyNum:%lu, definitionFieldNum:%d",
				fullKey.c_str(), keys.size(),
				m_compound_key_field_nums);
			ret = 0;
		}

		// decode key bitmap
		decodeBitmapKeys(compoundValue, keys, bitmapLen);

		realValue = compoundValue.substr(bitmapLen);
		split_values(realValue, values);
		if (values.size() != m_extra_value_field_nums) {
			log4cplus_info(
				"unmatched row, fullKey:%s, value:%s, keyNum:%lu, valueNum:%lu",
				fullKey.c_str(), compoundValue.c_str(),
				keys.size(), values.size());
			ret = 0;
		}

		// condition filter
		condition = (DTCFieldValue *)Job->request_condition();
		for (size_t idx = 1; idx < m_field_index_mapping.size();
		     idx++) {
			int fieldId = m_field_index_mapping[idx];
			std::string &fieldValue =
				idx < m_compound_key_field_nums ?
					keys[idx] :
					values[idx - m_compound_key_field_nums];

			// do condition filter
			ret = condition_filter(
				fieldValue, fieldId,
				table_define->field_type(fieldId), condition);
			if (ret < 0) {
				log4cplus_error(
					"string[%s] conver to value[%d] error: %d, %m",
					fieldValue.c_str(),
					table_define->field_type(fieldId), ret);
				condMatch = false;
				break;
			} else if (ret == 1) {
				// condition is not matched
				condMatch = false;
				break;
			}
		}

		if (condMatch) {
#ifdef DEBUG_INFO
			std::stringstream ss;
			ss << "delete key:[";
			for (size_t idx = 0; idx < m_compound_key_field_nums;
			     idx++) {
				ss << keys[idx];
				if (idx != m_compound_key_field_nums - 1)
					ss << ",";
			}
			ss << "]";
			log4cplus_error("%s", ss.str().c_str());
#endif
			deleteKeys.insert(fullKey);
			affectRows++;
		}

	NEXT_ENTRY:
		// move iterator to the next key
		ret = db_connect->next_entry(rocksItr, fullKey, compoundValue);
		if (ret < 0) {
			Job->set_error_dup(ret, __FUNCTION__,
					   "get next entry failed!");
			db_connect->retrieve_end(rocksItr);
			return -1;
		} else if (ret == 1) {
			// reach to the end of the storage
			break;
		}
	}

	// delete key from Rocksdb storage, inside the 'retrieve end' for transaction isolation, this delete will be not seen before release this retrieve
	ret = db_connect->batch_update(
		deleteKeys, std::map<std::string, std::string>(), true);
	if (ret != 0) {
		log4cplus_error("batch update rocksdb failed! key:%s",
				prefixKey.c_str());
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__, "update failed!");
		return -1;
	}

	db_connect->retrieve_end(rocksItr);
	Job->resultInfo.set_affected_rows(affectRows);

	return (0);
}

int RocksdbProcess::process_task(DtcJob *Job)
{
	if (Job == NULL) {
		log4cplus_error("Job is NULL!%s", "");
		return (-1);
	}

	table_define = TableDefinitionManager::instance()->get_cur_table_def();

	switch (Job->request_code()) {
	case DRequest::TYPE_PASS:
	case DRequest::Purge:
	case DRequest::Flush:
		return 0;

	case DRequest::Get:
		return process_select(Job);

	case DRequest::Insert:
		return process_insert(Job);

	case DRequest::Update:
		return process_update(Job);

	case DRequest::Delete:
		return process_delete(Job);

	case DRequest::Replace:
		return process_replace(Job);

	case DRequest::ReloadConfig:
		return process_reload_config(Job);

	// master-slave replication
	case DRequest::Replicate:
		return ProcessReplicate(Job);
		// cluster scaleable
		//case DRequest::Migrate:
		//  return ProcessMigrate();

	default:
		Job->set_error(-EC_BAD_COMMAND, __FUNCTION__,
			       "invalid request-code");
		return (-1);
	}
}

//add by frankyang 处理更新过的交易日志
int RocksdbProcess::process_replace(DtcJob *Job)
{
	int ret, affectRows = 0;

	set_title("REPLACE...");

	// primary key in DTC
	std::vector<std::string> keys, values;
	keys.resize(m_compound_key_field_nums);
	values.resize(m_extra_value_field_nums);

	std::string strKey;
	value_to_str(Job->request_key(), 0, strKey);
	keys[0] = strKey;

	// deal update info
	const DTCFieldValue *updateInfo = Job->request_operation();
	if (updateInfo != NULL) {
		for (int idx = 0; idx < updateInfo->num_fields(); idx++) {
			const int fid = updateInfo->field_id(idx);

			if (table_define->is_volatile(fid))
				continue;

			std::string fieldValue;
			switch (updateInfo->field_operation(idx)) {
			case DField::Set: {
				ret = value_to_str(updateInfo->field_value(idx),
						   fid, fieldValue);
				if (ret != 0) {
					Job->set_error(-EC_ERROR_BASE,
						       __FUNCTION__,
						       "get value failed!");
					log4cplus_error(
						"translate value failed");
					return (-1);
				}
				break;
			}

			case DField::Add: {
				// add additional value to the defaule value
				const DTCValue *addValue =
					updateInfo->field_value(idx);
				const DTCValue *defValue =
					table_define->default_value(idx);
				switch (updateInfo->field_type(idx)) {
				case DField::Signed:
					fieldValue = std::move(std::to_string(
						(long long)(addValue->s64 +
							    defValue->s64)));
					break;

				case DField::Unsigned:
					fieldValue = std::move(std::to_string((
						unsigned long long)(addValue->u64 +
								    defValue->u64)));
					break;

				case DField::Float:
					fieldValue = std::move(std::to_string(
						addValue->flt + defValue->flt));
					break;
				default:
					Job->set_error(-EC_ERROR_BASE,
						       __FUNCTION__,
						       "unkonwn field type!");
					log4cplus_error(
						"unsupport field data type:%d",
						updateInfo->field_type(idx));
					return -1;
				}
				break;
			}

			default:
				log4cplus_error(
					"unsupport field operation:%d",
					updateInfo->field_operation(idx));
				break;
			}

			int rocksidx = translate_field_idx(fid);
			assert(rocksidx >= 0 &&
			       rocksidx < m_compound_key_field_nums +
						  m_extra_value_field_nums);
			rocksidx < m_compound_key_field_nums ?
				keys[rocksidx] = std::move(fieldValue) :
				values[rocksidx - m_compound_key_field_nums] =
					std::move(fieldValue);
		}
	}

	// deal default value
	uint8_t mask[32];
	FIELD_ZERO(mask);
	if (updateInfo)
		updateInfo->build_field_mask(mask);

	for (int i = 1; i <= table_define->num_fields(); i++) {
		if (FIELD_ISSET(i, mask) || table_define->is_volatile(i))
			continue;

		std::string fieldValue;
		ret = value_to_str(table_define->default_value(i), i,
				   fieldValue);
		if (ret != 0) {
			Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
				       "get value failed!");
			log4cplus_error("translate value failed");
			return (-1);
		}

		int rocksidx = translate_field_idx(i);
		assert(rocksidx >= 0 &&
		       rocksidx < m_compound_key_field_nums +
					  m_extra_value_field_nums);
		rocksidx < m_compound_key_field_nums ?
			keys[rocksidx] = std::move(fieldValue) :
			values[rocksidx - m_compound_key_field_nums] =
				std::move(fieldValue);
	}

	// convert string type 'key' into lower case and build case letter bitmap
	std::string keyBitmaps;
	encode_bitmap_keys(keys, keyBitmaps);

	std::string rocksKey, rocksValue;
	rocksKey = std::move(key_format::do_encode(keys, mKeyfield_types));

	shrink_value(values, rocksValue);

	// add key bitmaps to the rocksdb value field
	keyBitmaps.append(rocksValue);

	ret = db_connect->replace_entry(rocksKey, keyBitmaps, true);
	if (ret == 0) {
		Job->resultInfo.set_affected_rows(1);
		return 0;
	}

	log4cplus_error("replace key failed, key:%s, code:%d", rocksKey.c_str(),
			ret);
	Job->set_error_dup(ret, __FUNCTION__, "replace key failed!");
	return -1;
}

int RocksdbProcess::ProcessReplicate(DtcJob *Job)
{
	log4cplus_info("come into rocksdb replicate!");

	int ret, totalRows = 0;

	// create resultWriter
	ret = Job->prepare_result_no_limit();
	if (ret != 0) {
		Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
			       "job prepare-result error");
		log4cplus_error("job prepare-result error: %d, %m", ret);
		return (-2);
	}

	// key for replication start
	std::string startKey, prevPrimaryKey, compoundKey, compoundValue;
	RocksDBConn::RocksItr_t rocksItr;

	// whether start a newly replication or not
	uint32_t start = Job->requestInfo.limit_start();
	uint32_t count = Job->requestInfo.limit_count();
	// if full replicate start from 0 and the start key is empty, means it's a newly replication
	bool isBeginRepl = (start == 0);
	if (likely(!isBeginRepl)) {
		// replicate with user given key
		ret = value_to_str(Job->request_key(), 0, startKey);
		if (ret != 0 || startKey.empty()) {
			log4cplus_error("replicate key can not be empty!");
			Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
				       "get replicate key failed!");
			return -1;
		}

		// encode the key to rocksdb format
		compoundKey = std::move(key_format::encode_bytes(startKey));
		if (compoundKey.empty()) {
			log4cplus_error("encode primary key failed! key:%s",
					startKey.c_str());
			Job->set_error(-EC_ERROR_BASE, __FUNCTION__,
				       "encode replicate key failed!");
			return -1;
		}

		prevPrimaryKey = compoundKey;

		ret = db_connect->search_lower_bound(compoundKey, compoundValue,
						     rocksItr);
	} else {
#if 0
    // get the last key for replication finished tag
    ret = db_connect->get_last_entry(compoundKey, compoundValue, rocksItr);
    if ( ret < 0 )
    {
      // replicate error, let the user decide to try again
      log4cplus_error("get last key failed!");
      Job->set_error(-EC_ERROR_BASE, __FUNCTION__, "get last replicate key failed!");
      return -1;
    }
    else if ( ret == 1 )
    {
      // empty database
      Job->set_total_rows(0);
      return 0;
    }

    // set the finished key of replicating into meta data column family
    // delete the odd migrate-end-key from that may insert by the previous slave
    m_repl_end_key = compoundKey;
    // use replace api to instead of insert, in case there has multi slave replicator, all
    // of them should always replicate to the latest one key
    ret = db_connect->replace_entry(gReplicatePrefixKey, m_repl_end_key, true, RocksDBConn::COLUMN_META_DATA);
    if ( ret != 0 )
    {
      log4cplus_error("save replicating-finished-key failed! key:%s", m_repl_end_key.c_str());
      Job->set_error(-EC_ERROR_BASE, __FUNCTION__, "save replicating finished key failed!");
      return -1;
    }
#endif

		// do forward retrieving for reducing duplicate replication
		startKey = "";
		prevPrimaryKey = "";

		ret = db_connect->get_first_entry(compoundKey, compoundValue,
						  rocksItr);
	}

	if (ret < 0) {
		log4cplus_error("query rocksdb failed! isBeginRepl:%d, key:%s",
				isBeginRepl, startKey.c_str());
		Job->set_error_dup(ret, __FUNCTION__, "do replication failed!");
		db_connect->retrieve_end(rocksItr);
		return -1;
	} else if (ret == 1) {
		// not found the key
		Job->set_total_rows(0);
		log4cplus_info("do full replication finished! %s",
			       startKey.c_str());
		db_connect->retrieve_end(rocksItr);
		return 0;
	}

	// iterate the matched prefix key and find out the real one from start to end
	int replLen = 0;
	while (true) {
		// 1.skip the user given key
		// 2.the same prefix key only get once
		if (key_matched(prevPrimaryKey, compoundKey) == 0) {
			// ignore the matched key that has been migrated in the previous call
		} else {
			// save row
			ret = save_row(compoundKey, compoundValue, false,
				       totalRows, Job);
			if (ret < 0) {
				// ignore the incorrect key and keep going
				log4cplus_error(
					"save row failed! key:%s, value:%s",
					compoundKey.c_str(),
					compoundValue.c_str());
			}

			key_format::get_format_key(compoundKey,
						   mKeyfield_types[0],
						   prevPrimaryKey);
		}

		// move iterator to the next key
		ret = db_connect->next_entry(rocksItr, compoundKey,
					     compoundValue);
		if (ret < 0) {
			log4cplus_error("iterate rocksdb failed! key:%s",
					startKey.c_str());
			Job->set_error(ret, __FUNCTION__,
				       "iterate rocksdb failed!");
			db_connect->retrieve_end(rocksItr);
			return -1;
		} else if (ret == 1) {
			// reach to the end
			break;
		}

		// has remaining value in rocksdb
		if (totalRows >= count)
			break;
		// replLen += (compoundKey,length() + compoundValue.length());
		// if ( relpLen >= MAX_REPLICATE_LEN )
		// {
		//   // avoid network congestion
		//   break;
		// }
	}

	Job->set_total_rows(totalRows);
	db_connect->retrieve_end(rocksItr);

	return (0);
}

int RocksdbProcess::process_direct_query(DirectRequestContext *request_context,
					 DirectResponseContext *response_context)
{
	log4cplus_info("come into process direct query!");

#ifdef PRINT_STAT
	mSTime = GET_TIMESTAMP();
#endif

	int ret;

	std::vector<QueryCond> primaryKeyConds;
	ret = analyse_primary_key_conds(request_context, primaryKeyConds);
	if (ret != 0) {
		log4cplus_error("query condition incorrect in query context!");
		response_context->s_row_nums = -EC_ERROR_BASE;
		return -1;
	}

	// prefix key
	std::string prefixKey = primaryKeyConds[0].s_cond_value;
	if (prefixKey.empty()) {
		log4cplus_error("dtc primary key can not be empty!");
		response_context->s_row_nums = -EC_ERROR_BASE;
		return -1;
	}

	if (mKeyfield_types[0] == DField::String)
		std::transform(prefixKey.begin(), prefixKey.end(),
			       prefixKey.begin(), ::tolower);

	// encode the key to rocksdb format
	std::string fullKey = std::move(key_format::encode_bytes(prefixKey));
	std::string encodePreKey = fullKey;

	int totalRows = 0;
	std::string value;
	RocksDBConn::RocksItr_t rocksItr;

	bool forwardDirection = (primaryKeyConds[0].s_cond_opr ==
					 (uint8_t)ConditionOperation::EQ ||
				 primaryKeyConds[0].s_cond_opr ==
					 (uint8_t)ConditionOperation::GT ||
				 primaryKeyConds[0].s_cond_opr ==
					 (uint8_t)ConditionOperation::GE);
	bool backwardEqual = primaryKeyConds[0].s_cond_opr ==
			     (uint8_t)ConditionOperation::LE;
	if (backwardEqual) {
		// if the query condtion is < || <=, use seek_for_prev to seek in the total_order_seek mode
		// will not use the prefix seek features, eg:
		//  1. we have the flowing union key in the rocks (101,xx), (102,xx), (103,xx)
		//  2. we use total_order_seek features for ranging query with primary key '102', and this
		//     lead the rocksdb doesn't use prefix_extractor to match the prefix key, so it use the
		//     entire key for comparing, and seek_for_prev will stop in the last key that <= the
		//     target key, so the iterator point to the key '(101, xx)', that's not what we want,
		//     wo need it point to the '(102,xx)'

		// do primary key equal query first in this section
		ret = db_connect->retrieve_start(fullKey, value, rocksItr,
						 true);
		if (ret < 0) {
			log4cplus_error("query rocksdb failed! key:%s, ret:%d",
					prefixKey.c_str(), ret);
			response_context->s_row_nums = -EC_ERROR_BASE;
			db_connect->retrieve_end(rocksItr);
			return -1;
		} else if (ret == 0) {
			while (true) {
				ret = key_matched(encodePreKey, fullKey);
				if (ret != 0) {
					// prefix key not matched, reach to the end
					break;
				}

				// save row
				ret = save_direct_row(prefixKey, fullKey, value,
						      request_context,
						      response_context,
						      totalRows);
				if (ret < 0) {
					// ignore the incorrect key and keep going
					log4cplus_error(
						"save row failed! key:%s, value:%s",
						fullKey.c_str(), value.c_str());
				} else if (ret == 1)
					break;

				// move iterator to the next key
				ret = db_connect->next_entry(rocksItr, fullKey,
							     value);
				if (ret < 0) {
					log4cplus_error(
						"iterate rocksdb failed! key:%s",
						prefixKey.c_str());
					response_context->s_row_nums =
						-EC_ERROR_BASE;
					db_connect->retrieve_end(rocksItr);
					return -1;
				} else if (ret == 1) {
					// reach to the storage end
					break;
				}

				// has remaining value in rocksdb
			}
		}
	}

	// range query in the following section
	ret = db_connect->retrieve_start(fullKey, value, rocksItr, false,
					 forwardDirection);
	if (ret < 0) {
		log4cplus_error("query rocksdb failed! key:%s, ret:%d",
				prefixKey.c_str(), ret);
		response_context->s_row_nums = -EC_ERROR_BASE;
		db_connect->retrieve_end(rocksItr);
		return -1;
	} else if (ret == 1) {
		// not found the key
		log4cplus_info("no matched key:%s", prefixKey.c_str());
		response_context->s_row_nums = 0;
		db_connect->retrieve_end(rocksItr);
		return 0;
	}

	// iterate the matched prefix key and find out the real one from start to end
	while (true) {
		ret = range_key_matched(fullKey, primaryKeyConds);
		if (ret == -1) {
			// prefix key not matched, reach to the end
			break;
		}

		if (ret == 0) {
			// save row
			ret = save_direct_row(prefixKey, fullKey, value,
					      request_context, response_context,
					      totalRows);
			if (ret < 0) {
				// ignore the incorrect key and keep going
				log4cplus_error(
					"save row failed! key:%s, value:%s",
					fullKey.c_str(), value.c_str());
			} else if (ret == 1)
				break;
		}

		// move iterator to the next key
		if (forwardDirection) {
			ret = db_connect->next_entry(rocksItr, fullKey, value);
		} else {
			ret = db_connect->prev_entry(rocksItr, fullKey, value);
		}
		if (ret < 0) {
			log4cplus_error("iterate rocksdb failed! key:%s",
					prefixKey.c_str());
			response_context->s_row_nums = ret;
			db_connect->retrieve_end(rocksItr);
			return -1;
		} else if (ret == 1) {
			// reach to the storage end
			break;
		}

		// has remaining value in rocksdb
	}

	// generate response rows in order by container
	if (m_order_by_unit) {
		OrderByUnitElement element;
		int start =
			request_context->s_limit_cond.s_limit_start >= 0 &&
					request_context->s_limit_cond
							.s_limit_step > 0 ?
				request_context->s_limit_cond.s_limit_start :
				0;
		while (true) {
			ret = m_order_by_unit->get_row(element);
			if (0 == ret) {
				delete m_order_by_unit;
				m_order_by_unit = NULL;
				break;
			}

			if (start != 0) {
				start--;
				continue;
			}

			build_direct_row(element.m_rocks_keys,
					 element.m_rocks_values,
					 response_context);
		}
	}

	response_context->s_row_nums = response_context->s_row_values.size();
	db_connect->retrieve_end(rocksItr);

#ifdef PRINT_STAT
	mETime = GET_TIMESTAMP();
	insert_stat(OperationType::OperationDirectQuery, mETime - mSTime);
#endif

	return (0);
}

int RocksdbProcess::encode_dtc_key(const std::string &oKey,
				   std::string &codedKey)
{
	int keyLen = oKey.length();
	static const int maxLen = 10240;
	assert(sizeof(int) + keyLen <= maxLen);
	static thread_local char keyBuff[maxLen];

	char *pos = keyBuff;
	*(int *)pos = keyLen;
	pos += sizeof(int);
	memcpy((void *)pos, (void *)oKey.data(), keyLen);

	codedKey.assign(keyBuff, keyLen + sizeof(int));

	return 0;
}

int RocksdbProcess::decode_keys(const std::string &compoundKey,
				std::vector<std::string> &keys)
{
	int ret;
	std::string keyField;
	char *head = const_cast<char *>(compoundKey.data());

	// decode dtckey first
	int keyLen = *(int *)head;
	head += sizeof(int);
	keyField.assign(head, keyLen);
	head += keyLen;
	keys.push_back(std::move(keyField));

	// decode other key fields
	for (int idx = 1; idx < m_compound_key_field_nums; idx++) {
		ret = get_value_by_id(head, m_field_index_mapping[idx],
				      keyField);
		assert(ret == 0);
		keys.push_back(std::move(keyField));
	}

	return 0;
}

int RocksdbProcess::encode_rocks_key(const std::vector<std::string> &keys,
				     std::string &rocksKey)
{
	assert(keys.size() == m_compound_key_field_nums);

	// evaluate space
	static int align = 1 << 12;
	int valueLen = 0, fid, fsize;
	int totalLen = align;
	char *valueBuff = (char *)malloc(totalLen);

	// encode key first
	int keyLen = keys[0].length();
	*(int *)valueBuff = keyLen;
	valueLen += sizeof(int);
	memcpy((void *)valueBuff, (void *)keys[0].data(), keyLen);
	valueLen += keyLen;

	for (size_t idx = 1; idx < m_compound_key_field_nums; idx++) {
		fid = m_field_index_mapping[idx];
		switch (table_define->field_type(fid)) {
		case DField::Signed: {
			fsize = table_define->field_size(fid);
			if (fsize > sizeof(int32_t)) {
				if (valueLen + sizeof(int64_t) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(int64_t) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(int64_t *)(valueBuff + valueLen) =
					strtoll(keys[idx].c_str(), NULL, 10);
				valueLen += sizeof(int64_t);
			} else {
				if (valueLen + sizeof(int32_t) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(int32_t) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(int32_t *)(valueBuff + valueLen) =
					strtol(keys[idx].c_str(), NULL, 10);
				valueLen += sizeof(int32_t);
			}
			break;
		}

		case DField::Unsigned: {
			fsize = table_define->field_size(fid);
			if (fsize > sizeof(uint32_t)) {
				if (valueLen + sizeof(uint64_t) > totalLen) {
					// expand buff
					totalLen =
						(valueLen + sizeof(uint64_t) +
						 align - 1) &
						-align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(uint64_t *)(valueBuff + valueLen) =
					strtoull(keys[idx].c_str(), NULL, 10);
				valueLen += sizeof(uint64_t);
			} else {
				if (valueLen + sizeof(uint32_t) > totalLen) {
					// expand buff
					totalLen =
						(valueLen + sizeof(uint32_t) +
						 align - 1) &
						-align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(uint32_t *)(valueBuff + valueLen) =
					strtoul(keys[idx].c_str(), NULL, 10);
				valueLen += sizeof(uint32_t);
			}
			break;
		}

		case DField::Float: {
			fsize = table_define->field_size(fid);
			if (fsize > sizeof(float)) {
				if (valueLen + sizeof(double) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(double) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(double *)(valueBuff + valueLen) =
					strtod(keys[idx].c_str(), NULL);
				valueLen += sizeof(double);
			} else {
				if (valueLen + sizeof(float) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(float) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(float *)(valueBuff + valueLen) =
					strtof(keys[idx].c_str(), NULL);
				valueLen += sizeof(float);
			}
			break;
		}

		case DField::String:
		case DField::Binary: {
			int len = keys[idx].length();
			fsize = len + sizeof(int);
			{
				if (valueLen + fsize > totalLen) {
					// expand buff
					totalLen =
						(valueLen + fsize + align - 1) &
						-align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(int *)(valueBuff + valueLen) = len;
				valueLen += sizeof(int);
				if (len > 0)
					memcpy((void *)(valueBuff + valueLen),
					       (void *)keys[idx].data(), len);
				valueLen += len;
			}
			break;
		}

		default:
			log4cplus_error("unexpected field type! type:%d",
					table_define->field_type(fid));
			return -1;
		};
	}

	rocksKey.assign(valueBuff, valueLen);
	free(valueBuff);

	return 0;
}

// 1. convert string type key into lower case
// 2. create bit map for those been converted keys
void RocksdbProcess::encode_bitmap_keys(std::vector<std::string> &keys,
					std::string &keyBitmaps)
{
	if (m_no_bitmap_key)
		return;

	std::vector<char> keyLocationBitmap, keyCaseBitmap;
	int8_t localBits = 0;
	bool hasBeenConverted = false;

	for (size_t idx = 0; idx < keys.size(); idx++) {
		switch (mKeyfield_types[idx]) {
		default:
			hasBeenConverted = false;
			break;
		case DField::String: {
			// maybe need convert
			std::vector<char> currentKeyBitmap;
			hasBeenConverted =
				convert_to_lower(keys[idx], currentKeyBitmap);
			if (hasBeenConverted) {
				keyCaseBitmap.insert(
					keyCaseBitmap.end(),
					std::make_move_iterator(
						currentKeyBitmap.begin()),
					std::make_move_iterator(
						currentKeyBitmap.end()));
			}
		}
		}

		// record key location bitmap
		if (hasBeenConverted) {
			int shift =
				BITS_OF_BYTE - 1 - 1 - idx % (BITS_OF_BYTE - 1);
			localBits = (localBits >> shift | 1U) << shift;
		}

		// the last boundary bit in this section and has remaining keys, need to set the
		// head bit for indicading
		if ((idx + 1) % (BITS_OF_BYTE - 1) == 0 ||
		    idx == keys.size() - 1) {
			if (idx != keys.size() - 1)
				localBits |= 128U;
			keyLocationBitmap.push_back((char)localBits);
			localBits = 0;
		}
	}

	// shrink bits to buffer
	keyBitmaps
		.append(std::string(keyLocationBitmap.begin(),
				    keyLocationBitmap.end()))
		.append(std::string(keyCaseBitmap.begin(),
				    keyCaseBitmap.end()));
}

void RocksdbProcess::decodeBitmapKeys(const std::string &rocksValue,
				      std::vector<std::string> &keys,
				      int &bitmapLen)
{
	bitmapLen = 0;

	if (m_no_bitmap_key)
		return;

	int8_t sectionBits;
	std::vector<char> keyLocationBitmap;

	// decode key location bitmap
	while (true) {
		sectionBits = rocksValue[bitmapLen];
		keyLocationBitmap.push_back(sectionBits);
		bitmapLen++;

		if ((sectionBits & 0x80) == 0)
			break;
	}

	int shift = 0;
	for (size_t idx = 0; idx < keys.size(); idx++) {
		sectionBits = keyLocationBitmap[idx / (BITS_OF_BYTE - 1)];
		shift = BITS_OF_BYTE - 1 - 1 - idx % (BITS_OF_BYTE - 1);

		switch (mKeyfield_types[idx]) {
		default:
			assert((sectionBits >> shift & 1U) == 0);
			break;
		case DField::String: {
			if ((sectionBits >> shift & 1U) == 0) {
				// no need to do convert
			} else {
				// recovery the origin key
				recover_to_upper(rocksValue, bitmapLen,
						 keys[idx]);
			}
		}
		}
	}
}

int RocksdbProcess::get_key_bitmap_len(const std::string &rocksValue)
{
	int bitmapLen = 0;

	if (m_no_bitmap_key)
		return bitmapLen;

	int8_t sectionBits;
	std::deque<char> keyLocationBitmap;

	// decode key location bitmap
	while (true) {
		sectionBits = rocksValue[bitmapLen];
		keyLocationBitmap.push_back(sectionBits);
		bitmapLen++;

		if ((sectionBits & 0x80) == 0)
			break;
	}

	int shift = 0;
	while (keyLocationBitmap.size() != 0) {
		sectionBits = keyLocationBitmap.front();
		for (int8_t idx = 1; idx < BITS_OF_BYTE; idx++) {
			shift = BITS_OF_BYTE - 1 - idx;

			if ((sectionBits >> shift & 1U) == 1) {
				// collect the key bitmap len
				int8_t keyBits;
				while (true) {
					keyBits =
						(int8_t)rocksValue[bitmapLen++];
					if ((keyBits & 0x80) == 0)
						break;
				}
			}
		}

		keyLocationBitmap.pop_front();
	}

	return bitmapLen;
}

bool RocksdbProcess::convert_to_lower(std::string &key,
				      std::vector<char> &keyCaseBitmap)
{
	bool hasConverted = false;
	int8_t caseBits = 0;
	char lowerBase = 'a' - 'A';
	for (size_t idx = 0; idx < key.length(); idx++) {
		char &cv = key.at(idx);
		if (cv >= 'A' && cv <= 'Z') {
			cv += lowerBase;

			int shift =
				BITS_OF_BYTE - 1 - 1 - idx % (BITS_OF_BYTE - 1);
			caseBits = (caseBits >> shift | 1U) << shift;

			hasConverted = true;
		}

		if ((idx + 1) % (BITS_OF_BYTE - 1) == 0 ||
		    idx == key.length() - 1) {
			if (idx != key.length() - 1)
				caseBits |= 128U;
			keyCaseBitmap.push_back((char)caseBits);
			caseBits = 0;
		}
	}

	return hasConverted;
}

void RocksdbProcess::recover_to_upper(const std::string &rocksValue,
				      int &bitmapLen, std::string &key)
{
	int shift;
	int kIdx = 0;
	bool hasRemaining = true;
	char upperBase = 'a' - 'A';
	int8_t sectionBits;

	do {
		sectionBits = rocksValue[bitmapLen];

		shift = BITS_OF_BYTE - 1 - 1 - kIdx % (BITS_OF_BYTE - 1);
		if (sectionBits >> shift & 1U) {
			// convert to upper mode
			char &cc = key[kIdx];
			assert(cc >= 'a' && cc <= 'z');
			cc -= upperBase;
		}

		kIdx++;
		if (kIdx % (BITS_OF_BYTE - 1) == 0) {
			bitmapLen++;
			hasRemaining = (sectionBits & 0x80) != 0;
		}
	} while (hasRemaining);
}

int RocksdbProcess::shrink_value(const std::vector<std::string> &values,
				 std::string &rocksValue)
{
	assert(values.size() == m_extra_value_field_nums);

	// evaluate space
	static int align = 1 << 12;
	int valueLen = 0, fid, fsize;
	int totalLen = align;
	char *valueBuff = (char *)malloc(totalLen);
	for (size_t idx = 0; idx < m_extra_value_field_nums; idx++) {
		fid = m_field_index_mapping[m_compound_key_field_nums + idx];
		switch (table_define->field_type(fid)) {
		case DField::Signed: {
			fsize = table_define->field_size(fid);
			if (fsize > sizeof(int32_t)) {
				if (valueLen + sizeof(int64_t) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(int64_t) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(int64_t *)(valueBuff + valueLen) =
					strtoll(values[idx].c_str(), NULL, 10);
				valueLen += sizeof(int64_t);
			} else {
				if (valueLen + sizeof(int32_t) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(int32_t) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(int32_t *)(valueBuff + valueLen) =
					strtol(values[idx].c_str(), NULL, 10);
				valueLen += sizeof(int32_t);
			}
			break;
		}

		case DField::Unsigned: {
			fsize = table_define->field_size(fid);
			if (fsize > sizeof(uint32_t)) {
				if (valueLen + sizeof(uint64_t) > totalLen) {
					// expand buff
					totalLen =
						(valueLen + sizeof(uint64_t) +
						 align - 1) &
						-align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(uint64_t *)(valueBuff + valueLen) =
					strtoull(values[idx].c_str(), NULL, 10);
				valueLen += sizeof(uint64_t);
			} else {
				if (valueLen + sizeof(uint32_t) > totalLen) {
					// expand buff
					totalLen =
						(valueLen + sizeof(uint32_t) +
						 align - 1) &
						-align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(uint32_t *)(valueBuff + valueLen) =
					strtoul(values[idx].c_str(), NULL, 10);
				valueLen += sizeof(uint32_t);
			}
			break;
		}

		case DField::Float: {
			fsize = table_define->field_size(fid);
			if (fsize > sizeof(float)) {
				if (valueLen + sizeof(double) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(double) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(double *)(valueBuff + valueLen) =
					strtod(values[idx].c_str(), NULL);
				valueLen += sizeof(double);
			} else {
				if (valueLen + sizeof(float) > totalLen) {
					// expand buff
					totalLen = (valueLen + sizeof(float) +
						    align - 1) &
						   -align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(float *)(valueBuff + valueLen) =
					strtof(values[idx].c_str(), NULL);
				valueLen += sizeof(float);
			}
			break;
		}

		case DField::String:
		case DField::Binary: {
			int len = values[idx].length();
			fsize = len + sizeof(int);
			{
				if (valueLen + fsize > totalLen) {
					// expand buff
					totalLen =
						(valueLen + fsize + align - 1) &
						-align;
					valueBuff = expand_buff(totalLen,
								valueBuff);
					if (!valueBuff)
						return -1;
				}

				*(int *)(valueBuff + valueLen) = len;
				valueLen += sizeof(int);
				if (len > 0)
					memcpy((void *)(valueBuff + valueLen),
					       (void *)values[idx].data(), len);
				valueLen += len;
			}
			break;
		}

		default:
			log4cplus_error("unexpected field type! type:%d",
					table_define->field_type(fid));
			return -1;
		};
	}

	rocksValue.assign(valueBuff, valueLen);
	free(valueBuff);

	return 0;
}

int RocksdbProcess::split_values(const std::string &compoundValue,
				 std::vector<std::string> &values)
{
	int ret;
	std::string value;
	char *head = const_cast<char *>(compoundValue.data());
	for (int idx = 0; idx < m_extra_value_field_nums; idx++) {
		ret = get_value_by_id(
			head,
			m_field_index_mapping[m_compound_key_field_nums + idx],
			value);
		assert(ret == 0);
		values.push_back(std::move(value));
	}

	return 0;
}

// translate dtcfid to rocksfid
int RocksdbProcess::translate_field_idx(int dtcfid)
{
	for (size_t idx = 0; idx < m_field_index_mapping.size(); idx++) {
		if (m_field_index_mapping[idx] == dtcfid)
			return idx;
	}

	return -1;
}

int RocksdbProcess::get_value_by_id(char *&valueHead, int fieldId,
				    std::string &fieldValue)
{
	assert(valueHead);

	// evaluate space
	int fsize;
	int field_type = table_define->field_type(fieldId);
	switch (field_type) {
	case DField::Signed: {
		fsize = table_define->field_size(fieldId);
		if (fsize > sizeof(int32_t)) {
			int64_t value = *(int64_t *)(valueHead);
			valueHead += sizeof(int64_t);
			fieldValue = std::move(std::to_string(value));
		} else {
			int32_t value = *(int32_t *)(valueHead);
			valueHead += sizeof(int32_t);
			fieldValue = std::move(std::to_string(value));
		}
		break;
	}

	case DField::Unsigned: {
		fsize = table_define->field_size(fieldId);
		if (fsize > sizeof(uint32_t)) {
			uint64_t value = *(uint64_t *)(valueHead);
			valueHead += sizeof(uint64_t);
			fieldValue = std::move(std::to_string(value));
		} else {
			uint32_t value = *(uint32_t *)(valueHead);
			valueHead += sizeof(uint32_t);
			fieldValue = std::move(std::to_string(value));
		}
		break;
	}

	case DField::Float: {
		fsize = table_define->field_size(fieldId);
		if (fsize <= sizeof(float)) {
			float value = *(float *)(valueHead);
			valueHead += sizeof(float);
			fieldValue = std::move(std::to_string(value));
		} else {
			double value = *(double *)(valueHead);
			valueHead += sizeof(double);
			fieldValue = std::move(std::to_string(value));
		}
		break;
	}

	case DField::String:
	case DField::Binary: {
		int len;
		{
			len = *(int *)(valueHead);
			valueHead += sizeof(int);

			fieldValue = std::move(std::string(valueHead, len));
			valueHead += len;
		}
		break;
	}

	default:
		log4cplus_error("unexpected field type! type:%d", field_type);
		return -1;
	};

	return 0;
}

char *RocksdbProcess::expand_buff(int len, char *oldPtr)
{
	char *newPtr = (char *)realloc((void *)oldPtr, len);
	if (!newPtr) {
		log4cplus_error("realloc memory failed!");
		free(oldPtr);
	}

	return newPtr;
}

// check two rocksdb key whether equal or not
int RocksdbProcess::rocks_key_matched(const std::string &rocksKey1,
				      const std::string &rocksKey2)
{
	return rocksKey1.compare(rocksKey2);
}

// check whether the key in the query conditon range matched or not
// 1 : in the range but not matched
// 0: key matched
// -1: in the out of the range
int RocksdbProcess::range_key_matched(const std::string &rocksKey,
				      const std::vector<QueryCond> &keyConds)
{
	std::string primaryKey;
	int field_type = mKeyfield_types[0];
	key_format::decode_primary_key(rocksKey, field_type, primaryKey);

	int ret;
	for (size_t idx = 0; idx < keyConds.size(); idx++) {
		ret = condition_filter(primaryKey, keyConds[idx].s_cond_value,
				       field_type, keyConds[idx].s_cond_opr);
		if (ret != 0) {
			// check boundary value
			switch (keyConds[idx].s_cond_opr) {
			/* enum {
           EQ = 0,
           NE = 1,
           LT = 2,
           LE = 3,
           GT = 4,
           GE = 5,
           }; */
			case 0:
			case 1: // not support now
			case 3:
			case 5:
				return -1;
			case 2:
			case 4:
				return primaryKey.compare(
					       keyConds[idx].s_cond_value) ==
						       0 ?
					       1 :
					       -1;
			default:
				log4cplus_error("unsupport condition:%d",
						keyConds[idx].s_cond_opr);
			}
		}
	}

	return 0;
}

int RocksdbProcess::analyse_primary_key_conds(
	DirectRequestContext *request_context,
	std::vector<QueryCond> &primaryKeyConds)
{
	std::vector<QueryCond> &queryConds = request_context->s_field_conds;
	auto itr = queryConds.begin();
	while (itr != queryConds.end()) {
		if (itr->s_field_index == 0) {
			switch ((ConditionOperation)itr->s_cond_opr) {
			case ConditionOperation::EQ:
			case ConditionOperation::LT:
			case ConditionOperation::LE:
			case ConditionOperation::GT:
			case ConditionOperation::GE:
				break;
			case ConditionOperation::NE:
			default:
				log4cplus_error(
					"unsupport query expression now! condExpr:%d",
					itr->s_cond_opr);
				return -1;
			}
			primaryKeyConds.push_back(*itr);
			itr = queryConds.erase(itr);
		} else {
			itr++;
		}
	}

	if (primaryKeyConds.size() <= 0) {
		log4cplus_error("no explicit primary key in query context!");
		return -1;
	}

	return 0;
}

void RocksdbProcess::init_title(int group, int role)
{
	title_prefix_size = snprintf(name, sizeof(name), "helper%d%c", group,
				     MACHINEROLESTRING[role]);
	memcpy(title, name, title_prefix_size);
	title[title_prefix_size++] = ':';
	title[title_prefix_size++] = ' ';
	title[title_prefix_size] = '\0';
	title[sizeof(title) - 1] = '\0';
}

void RocksdbProcess::set_title(const char *status)
{
	strncpy(title + title_prefix_size, status,
		sizeof(title) - 1 - title_prefix_size);
	set_proc_title(title);
}

int RocksdbProcess::process_reload_config(DtcJob *Job)
{
	const char *keyStr = g_dtc_config->get_str_val("cache", "DTCID");
	int cache_key = 0;
	if (keyStr == NULL) {
		cache_key = 0;
		log4cplus_info("DTCID not set!");
		return -1;
	} else if (!strcasecmp(keyStr, "none")) {
		log4cplus_error("DTCID set to NONE, Cache disabled");
		return -1;
	} else if (isdigit(keyStr[0])) {
		cache_key = strtol(keyStr, NULL, 0);
	} else {
		log4cplus_error("Invalid DTCID value \"%s\"", keyStr);
		return -1;
	}
	BlockProperties stInfo;
	BufferPond bufPool;
	memset(&stInfo, 0, sizeof(stInfo));
	stInfo.ipc_mem_key = cache_key;
	stInfo.key_size = TableDefinitionManager::instance()
				  ->get_cur_table_def()
				  ->key_format();
	stInfo.read_only = 1;

	if (bufPool.cache_open(&stInfo)) {
		log4cplus_error("%s", bufPool.error());
		Job->set_error(-EC_RELOAD_CONFIG_FAILED, __FUNCTION__,
			       "open cache error!");
		return -1;
	}

	bufPool.reload_table();
	log4cplus_error(
		"cmd notify work helper reload table, tableIdx : [%d], pid : [%d]",
		bufPool.shm_table_idx(), getpid());
	return 0;
}

void RocksdbProcess::insert_stat(RocksdbProcess::OperationType oprType,
				 int64_t timeElapse)
{
	assert(oprType >= OperationType::OperationInsert &&
	       oprType < OperationType::OperationDelete);
	int opr = (int)oprType;

	if (timeElapse < 1000)
		mOprTimeCost[opr][(int)TimeZone::TimeStatLevel0]++;
	else if (timeElapse < 2000)
		mOprTimeCost[opr][(int)TimeZone::TimeStatLevel1]++;
	else if (timeElapse < 3000)
		mOprTimeCost[opr][(int)TimeZone::TimeStatLevel2]++;
	else if (timeElapse < 4000)
		mOprTimeCost[opr][(int)TimeZone::TimeStatLevel3]++;
	else if (timeElapse < 5000)
		mOprTimeCost[opr][(int)TimeZone::TimeStatLevel4]++;
	else
		mOprTimeCost[opr][(int)TimeZone::TimeStatLevel5]++;

	mTotalOpr++;

	if (mTotalOpr % 10000 == 0)
		print_stat_info();

	return;
}

void RocksdbProcess::print_stat_info()
{
	int totalNum;

	std::stringstream ss;
	ss << "time cost per opr:\n";
	ss << "totalOpr:" << mTotalOpr << "\n";
	for (unsigned char idx0 = 0;
	     idx0 <= (unsigned char)OperationType::OperationQuery; idx0++) {
		switch ((OperationType)idx0) {
		case OperationType::OperationInsert: {
			totalNum = 0;

			ss << "Insert:[";
			for (unsigned char idx1 = 0;
			     idx1 < (unsigned char)TimeZone::TimeStatMax;
			     idx1++) {
				ss << mOprTimeCost[idx0][idx1];
				if (idx1 !=
				    (unsigned char)TimeZone::TimeStatMax - 1)
					ss << ", ";
				totalNum += mOprTimeCost[idx0][idx1];
			}
			ss << "] total:" << totalNum << "\n";
			break;
		}
		case OperationType::OperationUpdate: {
			totalNum = 0;

			ss << "Update:[";
			for (unsigned char idx1 = 0;
			     idx1 < (unsigned char)TimeZone::TimeStatMax;
			     idx1++) {
				ss << mOprTimeCost[idx0][idx1];
				if (idx1 !=
				    (unsigned char)TimeZone::TimeStatMax - 1)
					ss << ", ";
				totalNum += mOprTimeCost[idx0][idx1];
			}
			ss << "] total:" << totalNum << "\n";
			break;
		}
		case OperationType::OperationDirectQuery: {
			totalNum = 0;

			ss << "DirectQuery:[";
			for (unsigned char idx1 = 0;
			     idx1 < (unsigned char)TimeZone::TimeStatMax;
			     idx1++) {
				ss << mOprTimeCost[idx0][idx1];
				if (idx1 !=
				    (unsigned char)TimeZone::TimeStatMax - 1)
					ss << ", ";
				totalNum += mOprTimeCost[idx0][idx1];
			}
			ss << "] total:" << totalNum << "\n";
			break;
		}
		case OperationType::OperationQuery: {
			totalNum = 0;

			ss << "Query:[";
			for (unsigned char idx1 = 0;
			     idx1 < (unsigned char)TimeZone::TimeStatMax;
			     idx1++) {
				ss << mOprTimeCost[idx0][idx1];
				if (idx1 !=
				    (unsigned char)TimeZone::TimeStatMax - 1)
					ss << ", ";
				totalNum += mOprTimeCost[idx0][idx1];
			}
			ss << "] total:" << totalNum << "\n";
			break;
		}
		case OperationType::OperationReplace: {
			totalNum = 0;

			ss << "Replace:[";
			for (unsigned char idx1 = 0;
			     idx1 < (unsigned char)TimeZone::TimeStatMax;
			     idx1++) {
				ss << mOprTimeCost[idx0][idx1];
				if (idx1 !=
				    (unsigned char)TimeZone::TimeStatMax - 1)
					ss << ", ";
				totalNum += mOprTimeCost[idx0][idx1];
			}
			ss << "] total:" << totalNum << "\n";
			break;
		}
		case OperationType::OperationDelete: {
			totalNum = 0;

			ss << "Delete:[";
			for (unsigned char idx1 = 0;
			     idx1 < (unsigned char)TimeZone::TimeStatMax;
			     idx1++) {
				ss << mOprTimeCost[idx0][idx1];
				if (idx1 !=
				    (unsigned char)TimeZone::TimeStatMax - 1)
					ss << ", ";
				totalNum += mOprTimeCost[idx0][idx1];
			}
			ss << "] total:" << totalNum << "\n";
			break;
		}
		}
	}

	log4cplus_error("%s", ss.str().c_str());

	return;
}
