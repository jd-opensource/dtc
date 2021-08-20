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

#ifndef __DB_PROCESS_ROCKS_H__
#define __DB_PROCESS_ROCKS_H__

#include "db_process_base.h"
#include "rocksdb_conn.h"
#include "key_format.h"
#include "rocksdb_key_comparator.h"
#include "rocksdb_direct_context.h"
#include "rocksdb_orderby_unit.h"

class RocksdbProcess : public HelperProcessBase {
    private:
	int error_no;
	RocksDBConn *db_connect;

	char name[16];
	char title[80];
	int title_prefix_size;

	DTCTableDefinition *table_define;
	int m_compound_key_field_nums;
	int m_extra_value_field_nums;
	std::vector<uint8_t>
		m_field_index_mapping; // DTC field index map to rocksdb, index is
	// the fieldid in rocksdb, and the value is the fieldid in DTC
	std::vector<uint8_t>
		m_reverse_field_index_mapping; // rocksdb idx map to dtc, value is rocks idx
	std::vector<int> mKeyfield_types;
	bool m_no_bitmap_key;

	int self_group_id;
	unsigned int proc_timeout;

	// denoted finished key for replication
	std::string m_repl_end_key;

	// for migrating
	std::string m_prev_migrate_key; // rocks key without suffix
	std::string m_current_migrate_key;
	int64_t m_uncommited_mig_id;

	RocksdbOrderByUnit *m_order_by_unit;

	// statistic info
	enum class TimeZone : unsigned char {
		TimeStatLevel0, // < 1ms
		TimeStatLevel1, // [1ms, 2ms)
		TimeStatLevel2, // < [2ms, 3ms)
		TimeStatLevel3, // < [3ms, 4ms)
		TimeStatLevel4, // < [4ms, 5ms)
		TimeStatLevel5, // >= 5ms
		TimeStatMax
	};

	enum class OperationType : unsigned char {
		OperationInsert,
		OperationUpdate,
		OperationDirectQuery,
		OperationQuery,
		OperationReplace,
		OperationDelete
	};

	std::vector<std::vector<int> > mOprTimeCost = {
		{ 0, 0, 0, 0, 0, 0 }, // OperationType::OperationInsert
		{ 0, 0, 0, 0, 0, 0 }, // update
		{ 0, 0, 0, 0, 0, 0 }, // direct query
		{ 0, 0, 0, 0, 0, 0 } // query
	};
	int64_t mSTime;
	int64_t mETime;
	int64_t mTotalOpr;

    public:
	RocksdbProcess(RocksDBConn *conn);
	virtual ~RocksdbProcess();

	int do_init(int group_id, const DbConfig *Config,
		    DTCTableDefinition *tdef, int slave);
	int check_table();

	int process_task(DtcJob *Job);

	void init_title(int m, int t);
	void set_title(const char *status);
	const char *Name(void)
	{
		return name;
	}
	void set_proc_timeout(unsigned int seconds)
	{
		proc_timeout = seconds;
	}

	int process_direct_query(DirectRequestContext *request_context,
				 DirectResponseContext *response_context);

    protected:
	void init_ping_timeout(void);
	void use_matched_rows(void);

	inline int value_to_str(const DTCValue *Value, int fieldId,
				std::string &strValue);
	std::string value_to_str(const DTCValue *value, int field_type);

	inline int set_default_value(int field_type, DTCValue &Value);
	inline int str_to_value(const std::string &str, int field_type,
				DTCValue &Value);

	int save_row(const std::string &compoundKey,
		     const std::string &compoundValue, bool countOnly,
		     int &totalRows, DtcJob *Job);

	int save_direct_row(const std::string &prefixKey,
			    const std::string &compoundKey,
			    const std::string &compoundValue,
			    DirectRequestContext *request_context,
			    DirectResponseContext *response_context,
			    int &totalRows);

	void build_direct_row(const std::vector<std::string> &keys,
			      const std::vector<std::string> &values,
			      DirectResponseContext *response_context);

	int update_row(const std::string &prefixKey,
		       const std::string &compoundKey,
		       const std::string &compoundValue, DtcJob *Job,
		       std::string &newKey, std::string &newValue);

	int whether_update_key(const DTCFieldValue *UpdateInfo, bool &updateKey,
			       bool &updateValue);

	int shrink_value(const std::vector<std::string> &values,
			 std::string &rocksValue);

	int split_values(const std::string &compoundValue,
			 std::vector<std::string> &values);

	int translate_field_idx(int dtcfid);

	int get_value_by_id(char *&valueHead, int fieldId,
			    std::string &fieldValue);

	char *expand_buff(int len, char *oldPtr);

	int process_select(DtcJob *Job);
	int process_insert(DtcJob *Job);
	int process_update(DtcJob *Job);
	int process_delete(DtcJob *Job);
	int process_replace(DtcJob *Job);
	int process_reload_config(DtcJob *Job);
	int ProcessReplicate(DtcJob *Job);

	int value_add_to_str(const DTCValue *additionValue, int ifield_type,
			     std::string &baseValue);

	int condition_filter(const std::string &rocksValue, int fieldid,
			     int field_type, const DTCFieldValue *condition);

	int condition_filter(const std::string &rocksValue,
			     const std::string &condValue, int field_type,
			     int comparator);

	template <class... Args> bool is_matched_template(Args... len);

	template <class T>
	bool is_matched(const T lv, int comparator, const T rv);

	//template<char*>
	bool is_matched(const char *lv, int comparator, const char *rv,
			int lLen, int rLen, bool caseSensitive);

    private:
	int get_replicate_end_key();

	int encode_dtc_key(const std::string &oKey, std::string &codedKey);

	int encode_rocks_key(const std::vector<std::string> &keys,
			     std::string &rocksKey);

	void encode_bitmap_keys(std::vector<std::string> &keys,
				std::string &keyBitmaps);

	void decodeBitmapKeys(const std::string &rocksValue,
			      std::vector<std::string> &keys, int &bitmapLen);

	int decode_keys(const std::string &compoundKey,
			std::vector<std::string> &keys);

	bool convert_to_lower(std::string &key,
			      std::vector<char> &keyCaseBitmap);

	void recover_to_upper(const std::string &rocksValue, int &bitmapLen,
			      std::string &key);

	int get_key_bitmap_len(const std::string &rocksValue);

	// check dtc key whether match the prifix of the `rockskey`
	inline int key_matched(const std::string &dtcKey,
			       const std::string &rocksKey)
	{
		int dtcKLen = dtcKey.length();
		int rockKLen = rocksKey.length();

		if ((dtcKLen == 0 && rockKLen != 0) || dtcKLen > rockKLen)
			return 1;

		// compare with case sensitive
		return rocksKey.compare(0, dtcKLen, dtcKey);
	}

	// check two rocksdb key whether equal or not
	int rocks_key_matched(const std::string &rocksKey1,
			      const std::string &rocksKey2);
	int range_key_matched(const std::string &rocksKey,
			      const std::vector<QueryCond> &keyConds);

	int analyse_primary_key_conds(DirectRequestContext *request_context,
				      std::vector<QueryCond> &primaryKeyConds);

	void insert_stat(RocksdbProcess::OperationType opr, int64_t timeElapse);

	void print_stat_info();
};

#endif // __DB_PROCESS_ROCKS_H__
