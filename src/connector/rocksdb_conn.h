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
#ifndef __ROCKS_DB_CONN_H__
#define __ROCKS_DB_CONN_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <set>
#include <log/log.h>
#include "algorithm/singleton.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "auto_roll_logger.h"
#include "protocol.h"
using namespace rocksdb;

class CaseSensitiveFreeComparator;
class CommKeyComparator;

class RocksDBConn {
    public:
	typedef Iterator *RocksItr_t;

	// errcode bring back to dtc for error handling, it must match the MYSQL errno
	enum ErrCode {
		ERROR_INTERNAL,
		ERROR_KEY_NOT_FOUND = 1032,
		ERROR_DUPLICATE_KEY = 1062,
	};

	enum ColumnFamiliesIndex {
		COLUMN_REAL_DATA =
			0, // save DTC user data, use Default column family
		COLUMN_META_DATA =
			1, // save metadata info, such as migration state
		COLUMN_DEFAULT, // NOT use, but must have it

		COLUMN_FAMILIES_NUM,
	};

    private:
	std::string m_rocks_db_path;
	Env *m_rocks_env;
	DB *m_rocks_db;
	// Iterator* mIterator;
	std::vector<ColumnFamilyHandle *>
		m_column_handles; // element index match the enum ColumnFamiliesIndex

	std::shared_ptr<rocksdb::Logger> m_rocksdb_logger;

	int m_dtckey_type;
	int m_fixed_key_len;

	// CaseSensitiveFreeComparator* mCommKeyComparator;
	std::shared_ptr<const SliceTransform> m_prefix_extractor;

	std::unique_ptr<TraceWriter> m_trace_writer;

	bool m_has_connected;
	char achErr[400];
	int dberr;

    private:
    public:
	RocksDBConn();
	~RocksDBConn();

	static RocksDBConn *instance()
	{
		return Singleton<RocksDBConn>::instance();
	}

	void set_key_type(int type)
	{
		m_dtckey_type = type;
	}
	int Open(const std::string &dbPath);
	int Close();

	const char *get_err_msg();
	int get_errno();
	int get_raw_errno();

	// get value of the specific key that must full mached
	int get_entry(const std::string &key, std::string &value,
		      ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// if key exist, return 'duplicate key' error
	int insert_entry(const std::string &key, const std::string &value,
			 bool syncMode = false,
			 ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// if key not exist,do nothing; otherwise, update its value
	int update_entry(const std::string &key, const std::string &value,
			 bool syncMode = false,
			 ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// if key not exist, insert it; otherwise, update its value
	int replace_entry(const std::string &key, const std::string &value,
			  bool syncMode = false,
			  ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// append a 'delete' tombstone to indicate the key has been deleted, the tombstone can
	// not been compacted unless in the bottom level compaction
	int delete_entry(const std::string &key, bool syncMode = false,
			 ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// delete the exist key directly instead of append a tombstone for decreasing the number
	// of tombstone those may duplicated and long time existen in the storage, that may inefficient
	// the read perfomance. before use this interface, wo must guarantted the following rules:
	// 1.the removed key must be written for only once
	int
	single_delete_entry(const std::string &key, bool syncMode = false,
			    ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	/*
  // use rocksdb 'DeleteRange' API to delete a range of keys among [startKey, endKey)
  int deleteRangeEntry(
      const std::string &startKey,
      const std;:string &endKey,
      bool syncMode = false,
      ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);*/

	// not support atomic update across column families
	int batch_update(const std::set<std::string> &deletedKeys,
			 const std::map<std::string, std::string> &newEntries,
			 bool syncMode = false,
			 ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// API only for migration, delete the key and bring back its value
	int migrate_entry(const std::string &key, std::string &value,
			  ColumnFamiliesIndex colIndex, bool syncMode = false);

	/* the following function set are using for range query, replicateStart
   * and replicateEnd must appear together */
	// start an replicateion with the given key
	int retrieve_start(
		std::string &startKey, std::string &value, RocksItr_t &itr,
		bool searchMode =
			false, // use prefix mode to search the key, if false, use total order seek
		bool forwardDirection = true,
		ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// release the iteration snapshot
	int retrieve_end(RocksItr_t itr);

	int prev_entry(RocksItr_t itr, std::string &key, std::string &value);

	// for retrieving the key in the snapshot windows that allocate by 'retrieve_start'
	int next_entry(RocksItr_t itr, std::string &key, std::string &value);

	// search the first key in the source that at or past target( >= ).
	int search_lower_bound(std::string &targetKey, std::string &value,
			       RocksItr_t &itr,
			       ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// search the last key in the source that at or before target( <= ).
	int search_upper_bound(std::string &targetKey, std::string &value,
			       RocksItr_t &itr,
			       ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// get the first key-value in current database
	int get_first_entry(std::string &key, std::string &value,
			    RocksItr_t &itr,
			    ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	// get the last key-value in current database
	int get_last_entry(std::string &key, std::string &value,
			   RocksItr_t &itr,
			   ColumnFamiliesIndex colIndex = COLUMN_REAL_DATA);

	uint32_t escape_string(char To[], const char *From);

    private:
	int key_exist(const std::string &key, ColumnFamiliesIndex colIndex);

	int create_rocks_dir(const std::string &stuffDir);

	void init_db_options(Options &option);
	void init_statistic_info(Options &option);
	void init_prefix_extractor(ColumnFamilyOptions &option);
	void init_table_options(Options &option);
	void init_column_family_options(ColumnFamilyOptions &colOptions);
	void init_flushing_options(Options &option);
	void init_compaction_options(Options &colOptions);
	void init_environment_options(Options &option);
	int create_tracer();
};

#endif // __ROCKS_DB_CONN_H__
