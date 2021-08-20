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

#ifndef __DTC_HB_LOG_H
#define __DTC_HB_LOG_H

#include "log/logger.h"
#include "journal_id.h"
#include "task/task_request.h"
#include "field/field.h"
#include "raw_data.h"
#include "table/hotbackup_table_def.h"
#include "sys_malloc.h"
#include "table/table_def.h"

class BinlogWriter;
class BinlogReader;

class HBLog {
    public:
	//传入编解码的表结构
	HBLog(DTCTableDefinition *tbl);
	~HBLog();

	int init(const char *path, const char *prefix, uint64_t total,
		 off_t max_size);
	int Seek(const JournalID &);

	JournalID get_reader_jid(void);
	JournalID get_writer_jid(void);

	//不带value，只写更新key
	int write_update_key(DTCValue key, int type);

	//将多条log记录编码进TaskReqeust
	int task_append_all_rows(DTCJobOperation &, int limit);

	//提供给LRUBitUnit来记录lru变更
	int write_lru_hb_log(DTCJobOperation &job);
	int write_update_log(DTCJobOperation &job);
	int write_update_key(DTCValue key, DTCValue v, int type);

    private:
	DTCTableDefinition *tabledef_;
	BinlogWriter *log_writer_;
	BinlogReader *log_reader_;
};

#endif
