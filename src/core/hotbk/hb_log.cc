/*
* Copyright JD.com, Inc.
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

#include "hb_log.h"
#include "global.h"
#include "table/hotbackup_table_def.h"

HBLog::HBLog(DTCTableDefinition *tbl)
	: tabledef_(tbl), log_writer_(0), log_reader_(0)
{
}

HBLog::~HBLog()
{
	DELETE(log_writer_);
	DELETE(log_reader_);
}

int HBLog::init(const char *path, const char *prefix, uint64_t total,
		off_t max_size)
{
	log_writer_ = new BinlogWriter;
	log_reader_ = new BinlogReader;

	if (log_writer_->init(path, prefix, total, max_size)) {
		log4cplus_error("init log_writer failed");
		return DTC_CODE_FAILED;
	}

	if (log_reader_->init(path, prefix)) {
		log4cplus_error("init log_reader failed");
		return -2;
	}

	return DTC_CODE_SUCCESS;
}

int HBLog::write_update_log(DTCJobOperation &job)
{
	RawData *raw_data;
	NEW(RawData(&g_stSysMalloc, 1), raw_data);

	if (!raw_data) {
		log4cplus_error("raw_data is null");
		return DTC_CODE_FAILED;
	}

	HotBackTask &hotbacktask = job.get_hot_back_task();
	int type = hotbacktask.get_type();
	if (raw_data->init(0, tabledef_->key_size(), (const char *)&type, 0, -1,
			   -1, 0)) {
		DELETE(raw_data);
		return DTC_CODE_FAILED;
	}
	DTCValue key;
	DTCValue value;
	if (0 == hotbacktask.get_packed_key_len()) {
		log4cplus_error("packedkey len is  zero");
		return DTC_CODE_FAILED;
	} else {
		key.Set(hotbacktask.get_packed_key(),
			hotbacktask.get_packed_key_len());
	}

	if (0 == hotbacktask.get_value_len()) {
		value.Set(0);
	} else {
		value.Set(hotbacktask.get_value(), hotbacktask.get_value_len());
	}

	RowValue row(tabledef_);
	row[0].u64 = type;
	row[1].u64 = hotbacktask.get_flag();
	row[2] = key;
	row[3] = value;
	log4cplus_debug(" tye is %d, flag %d", type, hotbacktask.get_flag());
	raw_data->insert_row(row, false, false);
	log_writer_->insert_header(type, 0, 1);
	log_writer_->append_body(raw_data->get_addr(), raw_data->data_size());
	DELETE(raw_data);

	log4cplus_debug(" packed key len:%d,key len:%d, key :%s", key.bin.len,
			*(unsigned char *)key.bin.ptr, key.bin.ptr + 1);
	return log_writer_->Commit();
}

int HBLog::write_lru_hb_log(DTCJobOperation &job)
{
	RawData *raw_data;
	NEW(RawData(&g_stSysMalloc, 1), raw_data);

	if (!raw_data) {
		log4cplus_error("raw_data is null");
		return DTC_CODE_FAILED;
	}

	HotBackTask &hotbacktask = job.get_hot_back_task();
	int type = hotbacktask.get_type();
	if (raw_data->init(0, tabledef_->key_size(), (const char *)&type, 0, -1,
			   -1, 0)) {
		DELETE(raw_data);
		return DTC_CODE_FAILED;
	}
	DTCValue key;
	if (0 == hotbacktask.get_packed_key_len()) {
		log4cplus_error("packedkey len is  zero");
		return DTC_CODE_FAILED;
	} else {
		key.Set(hotbacktask.get_packed_key(),
			hotbacktask.get_packed_key_len());
	}

	RowValue row(tabledef_);
	row[0].u64 = type;
	row[1].u64 = hotbacktask.get_flag();
	row[2] = key;
	row[3].Set(0);
	log4cplus_debug(" type is %d, flag %d", type, hotbacktask.get_flag());
	raw_data->insert_row(row, false, false);
	log_writer_->insert_header(BINLOG_LRU, 0, 1);
	log_writer_->append_body(raw_data->get_addr(), raw_data->data_size());
	DELETE(raw_data);

	log4cplus_debug(
		" write lru hotback log, packed key len:%d,key len:%d, key :%s",
		key.bin.len, *(unsigned char *)key.bin.ptr, key.bin.ptr + 1);
	return log_writer_->Commit();
}

int HBLog::Seek(const JournalID &v)
{
	return log_reader_->Seek(v);
}

/* Batch fetch update keys, return count of update keys */
int HBLog::task_append_all_rows(DTCJobOperation &job, int limit)
{
	int count;
	for (count = 0; count < limit; ++count) {
		/* No pending logs to process */
		if (log_reader_->Read())
			break;

		RawData *raw_data;

		NEW(RawData(&g_stSysMalloc, 0), raw_data);

		if (!raw_data) {
			log4cplus_error("allocate rawdata mem failed");
			return DTC_CODE_FAILED;
		}

		if (raw_data->check_size(g_stSysMalloc.get_handle(
						 log_reader_->record_pointer()),
					 0, tabledef_->key_size(),
					 log_reader_->record_length(0)) < 0) {
			log4cplus_error("raw data broken: wrong size");
			DELETE(raw_data);
			return DTC_CODE_FAILED;
		}

		/* attach raw data read from one binlog */
		if (raw_data->do_attach(g_stSysMalloc.get_handle(
						log_reader_->record_pointer()),
					0, tabledef_->key_size())) {
			log4cplus_error("attach rawdata mem failed");

			DELETE(raw_data);
			return DTC_CODE_FAILED;
		}

		RowValue r(tabledef_);
		r[0].u64 = *(unsigned *)raw_data->key();

		unsigned char flag = 0;
		while (raw_data->decode_row(r, flag) == 0) {
			log4cplus_debug("type: " UINT64FMT ", flag: " UINT64FMT
					", key:%s, value :%s",
					r[0].u64, r[1].u64, r[2].bin.ptr,
					r[3].bin.ptr);
			log4cplus_debug("binlog-type: %d",
					log_reader_->binlog_type());

			job.append_row(&r);
		}

		DELETE(raw_data);
	}

	return count;
}

JournalID HBLog::get_reader_jid(void)
{
	return log_reader_->query_id();
}

JournalID HBLog::get_writer_jid(void)
{
	return log_writer_->query_id();
}
