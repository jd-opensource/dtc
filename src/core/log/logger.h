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

#ifndef __DTC_LOGGER_H
#define __DTC_LOGGER_H

#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include "buffer.h"
#include "log/log.h"
#include "journal_id.h"

#define MAX_PATH_NAME_LEN 8192

/*
 * DTC binlog base class(file)
 */
class LogBase {
    public:
	LogBase();
	virtual ~LogBase();

    protected:
	int set_path(const char *path, const char *prefix);
	void file_name(char *s, int len, uint32_t serail);
	int open_file(uint32_t serial, int read);
	void close_file();
	int scan_serial(uint32_t *min, uint32_t *max);
	int stat_size(off_t *);
	int delete_file(uint32_t serial);

    private:
	LogBase(const LogBase &);

    protected:
	int _fd;

    private:
	char _path[MAX_PATH_NAME_LEN]; //Directory where log set is located
	char _prefix[MAX_PATH_NAME_LEN]; //File prefix of log set
};

class LogWriter : public LogBase {
    public:
	int open(const char *path, const char *prefix, off_t max_size,
		 uint64_t total_size);
	int write(const void *buf, size_t size);
	JournalID query();

    public:
	LogWriter();
	virtual ~LogWriter();

    private:
	int shift_file();

    private:
	off_t _cur_size; //Current log file size
	off_t _max_size; //Maximum size allowed for single log file
	uint64_t _total_size; //Maximum size allowed for log set
	uint32_t _cur_max_serial; //Current log file maximum serial number
	uint32_t _cur_min_serial; //Current log file minimum serial number
};

class LogReader : public LogBase {
    public:
	int open(const char *path, const char *prefix);
	int read(void *buf, size_t size);

	int seek(const JournalID &);
	JournalID query();

    public:
	LogReader();
	virtual ~LogReader();

    private:
	void refresh();

    private:
	uint32_t _min_serial; //Minimum file serial number of log set
	uint32_t _max_serial; //Maximum file serial number of log set
	uint32_t _cur_serial; //Current log file serial number
	off_t _cur_offset; //Current log file offset
};

/////////////////////////////////////////////////////////////////////
/*
 * generic binlog header
 */
typedef struct binlog_header {
	uint32_t length; //Length
	uint8_t version; //Version
	uint8_t type; //Type: bitmap, dtc, other
	uint8_t operater; //Operation: insert, select, update ...
	uint8_t reserve[5]; //Reserved
	uint32_t timestamp; //Timestamp
	uint32_t recordcount; //Number of sub-records
	uint8_t endof[0];
} __attribute__((__aligned__(1))) binlog_header_t;

/*
 * binlog type
 * t
 */
typedef enum binlog_type {
	BINLOG_LRU = 1,
	BINLOG_INSERT = 2,
	BINLOG_UPDATE = 4,
	BINLOG_PRUGE = 8,

} BINLOG_TYPE;

/*
 * binlog class 
 */
#define BINLOG_MAX_SIZE (100 * (1U << 20)) //100M, default single log file size
#define BINLOG_MAX_TOTAL_SIZE (3ULL << 30) //3G, default maximum log file serial number
#define BINLOG_DEFAULT_VERSION 0x02

class BinlogWriter {
    public:
	int init(const char *path, const char *prefix,
		 uint64_t total_size = BINLOG_MAX_TOTAL_SIZE,
		 off_t max_size = BINLOG_MAX_SIZE);
	int insert_header(uint8_t type, uint8_t operater, uint32_t recordcount);
	int append_body(const void *buf, size_t size);

	int Commit();
	int Abort();
	JournalID query_id();

    public:
	BinlogWriter();
	virtual ~BinlogWriter();

    private:
	BinlogWriter(const BinlogWriter &);

    private:
	LogWriter _log_writer; //Writer
	buffer _codec_buffer; //Encoding buffer
};

class BinlogReader {
    public:
	int init(const char *path, const char *prefix);

	int Read(); //Sequential read, read one binlog record each time
	int Seek(const JournalID &);
	JournalID query_id();

	uint8_t binlog_type();
	uint8_t binlog_operator();

	uint32_t record_count();
	char *record_pointer(int id = 0);
	size_t record_length(int id = 0);

    public:
	BinlogReader();
	virtual ~BinlogReader();

    private:
	BinlogReader(const BinlogReader &);

    private:
	LogReader _log_reader; //Reader
	buffer _codec_buffer; //Encoding buffer
};

#endif
