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

#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <time.h>
#include <strings.h>
#include "logger.h"
#include "log/log.h"
#include "global.h"

LogBase::LogBase() : _fd(-1)
{
	bzero(_path, sizeof(_path));
	bzero(_prefix, sizeof(_prefix));
}

LogBase::~LogBase()
{
	close_file();
}

int LogBase::set_path(const char *path, const char *prefix)
{
	snprintf(_path, sizeof(_path), "%s", path);
	snprintf(_prefix, sizeof(_prefix), "%s", prefix);

	mkdir(_path, 0777);

	if (access(_path, W_OK | X_OK) < 0) {
		log4cplus_error("dir(%s) Not writable", _path);
		return -1;
	}

	return 0;
}

void LogBase::close_file()
{
	if (_fd > 0) {
		::close(_fd);
		_fd = -1;
	}
}

int LogBase::stat_size(off_t *s)
{
	struct stat st;
	if (fstat(_fd, &st))
		return -1;

	*s = st.st_size;
	return 0;
}

int LogBase::delete_file(uint32_t serial)
{
	char file[MAX_PATH_NAME_LEN] = { 0 };
	file_name(file, MAX_PATH_NAME_LEN, serial);

	return unlink(file);
}

int LogBase::open_file(uint32_t serial, int read)
{
	char file[MAX_PATH_NAME_LEN] = { 0 };
	file_name(file, MAX_PATH_NAME_LEN, serial);

	read ? _fd = ::open(file, O_RDONLY | O_LARGEFILE, 0644) :
	       _fd = ::open(file,
			    O_WRONLY | O_APPEND | O_CREAT | O_LARGEFILE |
				    O_TRUNC,
			    0644);

	if (_fd < 0) {
		log4cplus_debug("open file[%s] failed, %m", file);
		return -1;
	}

	return 0;
}

int LogBase::scan_serial(uint32_t *min, uint32_t *max)
{
	DIR *dir = opendir(_path);
	if (!dir)
		return -1;

	struct dirent *drt = readdir(dir);
	if (!drt) {
		closedir(dir);
		return -2;
	}

	*min = (uint32_t)((1ULL << 32) - 1);
	*max = 0;

	char prefix[MAX_PATH_NAME_LEN] = { 0 };
	int prefix_len = strlen(_prefix);
	if (prefix_len + 8 >= MAX_PATH_NAME_LEN) {
		return -1;
	}
	// Use safer string operations to avoid truncation warnings
	strncpy(prefix, _prefix, MAX_PATH_NAME_LEN - 9);
	strcat(prefix, ".binlog.");

	int l = strlen(prefix);
	uint32_t v = 0;
	int found = 0;

	for (; drt; drt = readdir(dir)) {
		int n = strncmp(drt->d_name, prefix, l);
		if (n == 0) {
			v = strtoul(drt->d_name + l, NULL, 10);
			v >= 1 ? (*max < v ? *max = v : v),
				(v < *min ? *min = v : v) : v;
			found = 1;
		}
	}

	found ? *max : (*max = 0, *min = 0);

	log4cplus_debug("scan serial: min=%u, max=%u\n", *min, *max);

	closedir(dir);
	return 0;
}

void LogBase::file_name(char *s, int len, unsigned serial)
{
	snprintf(s, len, "%s/%s.binlog.%u", _path, _prefix, serial);
}

LogWriter::LogWriter()
	: LogBase(), _cur_size(0), _max_size(0), _total_size(0),
	  _cur_max_serial(0), //serial start 0
	  _cur_min_serial(0) //serial start 0
{
}

LogWriter::~LogWriter()
{
}

int LogWriter::open(const char *path, const char *prefix, off_t max_size,
		    uint64_t total_size)
{
	if (set_path(path, prefix))
		return -1;

	_max_size = max_size;
	_total_size = total_size;

	if (scan_serial(&_cur_min_serial, &_cur_max_serial)) {
		log4cplus_debug("scan file serial failed, %m");
		return -1;
	}

	_cur_max_serial += 1; //skip current binlog file.
	return open_file(_cur_max_serial, 0);
}

int LogWriter::write(const void *buf, size_t size)
{
	unsigned int unused;

	unused = ::write(_fd, buf, size);
	if (unused != size) {
		//		log4cplus_error("wirte hblog[input size %u, write success size %d] err", size, unused);
		log4cplus_error(
			"wirte hblog[input size %u, write success size %u] err, %m",
			(unsigned int)size, unused);
	}
	_cur_size += size;
	return shift_file();
}

JournalID LogWriter::query()
{
	JournalID v(_cur_max_serial, _cur_size);
	return v;
}

int LogWriter::shift_file()
{
	int need_shift = 0;
	int need_delete = 0;

	if (_cur_size >= _max_size)
		need_shift = 1;
	else
		return 0;

	uint64_t total = _cur_max_serial - _cur_min_serial;
	total *= _max_size;

	if (total >= _total_size) {
		need_delete = 1;
	}

	log4cplus_debug("shift file: cur_size:" UINT64FMT
			", total_size:" UINT64FMT ",  \
			shift:%d, cur_min_serial=%u, cur_max_serial=%u\n",
			total, _total_size, need_shift, _cur_min_serial,
			_cur_max_serial);

	if (need_shift) {
		if (need_delete) {
			delete_file(_cur_min_serial);
			_cur_min_serial += 1;
		}

		close_file();

		_cur_size = 0;
		_cur_max_serial += 1;
	}

	return open_file(_cur_max_serial, 0);
}

LogReader::LogReader()
	: LogBase(), _min_serial(0), _max_serial(0), _cur_serial(0),
	  _cur_offset(0)
{
}

LogReader::~LogReader()
{
}

int LogReader::open(const char *path, const char *prefix)
{
	if (set_path(path, prefix))
		return -1;

	//refresh directory
	refresh();

	_cur_serial = _min_serial;
	_cur_offset = 0;

	return open_file(_cur_serial, 1);
}

void LogReader::refresh()
{
	scan_serial(&_min_serial, &_max_serial);
}

int LogReader::read(void *buf, size_t size)
{
	ssize_t rd = ::read(_fd, buf, size);
	if (rd == (ssize_t)size) {
		_cur_offset += rd;
		return 0;
	} else if (rd < 0) {
		return -1;
	}

	// If there is a larger serial, discard the buf content and switch files. Otherwise, revert the file pointer
	refresh();

	if (_cur_serial < _max_serial) {
		_cur_serial += 1;
		_cur_offset = 0;

		close_file();
		// Skip files with non-existent serial numbers
		while (open_file(_cur_serial, 1) == -1 &&
		       _cur_serial < _max_serial)
			_cur_serial += 1;

		if (_fd > 0 && _cur_serial <= _max_serial)
			return read(buf, size);
		else
			return -1;
	}

	// Revert file pointer
	if (rd > 0) {
		seek(JournalID(_cur_serial, _cur_offset));
	}

	return -1;
}

JournalID LogReader::query()
{
	JournalID v(_cur_serial, _cur_offset);
	return v;
}

int LogReader::seek(const JournalID &v)
{
	char file[MAX_PATH_NAME_LEN] = { 0 };
	file_name(file, MAX_PATH_NAME_LEN, v.serial);

	/* Ensure the file exists */
	if (access(file, F_OK))
		return -1;

	if (v.serial != _cur_serial) {
		close_file();

		if (open_file(v.serial, 1) == -1) {
			log4cplus_debug("hblog %u not exist, seek failed",
					v.serial);
			return -1;
		}
	}

	log4cplus_debug("open serial=%u, %m", v.serial);

	off_t file_size = 0;
	stat_size(&file_size);

	if (v.offset > (uint32_t)file_size)
		return -1;

	lseek(_fd, v.offset, SEEK_SET);

	_cur_offset = v.offset;
	_cur_serial = v.serial;
	return 0;
}

BinlogWriter::BinlogWriter() : _log_writer()

{
}

BinlogWriter::~BinlogWriter()
{
}

int BinlogWriter::init(const char *path, const char *prefix, uint64_t total,
		       off_t max_size)
{
	return _log_writer.open(path, prefix, max_size, total);
}

#define struct_sizeof(t) sizeof(((binlog_header_t *)NULL)->t)
#define struct_typeof(t) typeof(((binlog_header_t *)NULL)->t)

int BinlogWriter::insert_header(uint8_t type, uint8_t operater, uint32_t count)
{
	_codec_buffer.clear();

	_codec_buffer.expand(offsetof(binlog_header_t, endof));

	_codec_buffer << (struct_typeof(length))0; //length
	_codec_buffer
		<< (struct_typeof(version))BINLOG_DEFAULT_VERSION; //version
	_codec_buffer << (struct_typeof(type))type; //type
	_codec_buffer << (struct_typeof(operater))operater; //operator
	_codec_buffer.append("\0\0\0\0\0", 5); //reserve char[5]
	_codec_buffer << (struct_typeof(timestamp))(time(NULL)); //timestamp
	_codec_buffer << (struct_typeof(recordcount))count; //recordcount

	return 0;
}

int BinlogWriter::append_body(const void *buf, size_t size)
{
	_codec_buffer.append((char *)&size, struct_sizeof(length));
	_codec_buffer.append((const char *)buf, size);

	return 0;
}

int BinlogWriter::Commit()
{
	// Calculate total length
	uint32_t total = _codec_buffer.size();
	total -= struct_sizeof(length);

	// Write total length
	struct_typeof(length) *length =
		(struct_typeof(length) *)(_codec_buffer.c_str());
	*length = total;

	return _log_writer.write(_codec_buffer.c_str(), _codec_buffer.size());
}

int BinlogWriter::Abort()
{
	_codec_buffer.clear();
	return 0;
}

JournalID BinlogWriter::query_id()
{
	return _log_writer.query();
}

BinlogReader::BinlogReader() : _log_reader()
{
}
BinlogReader::~BinlogReader()
{
}

int BinlogReader::init(const char *path, const char *prefix)
{
	return _log_reader.open(path, prefix);
}

int BinlogReader::Read()
{
	/* prepare buffer */
	if (_codec_buffer.resize(struct_sizeof(length)) < 0) {
		log4cplus_error("expand _codec_buffer failed");
		return -1;
	}
	/* read length part of one binlog */
	if (_log_reader.read(_codec_buffer.c_str(), struct_sizeof(length)))
		return -1;

	struct_typeof(length) len =
		*(struct_typeof(length) *)_codec_buffer.c_str();
	if (len < 8 || len >= (1 << 20) /*1M*/) {
		// filter some out of range length,
		// prevent client sending invalid jid crash server
		return -1;
	}
	_codec_buffer.resize(len + struct_sizeof(length));
	if (_log_reader.read(_codec_buffer.c_str() + struct_sizeof(length),
			     len))
		return -1;

	return 0;
}

JournalID BinlogReader::query_id()
{
	return _log_reader.query();
}

int BinlogReader::Seek(const JournalID &v)
{
	return _log_reader.seek(v);
}

uint8_t BinlogReader::binlog_type()
{
	return ((binlog_header_t *)(_codec_buffer.c_str()))->type;
}

uint8_t BinlogReader::binlog_operator()
{
	return ((binlog_header_t *)(_codec_buffer.c_str()))->operater;
}

uint32_t BinlogReader::record_count()
{
	return ((binlog_header_t *)(_codec_buffer.c_str()))->recordcount;
}

/*
 * binlog format:
 *
 * =====================================================
 *  binlog_header_t | len1 | record1 | len2 | record2 | ...
 * =====================================================
 *
 */
char *BinlogReader::record_pointer(int id)
{
	//record start
	char *p = (char *)(_codec_buffer.c_str() +
			   offsetof(binlog_header_t, endof));
	char *m = 0;
	uint32_t l = struct_sizeof(length);
	uint32_t ll = 0;

	for (int i = 0; i <= id; i++) {
		m = p + l;
		ll = *(struct_typeof(length) *)(m - struct_sizeof(length));
		l += (ll + struct_sizeof(length));
	}

	return m;
}

size_t BinlogReader::record_length(int id)
{
	char *p = (char *)(_codec_buffer.c_str() +
			   offsetof(binlog_header_t, endof));
	uint32_t ll, l;
	l = ll = 0;

	for (int i = 0; i <= id; i++) {
		l = *(struct_typeof(length) *)(p + ll);
		ll += (l + struct_sizeof(length));
	}

	return l;
}
