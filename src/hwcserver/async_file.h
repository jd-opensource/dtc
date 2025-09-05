/*
 * =====================================================================================
 *
 *       Filename:  async_file.h
 *
 *    Description:  async_file class definition.
 *
 *        Version:  1.0
 *        Created:  04/01/2021
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  chenyujie, chenyujie28@jd.com@jd.com
 *        Company:  JD.com, Inc.
 *
 * =====================================================================================
 */

#ifndef __HBP_ASYNC_FILE_H
#define __HBP_ASYNC_FILE_H

#include <list>
#include <stdint.h>
#include <dirent.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
// local
#include "hwc_global.h"
#include "afile_pos.h"
// common
#include "journal_id.h"
#include "buffer.h"
#include "log/log.h"

class CMapBase {
public:
	CMapBase();
	virtual ~ CMapBase();

public:
	int Mount(const char *path, int rw, int size = 0);
	void Unlink();
	const int Size() {
		return _size;
	} const char *ErrorMessage() {
		return _errmsg;
	}

private:
	void unmount();

	inline void close_fd() {
		if (_fd > 0) {
			close(_fd);
			_fd = -1;
		}
	}

	inline void unmap() {
		if (_map) {
			munmap((void *)_map, _size);
			_map = 0;
			_size = 0;
		}
	}

protected:
	int _fd;
	int _rw;
	int _size;
	char _path[256];
	volatile char *_map;

	char _errmsg[8192];
};

enum ESyncStatus
{
	E_SYNC_PURE = 0x00,
	E_SYNC_ROCKSDB_FULL_SYNC_ING = 0x01,
	E_SYNC_ROCKSDB_FULL_SYNC_FINISH = 0x02,
	E_SYNC_BINLOG_SYNC_ING = 0x03
};

/*
 * Async Controller file struct
 */

/* FIXME: Must be 8 bytes aligned */
struct CControl {
	JournalID jid;
	CReaderPos rpos;
	CWriterPos wpos;
	uint64_t flag;		/* dirty flag: whether full sync is completed */
};

/*
 * Async File Controller
 */
class CAsyncFileController : public CMapBase {
public:
	CAsyncFileController():CMapBase() {
	} virtual ~CAsyncFileController() {
	}

	inline int Init(const char *path = ASYNC_FILE_CONTROLLER) {
		return Mount(path, O_RDWR, ASYNC_FILE_CONTROLLER_SIZE);
	}

	inline void SwitchWriterPos() {
		WriterPos().Shift();
	}

	inline void SwitchReaderPos() {
		ReaderPos().Shift();
	}

	inline CReaderPos & ReaderPos() {
		CControl *p = (CControl *) _map;
		return p->rpos;
	}

	inline CWriterPos & WriterPos() {
		CControl *p = (CControl *) _map;
		return p->wpos;
	}

	inline JournalID & JournalId() {
		CControl *p = (CControl *) _map;
		return p->jid;
	}

	inline int IsDirty() {
		return (E_SYNC_ROCKSDB_FULL_SYNC_ING == DirtyFlag());
	}

	inline int GetDirty(){
		return DirtyFlag();
	}

	inline void SetDirty(int iState) {
		DirtyFlag() = iState;
	}

	inline void ClrDirty() {
		DirtyFlag() = E_SYNC_PURE;
	}

private:
	/*
	 * Currently only used to indicate whether full-sync is completed
	 */
	inline uint64_t & DirtyFlag() {
		CControl *p = (CControl *) _map;
		return p->flag;
	}
};

/*
 * Async File Implementation
 */
class CAsyncFileImpl:public CMapBase {
public:
	CAsyncFileImpl():CMapBase(), _pos() {
	} ~CAsyncFileImpl() {
	}

	int OpenForReader(CAsyncFilePos &);
	int OpenForWriter(CAsyncFilePos &);

	int Input(buffer &);
	int Output(buffer &);

	CAsyncFilePos & CurrentPos() {
		return _pos;
	}

private:

	inline void WriteEndFlag() {
		uint32_t *flag = (uint32_t *) ((char *)_map + _pos.offset);

		*flag = ASYNC_FILE_END_FLAG;
		_pos.Front(4);

		return;
	}

	inline int IsWriterEnd(int len) {
		/*
		 * 4-byte length + 4-byte end flag
		 */
		if (_pos.offset + len + 4 + 4 >= (unsigned)Size())
			return 1;

		return 0;
	}

	inline int IsReaderEnd() {
		uint32_t *flag = (uint32_t *) ((char *)_map + _pos.offset);

		if (*flag == ASYNC_FILE_END_FLAG)
			return 1;

		return 0;
	}

	inline void FileName(char *s, int len) {
		snprintf(s, len, ASYNC_FILE_NAME "%d", _pos.serial);
	}

private:
	CAsyncFilePos _pos;
};

/*
 * Writer
 */
class CAsyncFileWriter {
public:

	CAsyncFileWriter(int max = ASYNC_WRITER_MAP_FILES): _max(max) {
		bzero(_errmsg, sizeof(_errmsg));
	}

	~CAsyncFileWriter() {
		std::list < CAsyncFileImpl * >::iterator it, p;
		for (it = _asyncfiles.begin(); it != _asyncfiles.end();) {
			p = it;
			++it;

			DELETE(*p);
		}
	}

	int Open(void);
	int Write(buffer &);

	JournalID & JournalId(void) {
		return _controller.JournalId();
	}

	const char *ErrorMessage(void) {
		return _errmsg;
	}

private:

	inline void AddToList(CAsyncFileImpl * p) {
		// Control map files to a certain number, otherwise may cause disk flush
		if (_asyncfiles.size() >= (unsigned)_max)
			DropLastOne();

		_asyncfiles.push_front(p);
	}

	/*
	 * Unmap the oldest file held by writer
	 */
	inline void DropLastOne() {
		CAsyncFileImpl *p = _asyncfiles.back();
		DELETE(p);

		_asyncfiles.pop_back();
	}

private:
	std::list < CAsyncFileImpl * >_asyncfiles;
	CAsyncFileController _controller;
	int _max;
	char _errmsg[8192];
};

/*
 * Reader
 */
class CAsyncFileReader {
public:
	CAsyncFileReader(): _asyncfile(0), _processing(0) {
		bzero(_errmsg, sizeof(_errmsg));
	} ~CAsyncFileReader() {
	}

	int Open();
	int Read(buffer &);
	void Commit();

	const char *ErrorMessage(void) {
		return _errmsg;
	}
private:
	CAsyncFileImpl * _asyncfile;
	CAsyncFileController _controller;
	char _errmsg[8192];
	int _processing;
};

/*
 * Check log validity
 */
class CAsyncFileChecker {
public:
	CAsyncFileChecker():_asyncfile(0) {
		bzero(_errmsg, sizeof(_errmsg));
	} ~CAsyncFileChecker() {
	}

	int Check();

	const char *ErrorMessage() {
		return _errmsg;
	}
private:
	CAsyncFileImpl * _asyncfile;
	CAsyncFileController _controller;
	char _errmsg[8192];
};

#endif
