#include "async_file.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

////////////////CMapBase
CMapBase::CMapBase():
_fd(-1), _rw(0), _size(0), _map(0)
{
	bzero(_path, sizeof(_path));
	bzero(_errmsg, sizeof(_errmsg));
}

CMapBase::~CMapBase()
{
	unmount();
}

void CMapBase::unmount()
{
	close_fd();
	unmap();
}

void CMapBase::Unlink()
{
	/*
	 * After reading, truncate file to 0 and delete to prevent disk flush on startup
	 */
	if (_rw == O_RDONLY) {
		//int unused;
		//unused = ftruncate(_fd, 0);
		//unused = unlink(_path);
	}
}

int CMapBase::Mount(const char *path, int rw, int size)
{
	mkdir(ASYNC_FILE_PATH, 0777);

	if (access(ASYNC_FILE_PATH, W_OK | X_OK) < 0) {
		snprintf(_errmsg, sizeof(_errmsg), "dir(%s) Not writable",
			 ASYNC_FILE_PATH);
		return -1;
	}

	int flag = rw;
	int prot = 0;

	switch (flag) {
	case O_WRONLY:
	case O_RDWR:
	default:
		flag = O_CREAT | O_RDWR;
		prot = PROT_WRITE | PROT_READ;
		break;

	case O_RDONLY:
		prot = PROT_READ;
		break;
	}

	if ((_fd = open(path, flag, 0644)) < 0) {
		snprintf(_errmsg, sizeof(_errmsg),
			 "open failed[path:%s], errno:%d %m", path, errno);

		return -1;
	}

	struct stat st;
	fstat(_fd, &st);

	if (O_RDONLY == rw) {
		size = st.st_size;
	} else if (st.st_size != size) {
		if (ftruncate(_fd, size) != 0) {
			return -1;
		}
	}

	_map = (char *)mmap(0, size, prot, MAP_SHARED, _fd, 0);

	if (MAP_FAILED == _map) {
		snprintf(_errmsg, sizeof(_errmsg),
			 "map failed[path:%s, size:%d, _fd:%d], errno:%d %m\n",
			 path, size, _fd, errno);
		return -1;
	}

	_rw = rw;
	_size = size;
	snprintf(_path, sizeof(_path), "%s", path);

	return 0;
}

//////////////////FileImpl

int CAsyncFileImpl::OpenForWriter(CAsyncFilePos & pos)
{
	_pos = pos;

	char path[256] = { 0 };
	FileName(path, sizeof(path));

	return Mount(path, O_WRONLY, MAX_ASYNC_FILE_SIZE);
}

int CAsyncFileImpl::OpenForReader(CAsyncFilePos & pos)
{
	_pos = pos;

	char path[256] = { 0 };
	FileName(path, sizeof(path));

	return Mount(path, O_RDONLY);
}

int CAsyncFileImpl::Input(buffer & buff)
{
	if (IsWriterEnd(buff.size())) {
		WriteEndFlag();
		return CHBGlobal::ASYNC_NEED_SWTICH_FILE;
	}
	//len
	uint32_t *len = (uint32_t *) ((char *)_map + _pos.offset);
	*len = buff.size();
	_pos.Front(4);

	//buff
	memcpy((char *)_map + _pos.offset, buff.c_str(), *len);
	_pos.Front(*len);

	return CHBGlobal::ASYNC_PROCESS_OK;
}

int CAsyncFileImpl::Output(buffer & buff)
{
	if (IsReaderEnd())
		return CHBGlobal::ASYNC_NEED_SWTICH_FILE;

	//len
	uint32_t *len = (uint32_t *) ((char *)_map + _pos.offset);
	_pos.Front(4);

	//buff
	buff.append((char *)_map + _pos.offset, *len);
	_pos.Front(*len);

	return CHBGlobal::ASYNC_PROCESS_OK;
}

//////////////////Writer /////////////////////////
int CAsyncFileWriter::Open()
{
	if (_controller.Init()) {
		snprintf(_errmsg, sizeof(_errmsg), "controller init failed, %.4000s",
			 _controller.ErrorMessage());

		return CHBGlobal::ASYNC_PROCESS_ERR;
	}

	CAsyncFileImpl *p = new CAsyncFileImpl();
	if (p->OpenForWriter(_controller.WriterPos())) {
		snprintf(_errmsg, sizeof(_errmsg), "open for writer failed, %.4000s",
			 p->ErrorMessage());

		return CHBGlobal::ASYNC_PROCESS_ERR;
	}

	/*
	 * Add to writer's map file list
	 */
	AddToList(p);

	return CHBGlobal::ASYNC_PROCESS_OK;
}

int CAsyncFileWriter::Write(buffer & buf)
{
	if (_asyncfiles.size() <= 0) {
		snprintf(_errmsg, sizeof(_errmsg),
			 "__BUG__, writer maps is zero");
		return CHBGlobal::ERR_ASYNC_WRITER_LOGIC;
	}
	// Take a file from buffer to write
	int ret = _asyncfiles.front()->Input(buf);
	switch (ret) {
	case CHBGlobal::ASYNC_PROCESS_OK:
		{
			// Successfully written, update control file write pointer
			_controller.WriterPos() =
			    _asyncfiles.front()->CurrentPos();
			break;
		}

	case CHBGlobal::ASYNC_NEED_SWTICH_FILE:
		{
			// Already full, need to switch, prepare next file first, then switch
			CAsyncFilePos pos = _controller.WriterPos();
			pos.Shift();

			CAsyncFileImpl *p = new CAsyncFileImpl();
			if (p->OpenForWriter(pos)) {
				snprintf(_errmsg, sizeof(_errmsg),
					 "create CAsyncFileImpl failed, %.4000s",
					 p->ErrorMessage());

				DELETE(p);
				return CHBGlobal::ERR_ASYNC_SWTICH_FILE_ERR;
			}
			// Add to buffer
			AddToList(p);

			// Update control file
			_controller.SwitchWriterPos();

			// Continue writing
			return Write(buf);
			break;
		}
		//no default.
	}

	return CHBGlobal::ASYNC_PROCESS_OK;
}

/////////////////////////Reader ////////////////////
int CAsyncFileReader::Open()
{
	if (_controller.Init()) {
		snprintf(_errmsg, sizeof(_errmsg), "controller init failed, %.4000s",
			 _controller.ErrorMessage());

		return CHBGlobal::ASYNC_PROCESS_ERR;
	}

	_asyncfile = new CAsyncFileImpl();
	if (_asyncfile->OpenForReader(_controller.ReaderPos())) {
		snprintf(_errmsg, sizeof(_errmsg), "open for reader failed, %.4000s",
			 _asyncfile->ErrorMessage());

		return CHBGlobal::ASYNC_PROCESS_ERR;
	}

	return CHBGlobal::ASYNC_PROCESS_OK;
}

inline void CAsyncFileReader::Commit(void) {
	if(_processing) {
		_controller.ReaderPos() = _asyncfile->CurrentPos();
		_processing = 0;
	}
}

int CAsyncFileReader::Read(buffer & buff)
{
	if(!_asyncfile){
		snprintf(_errmsg, sizeof(_errmsg), "reader encounter logic error");
		return CHBGlobal::ERR_ASYNC_READER_LOGIC;
	}

	Commit();

	/* Check if file switching transient state occurs, if so suspend reader */
	if(_controller.ReaderPos().IsTransient(_controller.WriterPos())){
		return CHBGlobal::ASYNC_READER_WAIT_DATA;
	}

	/* Temporarily no more data */
	if (_controller.ReaderPos().EQ(_controller.WriterPos()))
		return CHBGlobal::ASYNC_READER_WAIT_DATA;

	/* ERROR */
	if( _controller.ReaderPos().GT(_controller.WriterPos())) {
		/* Afraid of encountering switching transient state again */
		usleep(1000);
		if( _controller.ReaderPos().GT(_controller.WriterPos())) {
			snprintf(_errmsg, sizeof(_errmsg), "reader pos is overflow");
			return CHBGlobal::ERR_ASYNC_READER_OVERFLOW;
		}
	}

	int ret = _asyncfile->Output(buff);
	switch (ret) {

	case CHBGlobal::ASYNC_PROCESS_OK:
		{
			// mark as processing, delay commit
			_processing = 1;
			
			// Update control file read pointer
			//_controller.ReaderPos() = _asyncfile->CurrentPos();
			break;
		}
	case CHBGlobal::ASYNC_NEED_SWTICH_FILE:
		{
			CAsyncFilePos pos = _controller.ReaderPos();
			pos.Shift();

			//delete file
			_asyncfile->Unlink();
			DELETE(_asyncfile);

			_asyncfile = new CAsyncFileImpl();
			if (_asyncfile->OpenForReader(pos)) {
				snprintf(_errmsg, sizeof(_errmsg),
					 "create CAsyncFileImpl failed, %.4000s",
					 _asyncfile->ErrorMessage());

				DELETE(_asyncfile);
				return CHBGlobal::ERR_ASYNC_SWTICH_FILE_ERR;
			}

			_controller.SwitchReaderPos();

			return Read(buff);
			break;
		}
	}

	return CHBGlobal::ASYNC_PROCESS_OK;
}

int CAsyncFileChecker::Check()
{
	if (_controller.Init()) {
		snprintf(_errmsg, sizeof(_errmsg), "controller init failed");
		return CHBGlobal::ERR_ASYNC_CONTROLLER_ERR;
	}

	if (_controller.IsDirty()) {
		snprintf(_errmsg, sizeof(_errmsg),
			 "full sync is not complete, status is dirty");
		return CHBGlobal::ERR_FULL_SYNC_NOT_COMPLETE;
	}

	/* Check if reader is faster than writer */
	if (_controller.ReaderPos().GT(_controller.WriterPos())) {
		snprintf(_errmsg, sizeof(_errmsg), "reader pos is overflow");
		return CHBGlobal::ERR_ASYNC_READER_OVERFLOW;
	}

	/* Check reader validity */
	if (!_controller.ReaderPos().Zero()) {
		_asyncfile = new CAsyncFileImpl;
		if (_asyncfile->OpenForReader(_controller.ReaderPos())) {
			snprintf(_errmsg, sizeof(_errmsg),
				 "reader pos is error");
			return CHBGlobal::ERR_ASYNC_READER_POS_ERR;
		}

		if (_controller.ReaderPos().offset >
		    (unsigned)_asyncfile->Size() + 4) {
			snprintf(_errmsg, sizeof(_errmsg),
				 "reader pos is error");
			return CHBGlobal::ERR_ASYNC_READER_POS_ERR;
		}
	}

	DELETE(_asyncfile);

	/* Check writer validity */
	if (!_controller.WriterPos().Zero()) {
		_asyncfile = new CAsyncFileImpl;
		if (_asyncfile->OpenForWriter(_controller.WriterPos())) {
			snprintf(_errmsg, sizeof(_errmsg),
				 "writer pos is error");
			return CHBGlobal::ERR_ASYNC_WRITER_POS_ERR;
		}

		if (_controller.WriterPos().offset >
		    (unsigned)_asyncfile->Size() + 4) {
			snprintf(_errmsg, sizeof(_errmsg),
				 "writer pos is error");
			return CHBGlobal::ERR_ASYNC_WRITER_POS_ERR;
		}
	}

	DELETE(_asyncfile);
	return 0;
}

/*
 * Test code
 */
#ifdef __UNIT_TEST__

#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
int W()
{
	buffer buff;
	buff.append("just for test");

	CAsyncFileWriter writer(3);
	if (writer.Open()) {
		printf("writer Open failed\n");
		return -1;
	}

	while (1) {
		writer.Write(buff);
		//usleep(1000);
	}

	return 0;
}

int R()
{

	CAsyncFileReader reader;
	if (reader.Open()) {
		printf("reader Open failed\n");
		return -1;
	}
	while (1) {
		buffer buff;
		int ret = reader.Read(buff);
		if (ret == CHBGlobal::ASYNC_READER_WAIT_DATA) {
			printf("no data. sleep 1 \n");
			sleep(1);
		} else {
			printf("error, \n");
			exit(0);
		}
		//                printf("buff.len=%d, buff.content=%9s\n", buff.size(), buff.c_str());
	}

	return 0;
}

int main()
{
	int ret = 0;
	CAsyncFileChecker check;
	if (ret = check.Check()) {
		printf("check not pass, ret=%d\n", ret);
		return -1;
	}

	int pid = fork();
	if (pid < 0) {
		printf("fork child failed: %m\n");
		return -1;
	} else if (pid > 0) {
		nice(5);
		W();
	} else {
		// Let writer run first, otherwise there's no file and reader will error
		sleep(3);
		R();
	}

	return 0;
}
#endif
