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

#include "pt_malloc.h"
#include "dtc_api.h"

class Defragment {
    public:
	Defragment()
	{
		_mem = NULL;
		_pstChunk = NULL;
		_keysize = -1;
		_s = NULL;
		_error_count = 0;
		_skip_count = 0;
		_ok_count = 0;
		_bulk_per_ten_microscoends = 1;
	}

	~Defragment()
	{
	}
	int do_attach(const char *key, int keysize, int step);
	char *get_key_by_handle(INTER_HANDLE_T handle, int *len);
	int proccess(INTER_HANDLE_T handle);
	int dump_mem(bool verbose = false);
	int dump_mem_new(const char *filename, uint64_t &memsize);
	int defrag_mem(int level, DTC::Server *s);
	int defrag_mem_new(int level, DTC::Server *s, const char *filename,
			   uint64_t memsize);
	int proccess_handle(const char *filename, DTC::Server *s);
	void frequency_limit(void);

    private:
	PtMalloc *_mem;
	MallocChunk *_pstChunk;
	int _keysize;
	DTC::Server *_s;

	//stat
	uint64_t _error_count;
	uint64_t _skip_count;
	uint64_t _ok_count;
	int _bulk_per_ten_microscoends;
};

#define SEARCH 0
#define MATCH 1
class DefragMemAlgo {
    public:
	DefragMemAlgo(int level, Defragment *master);
	~DefragMemAlgo();
	int Push(INTER_HANDLE_T handle, int used);

    private:
	int _status;
	INTER_HANDLE_T *_queue;
	Defragment *_master;
	int _count;
	int _level;
};
