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
#ifndef __ROCKSDB_DIRECT_PROCESS_H__
#define __ROCKSDB_DIRECT_PROCESS_H__

#include <string>

class HelperProcessBase;
class RocksdbDirectListener;
class PollerBase;

class RocksdbDirectProcess {
    private:
	std::string m_domain_socket_path;
	HelperProcessBase *m_rocksdb_process;
	PollerBase *m_rocks_direct_poll;
	RocksdbDirectListener *m_listener;

    public:
	RocksdbDirectProcess(const std::string &name,
			     HelperProcessBase *processor);

	RocksdbDirectProcess(const RocksdbDirectProcess &) = delete;
	void operator=(const RocksdbDirectProcess &) = delete;

	int init();
	int run_process();

    private:
	int add_listener_to_poll();
	// int addRocksdbWorkToPoll();
};

#endif // __ROCKSDB_DIRECT_PROCESS_H__
