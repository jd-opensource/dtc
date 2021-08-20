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
#ifndef __ROCKSDB_DIRECT_LISTENER_H__
#define __ROCKSDB_DIRECT_LISTENER_H__

#include "socket/socket_addr.h"
#include "poll/poller.h"

#include <string>

class HelperProcessBase;
class PollerBase;

class RocksdbDirectListener : public EpollBase {
    private:
	std::string m_domain_socket_path;
	HelperProcessBase *m_rocksdb_process;
	PollerBase *m_poller_thread;

    public:
	RocksdbDirectListener(const std::string &name,
			      HelperProcessBase *processor, PollerBase *poll);

	virtual ~RocksdbDirectListener();

	int do_bind();
	int attach_thread();
	virtual void input_notify(void);

    private:
	// void init();
};

#endif // __ROCKSDB_DIRECT_LISTENER_H__
