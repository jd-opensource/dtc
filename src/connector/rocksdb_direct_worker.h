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

#ifndef __ROCKSDB_DIRECT_WORKER_H__
#define __ROCKSDB_DIRECT_WORKER_H__

#include "rocksdb_direct_context.h"
#include "poll/poller.h"

class HelperProcessBase;
class PollerBase;

class RocksdbDirectWorker : public EpollBase {
    private:
	HelperProcessBase *m_db_process_rocks;
	DirectRequestContext m_request_context;
	DirectResponseContext m_response_context;

    public:
	RocksdbDirectWorker(HelperProcessBase *processor, PollerBase *poll,
			    int fd);

	virtual ~RocksdbDirectWorker();

	int add_event_to_poll();
	virtual void input_notify(void);

    private:
	int receive_request();
	void proc_direct_request();
	void send_response();
	int remove_from_event_poll();

	int receive_message(char *data, int data_len);

	int send_message(const char *data, int data_len);

	bool condtion_priority(const ConditionOperation lc,
			       const ConditionOperation rc);
};

#endif // __ROCKSDB_DIRECT_WORKER_H__
