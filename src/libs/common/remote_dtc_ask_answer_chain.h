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
#ifndef DYNAMIC_HELPER_COLLECTION_H__
#define DYNAMIC_HELPER_COLLECTION_H__

#include <map>
#include <string>
#include "task/task_request.h"

class ConnectorGroup;
class PollerBase;
class TimerList;

class RemoteDtcAskAnswerChain : public JobAskInterface<DTCJobOperation>,
				public JobAnswerInterface<DTCJobOperation>,
				public TimerObject {
    public:
	RemoteDtcAskAnswerChain(PollerBase *owner, int clientPerGroup);
	~RemoteDtcAskAnswerChain();

	void set_timer_handler(TimerList *recv, TimerList *conn,
			       TimerList *retry);

    private:
	virtual void job_ask_procedure(DTCJobOperation *t);
	virtual void job_answer_procedure(DTCJobOperation *t);
	virtual void job_timer_procedure();

	struct helper_group {
		ConnectorGroup *group;
		int used;
	};

	typedef std::map<std::string, helper_group> HelperMapType;
	HelperMapType m_groups;
	LinkQueue<DTCJobOperation *>::allocator m_task_queue_allocator;
	TimerList *m_recv_timer_list, *m_conn_timer_list, *m_retry_timer_list;
	int m_client_per_group;
};

#endif
