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
*/
#ifndef AGENT_MULTI_REQUEST_H___
#define AGENT_MULTI_REQUEST_H___

#include "list/list.h"
#include "value.h"

class DTCJobOperation;
typedef struct {
	DTCJobOperation *volatile job;
	volatile int processed;
} DecodedTask;

class ClientAgent;
class AgentMultiRequest : public ListObject<AgentMultiRequest> {
    public:
	AgentMultiRequest(DTCJobOperation *o);
	virtual ~AgentMultiRequest();

	int decode_agent_request();
	inline int packet_count()
	{
		return packetCnt;
	}
	inline DTCJobOperation *curr_task(int index)
	{
		return taskList[index].job;
	}
	void copy_reply_for_sub_task();
	void clear_owner_info();
	inline bool is_completed()
	{
		return compleTask == packetCnt;
	}
	void complete_task(int index);
	inline void detach_from_owner_client()
	{
		list_del();
	}
	inline bool is_curr_task_processed(int index)
	{
		return taskList[index].processed == 1;
	}
	inline void save_recved_result(char *buff, int len, int pktcnt,
				       uint8_t pktver)
	{
		packets.Set(buff, len);
		packetCnt = pktcnt;
		packetVersion = pktver;
	}

    private:
	DTCBinary packets;
	int packetCnt;
	uint8_t packetVersion;
	DTCJobOperation *owner;
	DecodedTask *volatile taskList;
	volatile int compleTask;
	ClientAgent *volatile owner_client_;
	void DecodeOneRequest(char *packetstart, int packetlen, int index);
};

#endif
