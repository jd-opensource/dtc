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
#ifndef __AGENT_CLIENT_H__
#define __AGENT_CLIENT_H__

#include <pthread.h>

#include "poll/poller.h"
#include "timer/timer_list.h"
#include "queue/lqueue.h"
#include "value.h"
#include "agent_receiver.h"

class Packet;
class AgentResultQueue {
    public:
	LinkQueue<Packet *> packet;

	AgentResultQueue()
	{
	}
	~AgentResultQueue();

	inline void Push(Packet *pkt)
	{
		packet.Push(pkt);
	}
	inline Packet *Pop()
	{
		return packet.Pop();
	}
	inline Packet *Front()
	{
		return packet.Front();
	}
	inline bool queue_empty()
	{
		return packet.queue_empty();
	}
};

class PollerBase;
class JobEntranceAskChain;
class AgentReceiver;
class AgentSender;
class AgentMultiRequest;
class AgentMultiRequest;
class DTCJobOperation;
class ClientAgent : public EpollBase, public TimerObject {
    public:
	ClientAgent(PollerBase *o, JobEntranceAskChain *u, int fd);
	virtual ~ClientAgent();

	int attach_thread();
	inline void add_packet(Packet *p)
	{
		resQueue.Push(p);
	}
	void remember_request(AgentMultiRequest *agentrequest);
	int send_result();
	void record_request_process_time(DTCJobOperation *job);

	virtual void input_notify();
	virtual void output_notify();

    private:
	PollerBase *ownerThread;
	JobEntranceAskChain *owner;
	TimerList *tlist;

	AgentReceiver *receiver;
	AgentSender *sender;
	AgentResultQueue resQueue;
	ListObject<AgentMultiRequest> rememberReqHeader;

	DTCJobOperation *parse_job_message(char *recvbuff, int recvlen,
					   int pktcnt, uint8_t pktver);
	int recv_request();
	void remember_request(DTCJobOperation *request);
};

#endif
