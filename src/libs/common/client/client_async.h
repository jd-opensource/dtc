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
#ifndef __CLIENT__ASYNC_H__
#define __CLIENT__ASYNC_H__

#include "poll/poller.h"
#include "task/task_request.h"
#include "packet/packet.h"
#include "timer/timer_list.h"
#include "list/list.h"

class DTCDecoderUnit;
class ClientAsync;

class AsyncInfo : private ListObject<AsyncInfo> {
    public:
	AsyncInfo(ClientAsync *c, DTCJobOperation *r);
	~AsyncInfo();
	void list_move_tail(ListObject<AsyncInfo> *a)
	{
		ListObject<AsyncInfo>::list_move_tail(a);
	}

	ClientAsync *cli;
	DTCJobOperation *req;
	Packet *pkt;
};

class ClientAsync : public EpollBase {
    public:
	friend class AsyncInfo;
	DTCDecoderUnit *owner;

	ClientAsync(DTCDecoderUnit *, int fd, int depth, void *peer, int ps);
	virtual ~ClientAsync();

	virtual int do_attach(void);
	int queue_result(AsyncInfo *);
	int queue_error(void);
	int flush_result(void);
	int adjust_events(void);

	virtual void input_notify(void);

    private:
	virtual void output_notify(void);

	int recv_request(void);
	int Response(void);
	int response_one(void);

    protected:
	SimpleReceiver receiver;
	DTCJobOperation *curReq; // decoding
	Packet *curRes; // sending

	ListObject<AsyncInfo> waitList;
	ListObject<AsyncInfo> done_list;

	void *addr;
	int addrLen;
	int maxReq;
	int numReq;
};

#endif
