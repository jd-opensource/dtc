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
#ifndef __CLIENT_SYNC_H__
#define __CLIENT_SYNC_H__

#include "poll/poller.h"
#include "timer/timer_list.h"
#include "receiver.h"

class DTCDecoderUnit;
class DTCJobOperation;
class Packet;
class ClientResourceSlot;

class ClientSync : public EpollBase, private TimerObject {
    public:
	DTCDecoderUnit *owner;
	void *addr;
	int addrLen;
	unsigned int resource_id;
	ClientResourceSlot *resource;
	uint32_t resource_seq;
	enum RscStatus {
		RscClean,
		RscDirty,
	};
	RscStatus rscStatus;

	ClientSync(DTCDecoderUnit *, int fd, void *, int);
	virtual ~ClientSync();

	virtual int do_attach(void);
	int send_result(void);

	virtual void input_notify(void);

    private:
	virtual void output_notify(void);

	int get_resource();
	void free_resource();
	void clean_resource();

    protected:
	enum ClientState {
		IdleState,
		RecvReqState, //wait for recv request, server side
		SendRepState, //wait for send response, server side
		ProcReqState, // IN processing
	};

	SimpleReceiver receiver;
	ClientState stage;
	DTCJobOperation *job;
	Packet *reply;

	int recv_request(void);
	int Response(void);
	void adjust_events(void);
};

#endif
