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
#ifndef __CLIENT__DGRAM_H__
#define __CLIENT__DGRAM_H__

#include <sys/socket.h>
#include "poll/poller.h"
#include "task/task_request.h"
#include "packet/packet.h"
#include "timer/timer_list.h"

class DTCDecoderUnit;
class ClientDgram;

struct DgramInfo {
	ClientDgram *cli;
	socklen_t len;
	char addr[0];
};

class ClientDgram : public EpollBase {
    public:
	DTCDecoderUnit *owner;

	ClientDgram(DTCDecoderUnit *, int fd);
	virtual ~ClientDgram();

	virtual int do_attach(void);
	int send_result(DTCJobOperation *, void *, int);

    protected:
	// recv on empty&no packets
	int recv_request(int noempty);

    private:
	int hastrunc;
	int mru;
	int mtu;
	int alen; // address length
	DgramInfo *abuf; // current packet address

	virtual void input_notify(void);
	int allocate_dgram_info(void);
	int init_socket_info(void);
};

#endif
