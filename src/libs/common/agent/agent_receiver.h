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
#ifndef __AGENT_RECEIVER_H__
#define __AGENT_RECEIVER_H__

#include <stdint.h>

#include "value.h"
#include "protocol.h"

#define AGENT_INIT_RECV_BUFF_SIZE 4096

typedef struct {
	char *buff;
	int len;
	int pktCnt;
	int err;
	uint8_t version;
} RecvedPacket;

class AgentReceiver {
    public:
	AgentReceiver(int f);
	virtual ~AgentReceiver();

	int initialization();
	RecvedPacket receive_network_packet();

    private:
	int fd;
	char *buffer;
	uint32_t offset;
	uint32_t buffSize;
	uint32_t pktTail;
	int pktCnt;

	int enlarge_buffer();
	bool is_need_enlarge_buffer();
	int recv_once();
	int real_recv();
	int recv_again();
	int decode_header_v1(DTC_HEADER_V1 *header);
	void set_recved_info(RecvedPacket &packet);
	int count_packet_v1();
	int count_packet_v2();
};

#endif
