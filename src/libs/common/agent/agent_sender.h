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
#ifndef __AGENT_SENDER_H__
#define __AGENT_SENDER_H__

#include <stdint.h>

#define SENDER_MAX_VEC 1024

class Packet;
class AgentSender {
    private:
	int fd;
	struct iovec *vec;
	uint32_t totalVec;
	uint32_t currVec;
	Packet **packet;
	uint32_t totalPacket;
	uint32_t currPacket;

	uint32_t totalLen;
	uint32_t sended;
	uint32_t leftLen;

	int broken;

    public:
	AgentSender(int f);
	virtual ~AgentSender();

	int initialization();
	int is_broken()
	{
		return broken;
	}
	int add_packet(Packet *pkt);
	int send_packet();
	inline uint32_t total_len()
	{
		return totalLen;
	}
	inline uint32_t Sended()
	{
		return sended;
	}
	inline uint32_t left_len()
	{
		return leftLen;
	}
	inline uint32_t vec_count()
	{
		return currVec;
	}
};

#endif
