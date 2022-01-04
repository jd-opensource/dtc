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
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <string.h>

#include "agent_receiver.h"
#include "log/log.h"
#include "mem_check.h"
#include "protocol.h"
#include "task/task_request.h"

AgentReceiver::AgentReceiver(int f)
	: fd(f), buffer(NULL), offset(0), buffSize(0), pktTail(0), pktCnt(0)
{
}

AgentReceiver::~AgentReceiver()
{
	/* fd will closed by ClientAgent */
	if (buffer)
		free(buffer);
}

int AgentReceiver::initialization()
{
	buffer = (char *)malloc(AGENT_INIT_RECV_BUFF_SIZE);
	if (NULL == buffer)
		return -1;
	buffSize = AGENT_INIT_RECV_BUFF_SIZE;
	return 0;
}

/*
discription: recv
main_chain:
<0: recv error or remote close
=0: nothing recved
>0: recved 
*/
int AgentReceiver::recv_once()
{
	int rv;

	rv = recv(fd, offset + buffer, buffSize - offset, 0);
	if (rv < 0) {
		if (EAGAIN == errno || EINTR == errno || EINPROGRESS == errno)
			return 0;
		log4cplus_error("agent receiver recv error: %m, %d", errno);
		return -errno;
	} else if (0 == rv) {
		log4cplus_debug("remote close connection, fd[%d]", fd);
		errno = ECONNRESET;
		return -errno;
	}

	offset += rv;

	return rv;
}

/*
return value:
<0: error
=0: nothing recved or have no completed packet
>0: recved and have completed packet
*/
RecvedPacket AgentReceiver::receive_network_packet()
{
	int err = 0;
	RecvedPacket packet;

	memset((void *)&packet, 0, sizeof(packet));

	if ((err = real_recv()) < 0) {
		packet.err = -1;
		return packet;
	} else if (err == 0)
		return packet;

	if ((err = recv_again()) < 0) {
		packet.err = -1;
		return packet;
	}

	set_recved_info(packet);

	return packet;
}

/*
discription: recv data
main_chain:
<0: recv error or remote close
=0: nothing recved
>0: received
*/
int AgentReceiver::real_recv()
{
	int rv = 0;

	if (is_need_enlarge_buffer()) {
		if (enlarge_buffer() < 0) {
			log4cplus_error("no mme enlarge recv buffer error");
			return -ENOMEM;
		}
	}

	if ((rv = recv_once()) < 0)
		return -1;

	return rv;
}

/*
main_chain:
=0: no need enlarge recv buffer or enlarge ok but recv nothing
<0: error
>0: recved
*/
int AgentReceiver::recv_again()
{
	int rv = 0;

	if (!is_need_enlarge_buffer())
		return 0;

	if (enlarge_buffer() < 0) {
		log4cplus_error("no mme enlarge recv buffer error");
		return -ENOMEM;
	}

	if ((rv = recv_once()) < 0)
		return -1;

	return rv;
}

int AgentReceiver::decode_header_v1(DTC_HEADER_V1 *header)
{
	if (header->version != 1) { // version not supported
		log4cplus_error("version incorrect: %d", header->version);
		return -1;
	}

	if (header->scts != DRequest::Section::Total) { // tags# mismatch
		log4cplus_error("session count incorrect: %d", header->scts);
		return -1;
	}

	int pktbodylen = 0;
	for (int i = 0; i < DRequest::Section::Total; i++) {
#if __BYTE_ORDER == __BIG_ENDIAN
		const unsigned int v = bswap_32(header->len[i]);
#else
		const unsigned int v = header->len[i];
#endif

		if (v > MAXPACKETSIZE) {
			log4cplus_error("section [%d] len > MAXPACKETSIZE", i);
			return -1;
		}

		pktbodylen += v;
	}

	if (pktbodylen > MAXPACKETSIZE) {
		log4cplus_error("packet len > MAXPACKETSIZE 20M");
		return -1;
	}

	return pktbodylen;
}

int AgentReceiver::count_packet_v2()
{
	char *pos = buffer;
	int leftlen = offset;

	pktCnt = 0;

	if (pos == NULL || leftlen == 0)
		return 0;

	DTC_HEADER_V2 *header = NULL;

	while (1) {
		DTC_HEADER_V2 *header = NULL;

		if (leftlen < (int)sizeof(DTC_HEADER_V2))
			break;

		pos += leftlen;
		leftlen = 0;
		pktCnt = 1;
	}

	pktTail = pos - buffer;

	return 0;
}

int AgentReceiver::count_packet_v1()
{
	char *pos = buffer;
	int leftlen = offset;

	pktCnt = 0;

	if (pos == NULL || leftlen == 0)
		return 0;

	while (1) {
		int pktbodylen = 0;
		DTC_HEADER_V1 *header = NULL;

		if (leftlen < (int)sizeof(DTC_HEADER_V1))
			break;

		header = (DTC_HEADER_V1 *)pos;
		pktbodylen = decode_header_v1(header);
		if (pktbodylen < 0)
			return -1;

		if (leftlen < (int)sizeof(DTC_HEADER_V1) + pktbodylen)
			break;

		pos += sizeof(DTC_HEADER_V1) + pktbodylen;
		leftlen -= sizeof(DTC_HEADER_V1) + pktbodylen;
		pktCnt++;
	}

	pktTail = pos - buffer;
	return 0;
}

/*
return value:
<0: error
=0: contain no packet
>0: contain packet
*/
void AgentReceiver::set_recved_info(RecvedPacket &packet)
{
	uint8_t ver = (uint8_t)(buffer[0]);

	if (ver == 1) {
		if (count_packet_v1() < 0) {
			packet.err = -1;
			return;
		}
	} else if (ver == 2) {
		if (count_packet_v2() < 0) {
			packet.err = -1;
			return;
		}
	} else {
		log4cplus_error("parse dtc protocol version error:%d", ver);
		return;
	}

	/* not even ont packet recved, do nothing */
	if (pktCnt == 0)
		return;

	char *tmpbuff;
	if (NULL == (tmpbuff = (char *)malloc(buffSize))) {
		/* recved buffer willbe free by outBuff */
		log4cplus_error("no mem malloc new recv buffer");
		packet.err = -1;
		return;
	}

	if (pktTail < offset)
		memcpy(tmpbuff, buffer + pktTail, offset - pktTail);

	packet.buff = buffer;
	packet.len = pktTail;
	packet.pktCnt = pktCnt;
	packet.version = ver;

	buffer = tmpbuff;
	offset -= pktTail;

	return;
}

/*
main_chain:
=0: enlarge ok
<0: enlarge error
*/
int AgentReceiver::enlarge_buffer()
{
	buffer = (char *)REALLOC(buffer, buffSize * 2);
	if (buffer == NULL)
		return -1;
	buffSize *= 2;
	return 0;
}

bool AgentReceiver::is_need_enlarge_buffer()
{
	return offset == buffSize;
}
