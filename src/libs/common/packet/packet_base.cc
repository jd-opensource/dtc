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
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <new>

#include "packet.h"
#include "protocol.h"

#include "../log/log.h"
int Packet::Send(int fd)
{
	if (nv <= 0)
		return SendResultDone;

	struct msghdr msgh;
	msgh.msg_name = NULL;
	msgh.msg_namelen = 0;
	msgh.msg_iov = v;
	msgh.msg_iovlen = nv;
	msgh.msg_control = NULL;
	msgh.msg_controllen = 0;
	msgh.msg_flags = 0;

	int rv = sendmsg(fd, &msgh, MSG_DONTWAIT | MSG_NOSIGNAL);

	if (rv < 0) {
		if (errno == EINTR || errno == EAGAIN || errno == EINPROGRESS)
			return SendResultMoreData;
		return SendResultError;
	}
	if (rv == 0)
		return SendResultMoreData;
	bytes -= rv;
	if (bytes == 0) {
		nv = 0;
		return SendResultDone;
	}
	while (nv > 0 && rv >= (int64_t)v->iov_len) {
		rv -= v->iov_len;
		v++;
		nv--;
	}
	if (rv > 0) {
		v->iov_base = (char *)v->iov_base + rv;
		v->iov_len -= rv;
	}
	return nv == 0 ? SendResultDone : SendResultMoreData;
}

int Packet::send_to(int fd, void *addr, int len)
{
	if (nv <= 0)
		return SendResultDone;
	struct msghdr msgh;
	msgh.msg_name = addr;
	msgh.msg_namelen = len;
	msgh.msg_iov = v;
	msgh.msg_iovlen = nv;
	msgh.msg_control = NULL;
	msgh.msg_controllen = 0;
	msgh.msg_flags = 0;

	int rv = sendmsg(fd, &msgh, MSG_DONTWAIT | MSG_NOSIGNAL);

	if (rv < 0) {
		return SendResultError;
	}
	if (rv == 0)
		return SendResultError;

	bytes -= rv;
	if (bytes == 0) {
		nv = 0;
		return SendResultDone;
	}
	return SendResultError;
}

int Packet::encode_header_v1(DTC_HEADER_V1 &header)
{
	int len = sizeof(header);
	for (int i = 0; i < DRequest::Section::Total; i++) {
		len += header.len[i];
#if __BYTE_ORDER == __BIG_ENDIAN
		header.len[i] = bswap_32(header.len[i]);
#endif
	}
	return len;
}

#if __BYTE_ORDER == __LITTLE_ENDIAN
int Packet::encode_header_v1(const DTC_HEADER_V1 &header)
{
	int len = sizeof(header);
	for (int i = 0; i < DRequest::Section::Total; i++) {
		len += header.len[i];
	}
	return len;
}
#endif
