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
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/un.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>

#include "socket/unix_socket.h"
#include "listener/listener.h"
#include "poll/poller_base.h"
#include "decode/decoder_base.h"
#include "log/log.h"
#include <stat_dtc.h>

extern int gMaxConnCnt;

DTCListener::DTCListener(const SocketAddress *s)
{
	sockaddr = s;
	bind = 0;
	window = 0;
	outPeer = NULL;
}

DTCListener::~DTCListener()
{
	if (sockaddr && sockaddr->socket_family() == AF_UNIX && netfd >= 0) {
		if (sockaddr->Name()[0] == '/') {
			unlink(sockaddr->Name());
		}
	}
}

int DTCListener::do_bind(int blog, int rbufsz, int wbufsz)
{
	if (bind)
		return 0;

	if ((netfd = socket_bind(sockaddr, blog, rbufsz, wbufsz, 1 /*reuse*/,
				 1 /*nodelay*/, 0 /*defer_accept*/)) == -1)
		return -1;

	bind = 1;

	return 0;
}

int DTCListener::do_attach(DecoderBase *unit, int blog, int rbufsz, int wbufsz)
{
	if (do_bind(blog, rbufsz, wbufsz) != 0)
		return -1;

	outPeer = unit;
	if (sockaddr->socket_type() == SOCK_DGRAM) {
		if (unit->process_dgram(netfd) < 0)
			return -1;
		else
			netfd = dup(netfd);
		return 0;
	}
	enable_input();
	return attach_poller(unit->owner_thread());
}

void DTCListener::input_notify(void)
{
	log4cplus_debug("enter input_notify.");
	int newfd = -1;
	socklen_t peerSize;
	struct sockaddr peer;

	while (true) {
		peerSize = sizeof(peer);
		newfd = accept(netfd, &peer, &peerSize);

		if (newfd == -1) {
			if (errno != EINTR && errno != EAGAIN)
				log4cplus_info("[%s]accept failed, fd=%d, %m",
					       sockaddr->Name(), netfd);

			break;
		}

		if (outPeer->process_stream(newfd, window, &peer, peerSize) < 0)
			close(newfd);
	}
	log4cplus_debug("leave input_notify.");
}
