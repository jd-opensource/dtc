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
#include <netinet/tcp.h>
#include "socket/socket_addr.h"
#include "../log/log.h"

int socket_bind(const SocketAddress *addr, int backlog, int rbufsz, int wbufsz,
		int reuse, int nodelay, int defer_accept)
{
	int netfd;

	if ((netfd = addr->create_socket()) == -1) {
		log4cplus_error("%s: make socket error, %m", addr->Name());
		return -1;
	}

	int optval = 1;
	if (reuse)
		setsockopt(netfd, SOL_SOCKET, SO_REUSEADDR, &optval,
			   sizeof(optval));
	if (nodelay)
		setsockopt(netfd, SOL_TCP, TCP_NODELAY, &optval,
			   sizeof(optval));

	/* Avoid empty connections without requests waking up epoll and wasting CPU resources */
	if (defer_accept) {
		optval = 60;
		setsockopt(netfd, SOL_TCP, TCP_DEFER_ACCEPT, &optval,
			   sizeof(optval));
	}

	if (addr->bind_socket(netfd) == -1) {
		log4cplus_error("%s: bind failed, %m", addr->Name());
		close(netfd);
		return -1;
	}

	if (addr->socket_type() == SOCK_STREAM &&
	    listen(netfd, backlog) == -1) {
		log4cplus_error("%s: listen failed, %m", addr->Name());
		close(netfd);
		return -1;
	}

	if (rbufsz)
		setsockopt(netfd, SOL_SOCKET, SO_RCVBUF, &rbufsz,
			   sizeof(rbufsz));
	if (wbufsz)
		setsockopt(netfd, SOL_SOCKET, SO_SNDBUF, &wbufsz,
			   sizeof(wbufsz));

	log4cplus_info("%s on %s",
		       addr->socket_type() == SOCK_STREAM ? "listen" : "bind",
		       addr->Name());
	return netfd;
}
