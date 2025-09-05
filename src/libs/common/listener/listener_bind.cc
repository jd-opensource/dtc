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
#include <netinet/tcp.h>

#include "socket/unix_socket.h"
#include "log/log.h"

int socket_bind(const char *addr, uint16_t port, int backlog)
{
	struct sockaddr_in inaddr;
	int reuse_addr = 1;
	int netfd;

	bzero(&inaddr, sizeof(struct sockaddr_in));
	inaddr.sin_family = AF_INET;
	inaddr.sin_port = htons(port);

	const char *end = strchr(addr, ':');
	if (end) {
		char *p = (char *)alloca(end - addr + 1);
		memcpy(p, addr, end - addr);
		p[end - addr] = '\0';
		addr = p;
	}
	if (strcmp(addr, "*") != 0 &&
	    inet_pton(AF_INET, addr, &inaddr.sin_addr) <= 0) {
		log4cplus_error("invalid address %s:%d", addr, port);
		return -1;
	}

	if ((netfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		log4cplus_error("make tcp socket error, %m");
		return -1;
	}

	setsockopt(netfd, SOL_SOCKET, SO_REUSEADDR, &reuse_addr,
		   sizeof(reuse_addr));
	setsockopt(netfd, SOL_TCP, TCP_NODELAY, &reuse_addr,
		   sizeof(reuse_addr));
	reuse_addr = 60;
	/* Avoid empty connections without requests waking up epoll and wasting CPU resources */
	setsockopt(netfd, SOL_TCP, TCP_DEFER_ACCEPT, &reuse_addr,
		   sizeof(reuse_addr));

	if (bind(netfd, (struct sockaddr *)&inaddr, sizeof(struct sockaddr)) ==
	    -1) {
		log4cplus_error("bind tcp %s:%u failed, %m", addr, port);
		close(netfd);
		return -1;
	}

	if (listen(netfd, backlog) == -1) {
		log4cplus_error("listen tcp %s:%u failed, %m", addr, port);
		close(netfd);
		return -1;
	}

	log4cplus_info("listen on tcp %s:%u", addr, port);
	return netfd;
}

int udp_sock_bind(const char *addr, uint16_t port, int rbufsz, int wbufsz)
{
	struct sockaddr_in inaddr;
	int netfd;

	bzero(&inaddr, sizeof(struct sockaddr_in));
	inaddr.sin_family = AF_INET;
	inaddr.sin_port = htons(port);

	const char *end = strchr(addr, ':');
	if (end) {
		char *p = (char *)alloca(end - addr + 1);
		memcpy(p, addr, end - addr);
		p[end - addr] = '\0';
		addr = p;
	}
	if (strcmp(addr, "*") != 0 &&
	    inet_pton(AF_INET, addr, &inaddr.sin_addr) <= 0) {
		log4cplus_error("invalid address %s:%d", addr, port);
		return -1;
	}

	if ((netfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
		log4cplus_error("make udp socket error, %m");
		return -1;
	}

	if (bind(netfd, (struct sockaddr *)&inaddr, sizeof(struct sockaddr)) ==
	    -1) {
		log4cplus_error("bind udp %s:%u failed, %m", addr, port);
		close(netfd);
		return -1;
	}

	if (rbufsz)
		setsockopt(netfd, SOL_SOCKET, SO_RCVBUF, &rbufsz,
			   sizeof(rbufsz));
	if (wbufsz)
		setsockopt(netfd, SOL_SOCKET, SO_SNDBUF, &wbufsz,
			   sizeof(wbufsz));

	log4cplus_info("listen on udp %s:%u", addr, port);
	return netfd;
}

int unix_sock_bind(const char *path, int backlog)
{
	struct sockaddr_un unaddr;
	int netfd;

	socklen_t addrlen = init_unix_socket_address(&unaddr, path);
	if ((netfd = socket(PF_UNIX, SOCK_STREAM, 0)) == -1) {
		log4cplus_error("%s", "make unix socket error, %m");
		return -1;
	}

	if (bind(netfd, (struct sockaddr *)&unaddr, addrlen) == -1) {
		log4cplus_error("bind unix %s failed, %m", path);
		close(netfd);
		return -1;
	}

	if (listen(netfd, backlog) == -1) {
		log4cplus_error("listen unix %s failed, %m", path);
		close(netfd);
		return -1;
	}

	log4cplus_info("listen on unix %s, fd=%d", path, netfd);
	return netfd;
}
