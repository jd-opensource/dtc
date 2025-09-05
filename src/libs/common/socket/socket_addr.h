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
#ifndef __CH_NETADDR_________
#define __CH_NETADDR_________

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <stdio.h>

class SocketAddress {
    public: // members
	char sockname[128];
	int socktype;
	socklen_t alen;
	union {
		struct sockaddr_in in4[1];
		struct sockaddr_in6 in6[1];
		struct sockaddr_un un[1];
		struct sockaddr addr[1];
	};

    public: // methods
	SocketAddress(void) : socktype(0), alen(0)
	{
		this->addr->sa_family = 0;
	}
	~SocketAddress(void)
	{
	}

	// initialize to name
	// success: return NULL
	// failure: return error message
	const char *set_address(const char *name, int port = 0,
				int type = SOCK_STREAM);
	const char *set_address(const char *name, const char *portstr);
	const char *set_address(const struct sockaddr *, int socklen, int type);
	const char *set_address(const struct sockaddr_un *, int socklen,
				int type);
	const char *set_address(const struct sockaddr_in *, int socklen,
				int type);
	const char *set_address(const struct sockaddr_in6 *, int socklen,
				int type);
	int socket_family(void) const
	{
		return this->addr->sa_family;
	}
	int socket_type(void) const
	{
		return this->socktype;
	}
	const char *socket_type_name(void) const
	{
		return this->socktype == SOCK_DGRAM ?
			       "DGRAM" :
			       this->socktype == SOCK_STREAM ?
			       "STREAM" :
			       this->socktype == SOCK_RAW ?
			       "RAW" :
			       this->socktype == SOCK_SEQPACKET ? "SEQPACKET" :
								  "<BAD>";
	}
	int create_socket(void) const
	{
		return socket(this->addr->sa_family, this->socktype, 0);
	}
	int connect_socket(int fd) const
	{
		return connect(fd, this->addr, this->alen);
	}
	int bind_socket(int fd) const
	{
		return bind(fd, this->addr, this->alen);
	}
	int send_to(int fd, const char *buf, int len, int flags = 0) const
	{
		return sendto(fd, buf, len, flags, this->addr, this->alen);
	}
	int recv_from(int fd, char *buf, int len, int flags = 0)
	{
		this->alen = sizeof(struct sockaddr);
		int ret =
			recvfrom(fd, buf, len, flags, this->addr, &this->alen);
		build_name();
		return ret;
	}
	int connect_socket(void) const
	{
		int fd = create_socket();
		if (fd >= 0) {
			if (connect_socket(fd) < 0)
				close(fd);
			fd = -1;
		}
		return fd;
	}
	int bind_socket(void) const
	{
		int fd = create_socket();
		if (fd >= 0) {
			if (bind_socket(fd) < 0)
				close(fd);
			fd = -1;
		}
		return fd;
	}

	int Equal(const SocketAddress *) const;
	int Equal(const char *) const;
	int Equal(const char *, int, int = SOCK_STREAM) const;
	int Equal(const char *, const char *) const;
	int Equal(const struct sockaddr *, int, int = SOCK_STREAM) const;
	int Equal(const struct sockaddr_un *, int, int = SOCK_STREAM) const;
	int Equal(const struct sockaddr_in *, int, int = SOCK_STREAM) const;
	int Equal(const struct sockaddr_in6 *, int, int = SOCK_STREAM) const;
	int Match(const SocketAddress *) const;
	int Match(const char *) const;
	int Match(const char *, int, int = SOCK_STREAM) const;
	int Match(const char *, const char *) const;
	int Match(const struct sockaddr *, int, int = SOCK_STREAM) const;
	int Match(const struct sockaddr_un *, int, int = SOCK_STREAM) const;
	int Match(const struct sockaddr_in *, int, int = SOCK_STREAM) const;
	int Match(const struct sockaddr_in6 *, int, int = SOCK_STREAM) const;
	const char *Name(void) const
	{
		return this->addr->sa_family == 0 ? NULL : sockname;
	}

    private:
	const char *set_unix_address(const char *name, int type = SOCK_STREAM);
	const char *set_ipv4_address(const char *name, int port = 0,
				     int type = SOCK_STREAM);
	const char *set_ipv6_address(const char *name, int port = 0,
				     int type = SOCK_STREAM);
	const char *set_host_address(const char *name, int port = 0,
				     int type = SOCK_STREAM);
	void build_name(void);
	void build_name_bad(void);
	void build_name_none(void);
	void build_name_ipv4(void);
	void build_name_ipv6(void);
	void build_name_unix(void);

    public: // static methods
	static inline int is_unix_socket_path(const char *path)
	{
		return path[0] == '/' || path[0] == '@';
	}

	static inline int init_unix_socket_address(struct sockaddr_un *addr,
						   const char *path)
	{
		bzero(addr, sizeof(struct sockaddr_un));
		addr->sun_family = AF_LOCAL;
		strncpy(addr->sun_path, path, sizeof(addr->sun_path) - 1);
		socklen_t addrlen = SUN_LEN(addr);
		if (path[0] == '@')
			addr->sun_path[0] = '\0';
		return addrlen;
	}
	static inline void set_socket_buffer(int netfd, int rbufsz, int wbufsz)
	{
		if (rbufsz)
			setsockopt(netfd, SOL_SOCKET, SO_RCVBUF, &rbufsz,
				   sizeof(rbufsz));
		if (wbufsz)
			setsockopt(netfd, SOL_SOCKET, SO_SNDBUF, &wbufsz,
				   sizeof(wbufsz));
	}
};

extern int socket_bind(const SocketAddress *addr, int backlog = 0,
		       int rbufsz = 0, int wbufsz = 0, int reuse = 0,
		       int nodelay = 0, int defer = 0);
#endif
