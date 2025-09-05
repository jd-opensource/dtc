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

#include "da_util.h"
#include "da_string.h"
#include "da_log.h"

static void log_and_exit(const char *cond, const char *file, int line,
		int panic) {
	log_error("assert '%s' failed @ (%s, %d)", cond, file, line);
	if (panic) {
		abort();
	}
}

void da_assert(const char * cond, const char *file, int line, int panic) {
	log_and_exit(cond, file, line, panic);
}

/*
 * copies at most <size-1> chars from <src> to <dst>. Last char is always
 * set to 0, unless <size> is 0. The number of chars copied is returned
 * (excluding the terminating zero).
 */
int da_strlcpy(char *dst, const char *src, int size) {
	char *orig = dst;
	if (size) {
		while (--size && (*dst = *src)) {
			src++;
			dst++;
		}
		*dst = 0;
	}
	return dst - orig;
}

/*
 * net work utils
 */
int set_reuseaddr(int fd) {
	int reuse;
	socklen_t len;

	reuse = 1;
	len = sizeof(reuse);

	return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, len);
}

int set_nonblocking(int fd) {
	int flags;

	flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0) {
		return flags;
	}

	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int get_soerror(int sd) {
	int status, err;
	socklen_t len;

	err = 0;
	len = sizeof(err);

	status = getsockopt(sd, SOL_SOCKET, SO_ERROR, &err, &len);
	if (status == 0) {
		errno = err;
	}

	return status;
}

bool nc_valid_port(int n) {
	if (n < 1 || n > UINT16_MAX) {
		return false;
	}

	return true;
}

/*
 * Disable Nagle algorithm on TCP socket.
 *
 * This option helps to minimize transmit latency by disabling coalescing
 * of data to fill up a TCP segment inside the kernel. Sockets with this
 * option must use readv() or writev() to do data transfer in bulk and
 * hence avoid the overhead of small packets.
 */
int set_tcpnodelay(int fd) {
	int nodelay;
	socklen_t len;

	nodelay = 1;
	len = sizeof(nodelay);

	return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, len);
}

int set_tcpquickack(int fd)
{
	int quickack;
	socklen_t len;

	quickack = 1;
	len = sizeof(quickack);

	return setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, len);
}

ssize_t _da_sendn(int sd, const void *vptr, size_t n) {
	size_t nleft;
	ssize_t nsend;
	const char *ptr;

	ptr = vptr;
	nleft = n;
	while (nleft > 0) {
		nsend = send(sd, ptr, nleft, 0);
		if (nsend < 0) {
			if (errno == EINTR) {
				continue;
			}
			return nsend;
		}
		if (nsend == 0) {
			return -1;
		}

		nleft -= (size_t) nsend;
		ptr += nsend;
	}

	return (ssize_t) n;
}

ssize_t _da_recvn(int sd, void *vptr, size_t n) {
	size_t nleft;
	ssize_t nrecv;
	char *ptr;

	ptr = vptr;
	nleft = n;
	while (nleft > 0) {
		nrecv = recv(sd, ptr, nleft, 0);
		if (nrecv < 0) {
			if (errno == EINTR) {
				continue;
			}
			return nrecv;
		}
		if (nrecv == 0) {
			break;
		}

		nleft -= (size_t) nrecv;
		ptr += nrecv;
	}

	return (ssize_t) (n - nleft);
}

char upper(char c)
{
	if (c >='a' && c <='z')
       c &= 0xDF;
	return c;
}

char lower(char c)
{
	if(c >= 'A' && c <= 'Z')
		c |=0x20;
	return c;
}

int _vscnprintf(char *buf, size_t size, const char *fmt, va_list args) {
	int n;

	n = vsnprintf(buf, size, fmt, args);

	/*
	 * The return value is the number of characters which would be written
	 * into buf not including the trailing '\0'. If size is == 0 the
	 * function returns 0.
	 *
	 * On error, the function also returns 0. This is to allow idiom such
	 * as len += _vscnprintf(...)
	 *
	 * See: http://lwn.net/Articles/69419/
	 */
	if (n <= 0) {
		return 0;
	}

	if (n < (int) size) {
		return n;
	}

	return (int) (size - 1);
}

int _scnprintf(char *buf, size_t size, const char *fmt, ...) {
	va_list args;
	int n;

	va_start(args, fmt);
	n = _vscnprintf(buf, size, fmt, args);
	va_end(args);

	return n;
}

int _da_atoi(uint8_t *line, size_t n) {
	int value;

	if (n == 0) {
		return -1;
	}

	for (value = 0; n--; line++) {
		if (*line < '0' || *line > '9') {
			return -1;
		}

		value = value * 10 + (*line - '0');
	}

	if (value < 0) {
		return -1;
	}

	return value;
}

/*
 * Unresolve the socket descriptor peer address by translating it to a
 * character string describing the host and service
 *
 * This routine is not reentrant
 */
char *
da_unresolve_peer_desc(int sd) {
	static struct sockinfo si;
	struct sockaddr *addr;
	socklen_t addrlen;
	int status;

	memset(&si, 0, sizeof(si));
	addr = (struct sockaddr *) &si.addr;
	addrlen = sizeof(si.addr);

	status = getpeername(sd, addr, &addrlen);
	if (status < 0) {
		return "unknown";
	}

	return da_unresolve_addr(addr, addrlen);
}

static int da_resolve_unix(struct string *name, struct sockinfo *si) {
	struct sockaddr_un *un;

	if (name->len >= DA_UNIX_ADDRSTRLEN) {
		return -1;
	}

	un = &si->addr.un;

	un->sun_family = AF_UNIX;
	da_memcpy(un->sun_path, name->data, name->len);
	un->sun_path[name->len] = '\0';

	si->family = AF_UNIX;
	si->addrlen = sizeof(*un);
	/* si->addr is an alias of un */

	return 0;
}

static int da_resolve_inet(struct string *name, int port, struct sockinfo *si) {
	int status;
	struct addrinfo *ai, *cai; /* head and current addrinfo */
	struct addrinfo hints;
	char *node, service[DA_UINTMAX_MAXLEN];
	bool found;

	ASSERT(da_valid_port(port));

	memset(&hints, 0, sizeof(hints));
	hints.ai_flags = AI_NUMERICSERV;
	hints.ai_family = AF_UNSPEC; /* AF_INET or AF_INET6 */
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = 0;
	hints.ai_addrlen = 0;
	hints.ai_addr = NULL;
	hints.ai_canonname = NULL;

	if (name != NULL) {
		node = (char *) name->data;
	} else {
		/*
		 * If AI_PASSIVE flag is specified in hints.ai_flags, and node is
		 * NULL, then the returned socket addresses will be suitable for
		 * bind(2)ing a socket that will accept(2) connections. The returned
		 * socket address will contain the wildcard IP address.
		 */
		node = NULL;
		hints.ai_flags |= AI_PASSIVE;
	}

	da_snprintf(service, DA_UINTMAX_MAXLEN, "%d", port);

	status = getaddrinfo(node, service, &hints, &ai);
	if (status < 0) {
		log_error("address resolution of node '%s' service '%s' failed: %s",
				node, service, gai_strerror(status));
		return -1;
	}

	/*
	 * getaddrinfo() can return a linked list of more than one addrinfo,
	 * since we requested for both AF_INET and AF_INET6 addresses and the
	 * host itself can be multi-homed. Since we don't care whether we are
	 * using ipv4 or ipv6, we just use the first address from this collection
	 * in the order in which it was returned.
	 *
	 * The sorting function used within getaddrinfo() is defined in RFC 3484;
	 * the order can be tweaked for a particular system by editing
	 * /etc/gai.conf
	 */
	for (cai = ai, found = false; cai != NULL; cai = cai->ai_next) {
		si->family = cai->ai_family;
		si->addrlen = cai->ai_addrlen;
		da_memcpy(&si->addr, cai->ai_addr, si->addrlen);
		found = true;
		break;
	}

	freeaddrinfo(ai);

	return !found ? -1 : 0;
}

/*
 * Resolve a hostname and service by translating it to socket address and
 * return it in si
 *
 * This routine is reentrant
 */
int da_resolve(struct string *name, int port, struct sockinfo *si) {
	if (name != NULL && name->data[0] == '/') {
		return da_resolve_unix(name, si);
	}

	return da_resolve_inet(name, port, si);
}

char *da_unresolve_addr(struct sockaddr *addr, socklen_t addrlen) {
	static char unresolve[NI_MAXHOST + NI_MAXSERV];
	static char host[NI_MAXHOST], service[NI_MAXSERV];
	int status;

	status = getnameinfo(addr, addrlen, host, sizeof(host), service,
			sizeof(service),NI_NUMERICHOST | NI_NUMERICSERV);
	if (status < 0) {
		return "unknown";
	}

	da_snprintf(unresolve, sizeof(unresolve), "%s:%s", host, service);

	return unresolve;
}

/*
 * Unresolve the socket descriptor address by translating it to a
 * character string describing the host and service
 *
 * This routine is not reentrant
 */
char *da_unresolve_desc(int sd)
{
    static struct sockinfo si;
    struct sockaddr *addr;
    socklen_t addrlen;
    int status;

    memset(&si, 0, sizeof(si));
    addr = (struct sockaddr *)&si.addr;
    addrlen = sizeof(si.addr);

    status = getsockname(sd, addr, &addrlen);
    if (status < 0) {
        return "unknown";
    }

    return da_unresolve_addr(addr, addrlen);
}

bool da_valid_port(int n)
{
    if (n < 1 || n > UINT16_MAX) {
        return false;
    }

    return true;
}

int64_t nc_usec_now(void)
{
    struct timeval now;
    int64_t usec;
    int status;

    status = gettimeofday(&now, NULL);
    if (status < 0) {
        log_error("gettimeofday failed: %s", strerror(errno));
        return -1;
    }

    usec = (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_usec;

    return usec;
}
