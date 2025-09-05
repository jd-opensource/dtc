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

#ifndef DA_UTIL_H_
#define DA_UTIL_H_

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>

struct string;

#define LF (uint8_t)10
#define CR (uint8_t)13
#define CRLF "\x0d\x0a"

#define NELEMS(a) ((sizeof(a)) / sizeof((a)[0]))

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define DA_MAXHOSTNAMELEN 256
#define DA_UNIX_ADDRSTRLEN                                                     \
  (sizeof(struct sockaddr_un) - offsetof(struct sockaddr_un, sun_path))

/*
 * Length of 1 byte, 2 bytes, 4 bytes, 8 bytes and largest integral
 * type (uintmax_t) in ascii, including the null terminator '\0'
 *
 * From stdint.h, we have:
 * # define UINT8_MAX	(255)
 * # define UINT16_MAX	(65535)
 * # define UINT32_MAX	(4294967295U)
 * # define UINT64_MAX	(__UINT64_C(18446744073709551615))
 */
#define DA_UINT8_MAXLEN (3 + 1)
#define DA_UINT16_MAXLEN (5 + 1)
#define DA_UINT32_MAXLEN (10 + 1)
#define DA_UINT64_MAXLEN (20 + 1)
#define DA_UINTMAX_MAXLEN DA_UINT64_MAXLEN

#define da_sendn(_s, _b, _n) _da_sendn(_s, _b, (size_t)(_n))
#define da_recvn(_s, _b, _n) _da_recvn(_s, _b, (size_t)(_n))

#define da_read(_d, _b, _n) read(_d, _b, (size_t)(_n))
#define da_readv(_d, _b, _n) readv(_d, _b, (int)(_n))
#define da_write(_d, _b, _n) write(_d, _b, (size_t)(_n))
#define da_writev(_d, _b, _n) writev(_d, _b, (int)(_n))

ssize_t _da_sendn(int sd, const void *vptr, size_t n);
ssize_t _da_recvn(int sd, void *vptr, size_t n);

#define da_gethostname(_name, _len) gethostname((char *)_name, (size_t)(_len))

#define da_atoi(_line, _n) _da_atoi((uint8_t *)_line, (size_t)(_n))

int _da_atoi(uint8_t *line, size_t n);

#ifdef DA_ASSERT_PANIC

#define ASSERT(_x)                                                             \
  do {                                                                         \
    if (!(_x)) {                                                               \
      da_assert(#_x, __FILE__, __LINE__, 1);                                   \
    }                                                                          \
  } while (0)

#elif DA_ASSERT_LOG

#define ASSERT(_x)                                                             \
  do {                                                                         \
    if (!(_x)) {                                                               \
      da_assert(#_x, __FILE__, __LINE__, 0);                                   \
    }                                                                          \
  } while (0)

#else
#define ASSERT(_x)
#endif

void da_assert(const char *cond, const char *file, int line, int panic);
void da_stacktrace(int skip_count);
void da_stacktrace_fd(int fd);

int da_strlcpy(char *dst, const char *src, int size);

int set_reuseaddr(int fd);
int set_nonblocking(int fd);
int set_tcpnodelay(int fd);
int set_tcpquickack(int fd);
int get_soerror(int sd);
bool nc_valid_port(int n);

char upper(char c);
char lower(char c);
int _scnprintf(char *buf, size_t size, const char *fmt, ...);
int _vscnprintf(char *buf, size_t size, const char *fmt, va_list args);

bool da_valid_port(int n);
/*
 * Address resolution for internet (ipv4 and ipv6) and unix domain
 * socket address.
 */

struct sockinfo {
  int family;        /* socket address family */
  socklen_t addrlen; /* socket address length */
  union {
    struct sockaddr_in in;   /* ipv4 socket address */
    struct sockaddr_in6 in6; /* ipv6 socket address */
    struct sockaddr_un un;   /* unix domain address */
  } addr;
};

int da_resolve(struct string *name, int port, struct sockinfo *si);
char *da_unresolve_addr(struct sockaddr *addr, socklen_t addrlen);
char *da_unresolve_peer_desc(int sd);
char *da_unresolve_desc(int sd);

int64_t nc_usec_now(void);

#endif /* DA_UTIL_H_ */
