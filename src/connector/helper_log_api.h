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
* 
*/
#ifndef __HELPER_LOG_API_H__
#define __HELPER_LOG_API_H__

#if HAS_LOGAPI

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "msglogapi.h"
#include "logfmt.h"
struct HelperLogApi {
	HelperLogApi();
	~HelperLogApi();

	TMsgLog log;
	unsigned int msec;
	unsigned int iplocal, iptarget;
	int id[4];

	void do_init(int id0, int id1, int id2, int id3);
	void init_target(const char *host);
	void start(void);
	void done(const char *fn, int ln, const char *op, int ret, int err);
};

inline HelperLogApi::CHelperLogApi(void) : log(6, "dtc")
{
}

inline HelperLogApi::~CHelperLogApi(void)
{
}

extern "C" {
extern unsigned int get_local_ip();
}
inline void HelperLogApi::do_init(int id0, int id1, int id2, int id3)
{
	log4cplus_debug("LogApi: id %d caller %d target %d interface %d", id0,
			id1, id2, id3);
	id[0] = id0;
	id[1] = id1;
	id[2] = id2;
	id[3] = id3;
	iplocal = get_local_ip();
	log4cplus_debug("local ip is %x\n", iplocal);
}

inline void HelperLogApi::init_target(const char *host)
{
	if (host == NULL || host[0] == '\0' || host[0] == '/' ||
	    !strcasecmp(host, "localhost"))
		host = "127.0.0.1";

	struct in_addr a;
	a.s_addr = inet_addr(host);
	iptarget = *(unsigned int *)&a;

	log4cplus_debug("remote ip is %x\n", iptarget);
}

inline void HelperLogApi::start(void)
{
	INIT_MSEC(msec);
}

inline void HelperLogApi::done(const char *fn, int ln, const char *op, int ret,
			       int err)
{
	CALC_MSEC(msec);
	if (id[0] == 0)
		return;
	log.msgprintf((unsigned int)6, (unsigned long)id[0],
		      MAINTENANCE_MONITOR_MODULE_INTERFACE_LOG_FORMAT, id[1],
		      id[2], id[3], // 0: ID
		      iplocal, iptarget, // 3: IP
		      0, 0, // 5: PORT
		      fn, ln, // 7: source position
		      "", // 9: file modification time
		      op, // 10: operation
		      err, // 11: MySQL ErrNo
		      ret ? 1 : 0, // 12: status
		      msec, -1, // 13: timing
		      0, 0, 0, 0, "", "", "", "", "");
}

#else

struct HelperLogApi {
	void do_init(int id0, int id1, int id2, int id3)
	{
	}
	void init_target(const char *host)
	{
	}
	void start(void)
	{
	}
	void done(const char *fn, int ln, const char *op, int ret, int err)
	{
	}
};

#endif

#endif
