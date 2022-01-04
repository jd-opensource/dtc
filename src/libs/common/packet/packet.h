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
#ifndef __CH_PACKET_H__
#define __CH_PACKET_H__

#include <sys/uio.h>

#include "protocol.h"
#include "section.h"
#include "socket/socket_addr.h"
#include "log/log.h"
#include "result.h"

class NCRequest;
union DTCValue;
class DtcJob;
class DTCJobOperation;
class DTCTableDefinition;

enum DTCSendResult { SendResultError, SendResultMoreData, SendResultDone };

/* just one buff malloced */
class Packet {
    private:
	struct iovec *v;
	int nv;
	int bytes;
	BufferChain *buf;
	int sendedVecCount;

	Packet(const Packet &);

    public:
	Packet() : v(NULL), nv(0), bytes(0), buf(NULL), sendedVecCount(0){};
	~Packet()
	{
		/* free buffer chain buffer in several place, not freed all here */
		FREE_IF(buf);
	}

	inline void Clean()
	{
		v = NULL;
		nv = 0;
		bytes = 0;
		if (buf)
			buf->Clean();
	}
	int Send(int fd);
	int send_to(int fd, void *name, int namelen);
	int send_to(int fd, SocketAddress *addr)
	{
		return addr == NULL ?
			       Send(fd) :
			       send_to(fd, (void *)addr->addr, (int)addr->alen);
	}

	/* for agent_sender */
	int Bytes(void);
	void free_result_buff();
	const struct iovec *IOVec()
	{
		return v;
	}
	int vec_count()
	{
		return nv;
	}
	void send_done_one_vec()
	{
		sendedVecCount++;
	}
	bool is_send_done()
	{
		return sendedVecCount == nv;
	}

	static int encode_header_v1(DTC_HEADER_V1 &header);
#if __BYTE_ORDER == __LITTLE_ENDIAN
	static int encode_header_v1(const DTC_HEADER_V1 &header);
#endif
	int encode_forward_request(DTCJobOperation &);
	int encode_pass_thru(DtcJob &);
	int encode_fetch_data(DTCJobOperation &);

	// encode result, for helper/server reply
	// side effect:
	// 	if error code set(result_code()<0), ignore DTCResultSet
	// 	if no result/error code set, no result_code() to zero
	// 	if no result key set, set result key to request key
	int encode_result(DtcJob &, int mtu = 0, uint32_t ts = 0);
	int encode_result_v2(DtcJob &, int mtu = 0, uint32_t ts = 0);
	int desc_tables_result(DtcJob *job);
	int encode_result(DTCJobOperation &, int mtu = 0);
	int encode_detect(const DTCTableDefinition *tdef, int sn = 1);
	int encode_request(NCRequest &r, const DTCValue *k = NULL);
	static int encode_simple_request(NCRequest &rq, const DTCValue *kptr,
					 char *&ptr, int &len);
	char *allocate_simple(int size);

	int encode_result(DtcJob *job, int mtu = 0, uint32_t ts = 0)
	{
		return encode_result(*job, mtu, ts);
	}
	int encode_result(DTCJobOperation *job, int mtu = 0)
	{
		return encode_result(*job, mtu);
	}
	int encode_forward_request(DTCJobOperation *job)
	{
		return encode_forward_request(*job);
	}

	/* for agent */
	int encode_agent_request(NCRequest &rq, const DTCValue *kptr);

	int encode_reload_config(const DTCTableDefinition *tdef, int sn = 1);

	int encode_mysql_protocol(ResultSet *rp, char *my_result, int *my_len,
				  DtcJob &job);
};

#endif
