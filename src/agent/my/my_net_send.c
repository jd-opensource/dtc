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
#include <inttypes.h>
#include <math.h>
#include <inttypes.h>
#include "da_protocal.h"
#include "../da_msg.h"
#include "../da_conn.h"
#include "../da_request.h"
#include "../da_buf.h"
#include "../da_util.h"
#include "../da_errno.h"
#include "../da_time.h"
#include "../da_core.h"

int net_send_server_greeting(struct msg *smsg, struct msg *dmsg) {
	uint8_t buf[MYSQL_ERRMSG_SIZE+10] = {0x0a, 0x38, 0x2e, 0x30, 0x2e, 0x32, 0x36, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x6f, 0x3c, 0x36, 0x36, 0x03, 0x68, 0x38, 0x46, 0x00, 0xff, 0xf7, 0xff, 0x02, 0x00, 0xff, 0x8f, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x2a, 0x0a, 0x60, 0x5c, 0x68, 0x50, 0x34, 0x6b, 0x0e, 0x27, 0x73, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00};
	uint8_t *pos, *start;
	
	start = buf;
	pos = buf;
	pos += 74;
	
	net_write(dmsg, buf, (size_t)(pos - start), 0);
	dmsg->mlen = (size_t)(pos - start);

	log_debug("dmsg len:%d", dmsg->mlen);
	return 0;
}

int net_send_ok(struct msg *smsg, struct conn *c_conn) {
	uint8_t buf[MYSQL_ERRMSG_SIZE+10] = {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00};
	uint8_t *pos, *start;
	struct msg* dmsg = NULL;
	uint8_t pkt_nr = smsg->pkt_nr;
	dmsg = msg_get(c_conn, false);
	if(dmsg == NULL)
	{
		log_error("get new msg error.");
		c_conn->error = 1;
		c_conn->err = CONN_MSG_GET_ERR;
		return -1;
	}

	start = buf;
	pos = buf;

	pos += 7;
	
	net_write(dmsg, buf, (size_t)(pos - start), ++pkt_nr);

	dmsg->pkt_nr = pkt_nr;
	dmsg->mlen = (size_t)(pos - start);
	log_debug("dmsg len:%d", dmsg->mlen);

	smsg->peer = dmsg;

	return 0;
}
