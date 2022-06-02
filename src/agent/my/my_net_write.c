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
#include "my_comm.h"

int net_write(struct msg *dmsg, uint8_t* buf, size_t len, uint8_t pkt_nr) 
{
	struct mbuf *start_buf, *end_buf;
	uint8_t header[MYSQL_HEADER_SIZE] = {0};

	ASSERT(len < MAX_PACKET_LENGTH);

	int_conv_3(header, (uint)len);
	header[3] = pkt_nr;
	log_debug("net_write header:0x%x 0x%x 0x%x 0x%x", header[0], header[1], header[2], header[3]);

	start_buf = mbuf_get();
	if(start_buf == NULL)
	{
		log_error("get new mbuf error");
		return -1;
	}
	mbuf_copy(start_buf, header, MYSQL_HEADER_SIZE);
	mbuf_copy(start_buf, buf, len);
	mbuf_insert(&dmsg->buf_q, start_buf);
	return 0;
}