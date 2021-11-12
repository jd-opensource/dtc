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
#include "my_command.h"

#define MY_HEADER_SIZE 4
#define MAXPACKETSIZE	(64<<20)
#define MultiKeyValue 32
#define __FLTFMT__	"%LA"
#define CONVERT_NULL_TO_EMPTY_STRING 1

enum fieldtype {
	None = 0,
	Signed = 1,
	Unsigned = 2,
	Float = 3,
	String = 4,
	Binary = 5,
	TotalType,
};

enum codefield {
	FD_HEADER = 0,
	FD_VERSION = 1,
	FD_TABLEDEFINE = 2,
	FD_REQUEST = 3,
	FD_RESULTINFO = 4,
	FD_UPDATEINFO = 5,
	FD_CONDITIONINFO = 6,
	FD_FIELDSET = 7,
	FD_RESULTSET = 8,
	FD_TOTALFIELD = 9,
};

enum codestate {
	ST_ID = 0, ST_LENTH = 1, ST_VALUE = 2,
};

/*
 * parse request msg
 */
void my_parse_req(struct msg *r) {
	struct mbuf *b;
	uint8_t *p;
	uint32_t input_packet_length = 0;
	enum enum_server_command command;
	int rc;

	log_debug("my_parse_req entry.");

	b = STAILQ_LAST(&r->buf_q, mbuf, next);
	p = r->pos;

	if (p < b->last) 
	{
		if (b->last - p < MY_HEADER_SIZE) {
			log_error("receive size small than package header. id:%d", r->id);
			goto error;
		}
		
		input_packet_length = uint3korr(p);
		p += 3;
		r->pkt_nr = (uint8_t)(*p);	// mysql sequence id
		p++;
		log_debug("pkt_nr:%d, packet len:%d", r->pkt_nr, input_packet_length);

		if(p + input_packet_length > b->last)
			goto error;

		if(r->owner->stage == CONN_STAGE_LOGGED_IN)
		{
			rc = my_get_command(p, input_packet_length, r->data, &command);
			if(rc)
			{
				if (rc < 0) 
				{
					log_error("parse command error:%d", rc);
					goto error;
				}

				r->command = command;
			}
		}

		p += input_packet_length;
		r->pos = p;
		
		goto success;
	}

error:
	r->pos = b->last;
	r->parse_res = MSG_PARSE_ERROR;
	log_debug("parse msg:%"PRIu64" error!", r->id);
	errno = EINVAL;
	log_debug("my_parse_req leave.");
	return;

success:
	r->parse_res = MSG_PARSE_OK;
	log_debug("parse msg:%"PRIu64" success!", r->id);
	log_debug("my_parse_req leave.");
	return;
}

void my_parse_rsp(struct msg *r) {
	struct mbuf *b;
	uint8_t *p;
	int ret;

	log_debug("my_parse_rsp entry.");

	b = STAILQ_LAST(&r->buf_q, mbuf, next);
	p = r->pos;
	r->token = NULL;

	if (p < b->last) 
	{
		if (b->last - p < sizeof(struct DTC_HEADER) + MY_HEADER_SIZE) {
			log_error("receive size small than package header. id:%d", r->id);
			p = b->last;
			r->parse_res = MSG_PARSE_ERROR;
			errno = EINVAL;
			return ;
		}
		r->peerid = ((struct DTC_HEADER*)p)->id;
		p = p + sizeof(struct DTC_HEADER);
		
		r->pkt_nr = (uint8_t)(p[3]);	// mysql sequence id
		log_debug("pkt_nr:%d, peerid:%d", r->pkt_nr, r->peerid);
		p = p + MY_HEADER_SIZE;

		p = b->last;
		r->pos = p;
		r->parse_res = MSG_PARSE_OK;
		log_debug("parse msg:%"PRIu64" success!", r->id);
	}
	else
	{
		r->parse_res = MSG_PARSE_ERROR;
		log_debug("parse msg:%"PRIu64" error!", r->id);
		errno = EINVAL;
	}

	log_debug("my_parse_rsp leave.");
	return;
}

int my_get_command(uint8_t *input_raw_packet, uint32_t input_packet_length, union COM_DATA* data, enum enum_server_command* cmd)
{
	*cmd = (enum enum_server_command)(uchar)input_raw_packet[0];

	if (*cmd >= COM_END) *cmd = COM_END;  // Wrong command

	if(parse_packet(input_raw_packet, input_packet_length, data, *cmd))
		return 1;

	return -1;
}

int my_do_command(struct msg *msg)
{
	int rc = NEXT_RSP_ERROR;

	switch (msg->command) {
    case COM_INIT_DB:
    case COM_REGISTER_SLAVE: 
    case COM_RESET_CONNECTION:
    case COM_CLONE: 
    case COM_CHANGE_USER: {
      rc = NEXT_RSP_OK;
      break;
    }
    case COM_STMT_EXECUTE: {
      rc = NEXT_FORWARD;
      break;
    }
    case COM_STMT_FETCH: {
      rc = NEXT_FORWARD;
      break;
    }
    case COM_STMT_SEND_LONG_DATA: {
      rc = NEXT_RSP_ERROR;
      break;
    }
    case COM_STMT_PREPARE: 
    case COM_STMT_CLOSE: 
    case COM_STMT_RESET: {
      rc = NEXT_RSP_ERROR;
      break;
    }
    case COM_QUERY: {
      rc = NEXT_FORWARD;
      break;
    }
    case COM_FIELD_LIST:  // This isn't actually needed
    case COM_QUIT:
    case COM_BINLOG_DUMP_GTID:
    case COM_BINLOG_DUMP:
    case COM_REFRESH: 
    case COM_STATISTICS: 
    case COM_PING:
    case COM_PROCESS_INFO:
    case COM_PROCESS_KILL:
    case COM_SET_OPTION:
    case COM_DEBUG:
      rc = NEXT_RSP_OK;
      break;
    case COM_SLEEP:
    case COM_CONNECT:         // Impossible here
    case COM_TIME:            // Impossible from client
    case COM_DELAYED_INSERT:  // INSERT DELAYED has been removed.
    case COM_END:
    default:
      log_error("error command:%d", msg->command);
	  rc = NEXT_RSP_ERROR;
      break;
  }

	return rc;
}