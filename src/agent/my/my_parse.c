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
	int ret;

	int state;
	int field;

	field = r->field;
	state = r->state;

	log_debug("my_parse_req entry.");

	b = STAILQ_LAST(&r->buf_q, mbuf, next);
	p = r->pos;

	while (p < b->last) {
		r->token = p;
		if (b->last - p < MY_HEADER_SIZE) {
			log_debug("receive size small than package header!");
			p = b->last;
			break;
		}
		
		r->pkt_nr = (uint8_t)(p[3]);	// mysql sequence id
		log_debug("pkt_nr:%d", r->pkt_nr);
		p = p + MY_HEADER_SIZE;

		if(r->owner->stage == CONN_STAGE_LOGGED_IN)
		{
			;
		}

		p = b->last;
		goto success;
	}

	ASSERT(p == b->last);
	if (r->token != NULL) {
		r->pos = r->token;
	} else {
		r->pos = p;
	}
	r->state = state;
	r->field = field;
	r->token = NULL;
	if (b->last == b->end) {
		r->parse_res = MSG_PARSE_REPAIR;
	} else {
		r->parse_res = MSG_PARSE_AGAIN;
	}
	return;
success:
	log_debug("parse msg:%"PRIu64" success!", r->id);
	r->pos = p;
	r->state = ST_ID;
	r->field = FD_VERSION;
	r->token = NULL;
	r->parse_res = MSG_PARSE_OK;
	return;
	error:
	log_debug("parse msg:%"PRIu64" error!", r->id);
	r->parse_res = MSG_PARSE_ERROR;
	r->state = state;
	r->field = field;
	r->token = NULL;
	errno = EINVAL;

	log_debug("my_parse_req leave.");
	return;
}

void my_parse_rsp(struct msg *r) {
	struct mbuf *b;
	uint8_t *p;
	int ret;

	int state;
	int field;

	field = r->field;
	state = r->state;

	log_debug("my_parse_rsp entry.");

	b = STAILQ_LAST(&r->buf_q, mbuf, next);
	p = r->pos;

	while (p < b->last) {
		r->token = p;
		if (b->last - p < sizeof(struct DTC_HEADER) + MY_HEADER_SIZE) {
			log_debug("receive size small than package header!");
			p = b->last;
			break;
		}
		r->peerid = ((struct DTC_HEADER*)p)->id;
		p = p + sizeof(struct DTC_HEADER);
		
		r->pkt_nr = (uint8_t)(p[3]);	// mysql sequence id
		log_debug("pkt_nr:%d, peerid:%d", r->pkt_nr, r->peerid);
		p = p + MY_HEADER_SIZE;

		p = b->last;
		goto success;
	}

	ASSERT(p == b->last);
	if (r->token != NULL) {
		r->pos = r->token;
	} else {
		r->pos = p;
	}
	r->state = state;
	r->field = field;
	r->token = NULL;
	if (b->last == b->end) {
		r->parse_res = MSG_PARSE_REPAIR;
	} else {
		r->parse_res = MSG_PARSE_AGAIN;
	}
	return;
	success:
	log_debug("parse msg:%"PRIu64" success!", r->id);
	r->pos = p;
	r->state = ST_ID;
	r->field = FD_VERSION;
	r->token = NULL;
	r->parse_res = MSG_PARSE_OK;
	return;
	error:
	log_debug("parse msg:%"PRIu64" error!", r->id);
	r->parse_res = MSG_PARSE_ERROR;
	r->state = state;
	r->field = field;
	r->token = NULL;
	errno = EINVAL;

	log_debug("my_parse_rsp leave.");
	return;

}