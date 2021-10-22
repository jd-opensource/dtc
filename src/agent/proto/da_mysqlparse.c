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

#define HEADER_SIZE 4
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


static int dtc_decode_value(enum fieldtype type, int lenth, uint8_t *p,
		CValue *val) {
	uint8_t *q;
	switch (type) {
	case None:
		break;
	case Signed:
	case Unsigned:
		if (lenth == 0 || lenth > 8) {
			goto decode_value_error;
		}
		q = (uint8_t *) p + 1;
		int64_t s64;
		s64 = *(int8_t *) p;
		switch (lenth) {
		case 8:
			s64 = (s64 << 8) | *q++;
		case 7:
			s64 = (s64 << 8) | *q++;
		case 6:
			s64 = (s64 << 8) | *q++;
		case 5:
			s64 = (s64 << 8) | *q++;
		case 4:
			s64 = (s64 << 8) | *q++;
		case 3:
			s64 = (s64 << 8) | *q++;
		case 2:
			s64 = (s64 << 8) | *q++;
		}
		val->s64 = s64;
		break;
	case Float:
		if (lenth < 3)
			goto decode_value_error;
		if (p[lenth - 1] != '\0')
			goto decode_value_error;
		if (!strcmp((char *) p, "NAN"))
			val->flt = NAN;
		else if (!strcmp((char *) p, "INF"))
			val->flt = INFINITY;
		else if (!strcmp((char *) p, "-INF"))
			val->flt = -INFINITY;
		else {
			long double ldf;
			if (sscanf((char *) p, __FLTFMT__, &ldf) != 1)
				goto decode_value_error;
			val->flt = ldf;
		}
		break;
	case String:
	case Binary:
		if (lenth == 0) {
#if CONVERT_NULL_TO_EMPTY_STRING
			val->str.data = p;
			val->str.len = 0;
#else
			val->str.data=NULL;
			val->str.len=0;
#endif
		} else {
			if (p[lenth - 1] != '\0')
				goto decode_value_error;
			val->str.data = p;
			val->str.len = lenth - 1;
		}

		break;
	default:
		goto decode_value_error;
	}
	return 0;
	decode_value_error: return -1;
}

static int dtc_decode_lenth(uint8_t **p) {
	int len = **p;
	*p += 1;
	if (len < 240) {
	} else if (len <= 252) {
		len = ((len & 0xF) << 8) + **p;
		*p += 1;
	} else if (len == 253) {
		len = ((**p) << 8) + (*(*p + 1));
		*p = *p + 2;
	} else if (len == 254) {
		len = ((**p) << 16) + (*(*p + 1) << 8) + (*(*p + 2));
		*p = *p + 3;
	} else {
		len = ((**p) << 24) + (*(*p + 1) << 16) + (*(*p + 2) << 8)
				+ (*(*p + 3));
		*p = *p + 4;
		if (len > MAXPACKETSIZE) {
			return -1;
		}
	}
	return len;
}

static int next_field(struct msg *r, int field) {
	int i;
	for (i = field; i < FD_TOTALFIELD - 1; i++) {
		if (r->seclen[i] != 0) {
			return i + 1;
		}
	}
	return FD_TOTALFIELD;
}

/*
 * parse request msg
 */
void mysql_parse_req(struct msg *r) {
	struct mbuf *b;
	uint8_t *p;
	int ret;

	int state;
	int field;

	field = r->field;
	state = r->state;

	b = STAILQ_LAST(&r->buf_q, mbuf, next);
	p = r->pos;

	while (p < b->last) {
		r->token = p;
		if (b->last - p < HEADER_SIZE) {
			log_debug("receive size small than package header!");
			p = b->last;
			break;
		}

		p = p + HEADER_SIZE;
		//TODO 手动设置最后调试
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
	log_debug("paese msg:%"PRIu64" success!", r->id);
	r->pos = p;
	r->state = ST_ID;
	r->field = FD_VERSION;
	r->token = NULL;
	r->parse_res = MSG_PARSE_OK;
	return;
	error:
	log_debug("paese msg:%"PRIu64" error!", r->id);
	r->parse_res = MSG_PARSE_ERROR;
	r->state = state;
	r->field = field;
	r->token = NULL;
	errno = EINVAL;
	return;
}


int my_server_greeting_reply(struct msg *smsg, struct msg *dmsg) {
	int ret, resultlen = 0, versionlen = 0;
	struct mbuf *start_buf, *end_buf;

	start_buf = mbuf_get();
	//start_buf->last += HEADER_SIZE;
	//dmsg->mlen += HEADER_SIZE;
	mbuf_insert(&dmsg->buf_q, start_buf);
	

	uint8_t buf[78] = {0x4a, 0x00, 0x00, 0x00, 0x0a, 0x38, 0x2e, 0x30, 0x2e, 0x32, 0x36, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x6f, 0x3c, 0x36, 0x36, 0x03, 0x68, 0x38, 0x46, 0x00, 0xff, 0xf7, 0xff, 0x02, 0x00, 0xff, 0x8f, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x2a, 0x0a, 0x60, 0x5c, 0x68, 0x50, 0x34, 0x6b, 0x0e, 0x27, 0x73, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00};

	mbuf_copy(start_buf, buf, 78);
	dmsg->mlen = 78;

	log_debug("dmsg len:%d", dmsg->mlen);
	return 0;
}



int my_ok_reply(struct msg *smsg, struct msg *dmsg) {
	int ret, resultlen = 0, versionlen = 0;
	struct mbuf *start_buf, *end_buf;

	start_buf = mbuf_get();
	//start_buf->last += HEADER_SIZE;
	//dmsg->mlen += HEADER_SIZE;
	mbuf_insert(&dmsg->buf_q, start_buf);
	
	
	uint8_t buf[11] = {0x07, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00};

	mbuf_copy(start_buf, buf, 11);
	dmsg->mlen = 11;

	log_debug("dmsg len:%d", dmsg->mlen);
	return 0;
}
