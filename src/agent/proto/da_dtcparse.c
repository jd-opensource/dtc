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

#define HEADER_SIZE sizeof(struct CPacketHeader)
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

enum fieldtype versionInfoDefinition[19] = { None, String,    // 1 -- table name
		Binary, // 2 -- data table hash [for hb register use, unless always send 0x00000...]
		Unsigned,  // 3 -- serial#
		Binary,    // 4 -- real table hash
		String,    // 5 -- client version
		String,    // 6 -- ctlib version
		String,    // 7 -- helper version [unuse]
		Unsigned,  // 8 -- keepalive timeout
		Unsigned,  // 9 -- key type
		Unsigned,  // 10 -- key field count
		Unsigned,  // 11 -- key value count
		Binary,    // 12 -- key type list
		Binary,    // 13 -- key name list
		Unsigned,  // 14 -- hot backup ID
		Signed,    // 15 -- hot backup master timestamp
		Signed,    // 16 -- hot backup slave timestamp
		Unsigned,  // 17 -- agent client id
		String,    // 18 -- accessKey
		};

enum fieldtype requestInfoDefinition[8] = { Binary,   // 0 -- key
		Unsigned, // 1 -- timeout
		Unsigned, // Limit start
		Unsigned, // Limit count
		String,	  // 4 -- trace msg
		Unsigned, // 5 -- Cache ID -- OBSOLETED
		String,   // 6 -- raw config string
		Unsigned, // 7 -- admin cmd code
		};
enum fieldtype resultinfoDefinition[10] = { Binary, // 0 -- key, original key or INSERT_ID
		Signed,   // 1 -- code
		String,   // 2 -- trace msg
		String,   // 3 -- from
		Signed,   // 4 -- affected rows, by delete/update
		Unsigned, // 5 -- total rows, not used. for SELECT SQL_CALC_FOUND_ROWS...
		Unsigned, // 6 -- insert_id, not used. for SELECT SQL_CALC_FOUND_ROWS...
		Binary,   // 7 -- query server info: version, binlogid,....
		Unsigned, // 8 -- server timestamp
		Unsigned, // 9 -- Hit Flag
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
void dtc_parse_req(struct msg *r) {
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
		switch (field) {
		case FD_HEADER: {
			r->token = p;
			if (b->last - p < HEADER_SIZE) {
				log_debug("receive size small than package header!");
				p = b->last;
				break;
			}
			struct CPacketHeader *pheader;
			pheader = (struct CPacketHeader *) p;
			r->seclen = pheader->len;
			r->cmd = (msg_type_t) pheader->cmd;
			r->flags = pheader->flags;
			log_debug("seclen:%d,%d,%d,%d,%d,%d,%d,%d", r->seclen[0],
					r->seclen[1], r->seclen[2], r->seclen[3], r->seclen[4],
					r->seclen[5], r->seclen[6], r->seclen[7]);
			p = p + HEADER_SIZE;
			field = next_field(r, FD_HEADER);
			if (field == FD_TOTALFIELD) {
				log_debug("all fields has been parsed,parse done!");
				goto success;
			}
			log_debug("next field to be parse:%d", field);
			state = ST_ID;

			r->sec_parsed_len = 0;
			r->parsed_len += HEADER_SIZE;
			r->token = NULL;
			break;
		}
		case FD_VERSION: {
			r->token = p;
			switch (state) {
			case ST_ID: {
				r->cur_parse_id = *(uint8_t *) p++;
				if (r->cur_parse_id > 18) {
					log_error("id:%d,version hasn't slot id max than 18",
							r->cur_parse_id);
					goto error;
				}
				//record len from start to keycount
				if (r->cur_parse_id == 11) {
					r->keycountstartlen = r->parsed_len;
				}
				r->cur_parse_type = versionInfoDefinition[r->cur_parse_id];
				++r->sec_parsed_len;
				++r->parsed_len;
				state = ST_LENTH;
				r->token = NULL;
				break;
			}
			case ST_LENTH: {
				int len = dtc_decode_lenth(&p);
				if (p >= b->last) {
					p = b->last;
					break;
				}

				if (r->cur_parse_id == 11) {
					r->keycountlen += (p - r->token) + len + 1;
				}

				r->cur_parse_lenth = len;
				r->parsed_len += p - r->token;
				r->sec_parsed_len += p - r->token;
				r->token = NULL;
				state = ST_VALUE;
				break;
			}
			case ST_VALUE: {
				CValue tempval = { 0 };
				switch (r->cur_parse_id) {
				case 3: {
					if (b->last - p < r->cur_parse_lenth) {
						log_debug(
								"parse version info serialnr id:%d,len:%d,not in one buff!",
								r->cur_parse_id, r->cur_parse_lenth);
						p = b->last;
						break;
					}
					ret = dtc_decode_value(r->cur_parse_type,
							r->cur_parse_lenth, p, &tempval);
					if (ret < 0) {
						log_error(
								"decode version info serialnr id:%d,length:%d error",
								r->cur_parse_id, r->cur_parse_lenth);
						goto error;
					}
					r->serialnr = tempval.u64;
					log_debug(
							"parse version info id:%d success,serialnr: %"PRIu64"",
							r->cur_parse_id, r->serialnr);

					p += r->cur_parse_lenth;
					r->sec_parsed_len += r->cur_parse_lenth;
					r->parsed_len += r->cur_parse_lenth;
					r->token = NULL;
					state = ST_ID;
					break;
				}
				case 18: {
					if (b->last - p < r->cur_parse_lenth) {
						log_debug(
								"parse version info accesstoken id:%d,len:%d,not in one buff!",
								r->cur_parse_id, r->cur_parse_lenth);
						p = b->last;
						break;
					}
					ret = dtc_decode_value(r->cur_parse_type,
							r->cur_parse_lenth, p, &tempval);
					if (ret < 0) {
						log_error(
								"decode version info accesstoken id:%d,length:%d error",
								r->cur_parse_id, r->cur_parse_lenth);
						goto error;
					}
					r->accesskey.data = tempval.str.data;
					r->accesskey.len = tempval.str.len;
					log_debug(
							"parse version info id:%d success,accesstoken '%.*s'",
							r->cur_parse_id, r->accesskey.len,
							r->accesskey.data);

					p += r->cur_parse_lenth;
					r->sec_parsed_len += r->cur_parse_lenth;
					r->parsed_len += r->cur_parse_lenth;
					r->token = NULL;
					state = ST_ID;
					break;
				}
				case 9: {
					if (b->last - p < r->cur_parse_lenth) {
						log_debug(
								"parse version info keytype id:%d,len:%d,not in one buff!",
								r->cur_parse_id, r->cur_parse_lenth);
						p = b->last;
						break;
					}
					ret = dtc_decode_value(r->cur_parse_type,
							r->cur_parse_lenth, p, &tempval);
					if (ret < 0) {
						log_error(
								"decode version info keytype id:%d,length:%d error",
								r->cur_parse_id, r->cur_parse_lenth);
						goto error;
					}

					r->keytype = tempval.u64;

					log_debug(
							"parse version info id:%d success,keytype %"PRIu64"",
							r->cur_parse_id, r->keytype);
					p += r->cur_parse_lenth;
					r->sec_parsed_len += r->cur_parse_lenth;
					r->parsed_len += r->cur_parse_lenth;
					r->token = NULL;
					state = ST_ID;
					break;
				}
				case 11: {
					if (b->last - p < r->cur_parse_lenth) {
						log_debug(
								"parse version info keyCount id:%d,len:%d,not in one buff!",
								r->cur_parse_id, r->cur_parse_lenth);
						p = b->last;
						break;
					}
					ret = dtc_decode_value(r->cur_parse_type,
							r->cur_parse_lenth, p, &tempval);
					if (ret < 0) {
						log_error(
								"decode version info keyCount id:%d,length:%d error",
								r->cur_parse_id, r->cur_parse_lenth);
						goto error;
					}
					r->keyCount = tempval.s64;

					log_debug(
							"parse version info id:%d success,keycount %"PRIu64"",
							r->cur_parse_id, r->keyCount);
					p += r->cur_parse_lenth;
					r->keycountendbuf = b;
					r->keycountendpos = p;

					r->sec_parsed_len += r->cur_parse_lenth;
					r->parsed_len += r->cur_parse_lenth;
					r->token = NULL;
					state = ST_ID;
					break;
				}
				default: {
					if (b->last - p < r->cur_parse_lenth) {
						r->cur_parse_lenth -= b->last - p;
						r->parsed_len += b->last - p;
						r->sec_parsed_len += b->last - p;
						log_debug("parse version info id:%d not in one buff!",
								r->cur_parse_id);
						r->token = NULL;
						p = b->last;

					} else {
						p += r->cur_parse_lenth;
						r->parsed_len += r->cur_parse_lenth;
						r->sec_parsed_len += r->cur_parse_lenth;
						r->token = NULL;
						state = ST_ID;
					}
				}
				}
				if (r->sec_parsed_len > r->seclen[0]) {
					log_error("parse version error,now parsed len:%d "
							"max than version info len:%d,state:%d",
							r->sec_parsed_len, r->seclen[0], state);
					goto error;
				}
				if (r->sec_parsed_len == r->seclen[0]) {
					field = next_field(r, FD_VERSION);
					if (field == FD_TOTALFIELD) {
						log_debug("all fields has been parsed,parse done!");
						goto success;
					}
					log_debug("next field to be parse:%d", field);
					state = ST_ID;
					r->sec_parsed_len = 0;
					r->token = NULL;
				}
				break;
			}
			}
			break;
		}
		case FD_TABLEDEFINE: {
			if (b->last - p < r->seclen[1] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[1] - r->sec_parsed_len;
				r->parsed_len += r->seclen[1] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[1] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[1]) {
				field = next_field(r, FD_TABLEDEFINE);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_REQUEST: {
			r->token = p;
			switch (state) {
			case ST_ID: {
				r->cur_parse_id = *(uint8_t *) p++;
				if (r->cur_parse_id > 7) {
					log_error("id:%d,request info hasn't slot id max than 7",
							r->cur_parse_id);
					goto error;
				}
				r->cur_parse_type = requestInfoDefinition[r->cur_parse_id];
				if (r->cur_parse_id == 0 && !(r->flags & MultiKeyValue)) {
					r->cur_parse_type = r->keytype;
				}
				log_debug("request parse %d", r->cur_parse_id);
				++r->sec_parsed_len;
				++r->parsed_len;
				state = ST_LENTH;
				r->token = NULL;
				break;
			}
			case ST_LENTH: {
				int len = dtc_decode_lenth(&p);
				if (p >= b->last) {
					p = b->last;
					break;
				}
				log_debug("request parse id:%d len:%d", r->cur_parse_id, len);
				if (r->cur_parse_id == 0) {
					r->keylen += (p - r->token) + len + 1;
				}

				r->cur_parse_lenth = len;
				r->parsed_len += p - r->token;
				r->sec_parsed_len += p - r->token;
				r->token = NULL;
				state = ST_VALUE;
				break;
			}
			case ST_VALUE: {
				CValue tempval = { 0 };
				if (r->cur_parse_id == 0) {
					if (r->cur_parse_type == None
							|| r->cur_parse_type == Float) {
						log_error("key type error,key type is %"PRIu64"",
								r->keytype);
						goto error;
					}
					if (!(r->flags & MultiKeyValue)) {
						if (b->last - p < r->cur_parse_lenth) {
							log_debug(
									"parse request info single key id:%d,len:%d,not in one buff!",
									r->cur_parse_id, r->cur_parse_lenth);
							p = b->last;
							break;
						}
						r->keys[0].start = p;
						r->keys[0].end = p + r->cur_parse_lenth;

						if (r->keytype == String || r->keytype == Binary) {
							r->keys[0].end -= 1;  //Remove the trailing \0 from string
						}

						log_debug(
								"parse request info id:%d success,single key '%.*s'",
								r->cur_parse_id,
								(int )(r->keys[0].end - r->keys[0].start),
								r->keys[0].start);

						p += r->cur_parse_lenth;

						r->keyendbuf = b;
						r->keyendpos = p;

						r->keyCount = 1;
						r->sec_parsed_len += r->cur_parse_lenth;
						r->parsed_len += r->cur_parse_lenth;
						r->token = NULL;
						state = ST_ID;
					} else {
						if (r->subkeylen == 0) {
							if (b->last - p < sizeof(uint32_t)) {
								p = b->last;
								break;
							}
							switch (r->keytype) {
							case Signed:
							case Unsigned:
								r->subkeylen = sizeof(uint64_t);
								break;
							case String:
							case Binary:
								r->subkeylen = *(uint32_t *) p
										+ sizeof(uint32_t);
								//p = p + sizeof(uint32_t);
								//r->cur_parse_lenth -= sizeof(uint32_t);
								//r->parsed_len += sizeof(uint32_t);
								break;
							default:
								goto error;
							}
						}
						if (b->last - p < r->subkeylen) {
							p = b->last;
							break;
						}

						log_debug(
								"the count:%d,sub key len:%d,current parse len:%d",
								r->subkeycount, r->subkeylen,
								r->cur_parse_lenth);
						r->keys[r->subkeycount].start = p;
						r->keys[r->subkeycount].end = p + r->subkeylen;

						log_debug(
								"parse request info id:%d success,multi key '%.*s'",
								r->cur_parse_id, r->subkeylen,
								r->keys[r->subkeycount].start);

						++r->subkeycount;
						p += r->subkeylen;
						r->subkeylen = 0;

						if (r->subkeycount >= r->keyCount) {
							r->parsed_len += r->cur_parse_lenth;
							r->sec_parsed_len += r->cur_parse_lenth;
							++p;
							r->keyendbuf = b;
							r->keyendpos = p;
							r->token = NULL;
							state = ST_ID;
						}

					}
				} else if (r->cur_parse_id == 2 || r->cur_parse_id == 3) {
					if (b->last - p < r->cur_parse_lenth) {
						log_debug(
								"parse request info limit info id:%d,len:%d,not in one buff!",
								r->cur_parse_id, r->cur_parse_lenth);
						p = b->last;
						break;
					}
					ret = dtc_decode_value(r->cur_parse_type,
							r->cur_parse_lenth, p, &tempval);
					if (ret < 0) {
						log_error(
								"decode request info limit info id:%d,length:%d error",
								r->cur_parse_id, r->cur_parse_lenth);
						goto error;
					}
					if ((r->flags & MultiKeyValue) && tempval.u64 != 0) {
						log_error("multi get request not support limit!");
						goto error;
					}

					log_debug(
							"decode request info limit info id:%d,length:%d val:%"PRIu64" success",
							r->cur_parse_id, r->cur_parse_lenth,tempval.u64);
					p += r->cur_parse_lenth;
					r->sec_parsed_len += r->cur_parse_lenth;
					r->parsed_len += r->cur_parse_lenth;
					r->token = NULL;
					state = ST_ID;
				} else {
					log_debug("buf len:%lu", b->last - p);
					if (b->last - p < r->cur_parse_lenth) {
						r->cur_parse_lenth -= b->last - p;
						r->parsed_len += b->last - p;
						r->sec_parsed_len += b->last - p;
						r->token = NULL;
						p = b->last;

					} else {
						p += r->cur_parse_lenth;
						r->parsed_len += r->cur_parse_lenth;
						r->sec_parsed_len += r->cur_parse_lenth;
						r->token = NULL;
						state = ST_ID;
					}
				}

				if (r->sec_parsed_len > r->seclen[2]) {
					log_error("parse request error,now parsed len:%d "
							"max than version info len:%d,state:%d",
							r->sec_parsed_len, r->seclen[2], state);
					goto error;
				}
				if (r->sec_parsed_len == r->seclen[2]) {
					field = next_field(r, FD_REQUEST);
					if (field == FD_TOTALFIELD) {
						log_debug("all fields has been parsed,parse done!");
						goto success;
					}
					log_debug("next field to be parse:%d", field);
					state = ST_ID;
					r->sec_parsed_len = 0;
					r->token = NULL;

				}
				break;
			}
			}

			break;
		}
		case FD_RESULTINFO: {
			if (b->last - p < r->seclen[3] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[3] - r->sec_parsed_len;
				r->parsed_len += r->seclen[3] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[3] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[3]) {
				field = next_field(r, FD_RESULTINFO);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_UPDATEINFO: {
			if (b->last - p < r->seclen[4] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[4] - r->sec_parsed_len;
				r->parsed_len += r->seclen[4] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[4] - r->sec_parsed_len;
				r->token = NULL;
			}

			if (r->sec_parsed_len == r->seclen[4]) {
				field = next_field(r, FD_UPDATEINFO);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_CONDITIONINFO: {
			if (b->last - p < r->seclen[5] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[5] - r->sec_parsed_len;
				r->parsed_len += r->seclen[5] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[5] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[5]) {
				field = next_field(r, FD_CONDITIONINFO);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_FIELDSET: {
			if (b->last - p < r->seclen[6] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[6] - r->sec_parsed_len;
				r->parsed_len += r->seclen[6] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[6] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[6]) {
				field = next_field(r, FD_FIELDSET);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_RESULTSET: {
			if (b->last - p < r->seclen[7] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[7] - r->sec_parsed_len;
				r->parsed_len += r->seclen[7] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[7] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[7]) {
				field = next_field(r, FD_RESULTSET);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}

		}
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

void dtc_parse_rsp(struct msg *r) {
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

		switch (field) {
		case FD_HEADER: {
			r->token = p;
			if (b->last - p < HEADER_SIZE) {
				log_debug("receive size small than package header!");
				p = b->last;
				break;
			}
			struct CPacketHeader *pheader;
			pheader = (struct CPacketHeader *) p;
			r->seclen = pheader->len;
			r->cmd = (msg_type_t) pheader->cmd;
			r->flags = pheader->flags;
			log_debug("seclen:%d,%d,%d,%d,%d,%d,%d,%d", r->seclen[0],
					r->seclen[1], r->seclen[2], r->seclen[3], r->seclen[4],
					r->seclen[5], r->seclen[6], r->seclen[7]);
			p = p + HEADER_SIZE;
			field = next_field(r, FD_HEADER);
			if (field == FD_TOTALFIELD) {
				log_debug("all fields has been parsed,parse done!");
				goto success;
			}
			log_debug("next field to be parse:%d", field);
			state = ST_ID;
			r->sec_parsed_len = 0;
			r->parsed_len += HEADER_SIZE;
			r->token = NULL;
			break;

		}
		case FD_VERSION: {
			r->token = p;
			switch (state) {
			case ST_ID: {
				r->cur_parse_id = *(uint8_t *) p++;
				if (r->cur_parse_id > 18) {
					log_error("id:%d,version hasn't slot id max than 18",
							r->cur_parse_id);
					goto error;
				}
				r->cur_parse_type = versionInfoDefinition[r->cur_parse_id];
				++r->sec_parsed_len;
				++r->parsed_len;
				state = ST_LENTH;
				r->token = NULL;
				break;
			}
			case ST_LENTH: {
				int len = dtc_decode_lenth(&p);
				if (p >= b->last) {
					p = b->last;
					break;
				}

				r->cur_parse_lenth = len;
				r->parsed_len += p - r->token;
				r->sec_parsed_len += p - r->token;
				r->token = NULL;
				state = ST_VALUE;
				break;
			}
			case ST_VALUE: {
				CValue tempval = { 0 };
				if (r->cur_parse_id == 17) {
					if (b->last - p < r->cur_parse_lenth) {
						log_debug(
								"parse version info agentid id:%d,len:%d,not in one buff!",
								r->cur_parse_id, r->cur_parse_lenth);
						p = b->last;
						break;
					}
					ret = dtc_decode_value(r->cur_parse_type,
							r->cur_parse_lenth, p, &tempval);
					if (ret < 0) {
						log_error(
								"decode version info agentid id:%d,length:%d error",
								r->cur_parse_id, r->cur_parse_lenth);
						goto error;
					}
					r->peerid = tempval.u64;

					log_debug(
							"parse version info id:%d success,agentid %"PRIu64"",
							r->cur_parse_id, r->peerid);

					p += r->cur_parse_lenth;
					r->sec_parsed_len += r->cur_parse_lenth;
					r->parsed_len += r->cur_parse_lenth;
					r->token = NULL;
					state = ST_ID;
				} else {
					log_debug("buf len:%lu", b->last - p);
					if (b->last - p < r->cur_parse_lenth) {
						r->cur_parse_lenth -= b->last - p;
						r->parsed_len += b->last - p;
						r->sec_parsed_len += b->last - p;
						r->token = NULL;
						p = b->last;

					} else {
						p += r->cur_parse_lenth;
						r->parsed_len += r->cur_parse_lenth;
						r->sec_parsed_len += r->cur_parse_lenth;
						r->token = NULL;
						state = ST_ID;
					}
				}
				if (r->sec_parsed_len > r->seclen[0]) {
					log_error("parse version error,now parsed len:%d "
							"max than version info len:%d,state:%d",
							r->sec_parsed_len, r->seclen[0], state);
					goto error;
				}
				if (r->sec_parsed_len == r->seclen[0]) {
					field = next_field(r, FD_VERSION);
					if (field == FD_TOTALFIELD) {
						log_debug("all fields has been parsed,parse done!");
						goto success;
					}
					log_debug("next field to be parse:%d", field);
					state = ST_ID;
					r->sec_parsed_len = 0;
					r->token = NULL;
				}
				break;
			}
			}
			break;
		}
		case FD_TABLEDEFINE: {
			if (b->last - p < r->seclen[1] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[1] - r->sec_parsed_len;
				r->parsed_len += r->seclen[1] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[1] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[1]) {
				field = next_field(r, FD_TABLEDEFINE);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_REQUEST: {
			if (b->last - p < r->seclen[2] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[2] - r->sec_parsed_len;
				r->parsed_len += r->seclen[2] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[2] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[2]) {
				field = next_field(r, FD_TABLEDEFINE);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_RESULTINFO: {
			r->token = p;
			switch (state) {
			case ST_ID: {
				r->cur_parse_id = *(uint8_t *) p++;
				if (r->cur_parse_id > 9) {
					log_error("id:%d,result info hasn't slot id max than 9",
							r->cur_parse_id);
					goto error;
				}

				if (r->cur_parse_id == 5) {
					r->keyendbuf = b;
					r->keyendpos = p - 1;
				}

				r->cur_parse_type = resultinfoDefinition[r->cur_parse_id];
				++r->sec_parsed_len;
				++r->parsed_len;
				state = ST_LENTH;
				r->token = NULL;
				break;
			}
			case ST_LENTH: {
				int len = dtc_decode_lenth(&p);
				if (p >= b->last) {
					p = b->last;
					break;
				}

				if (r->cur_parse_id == 5) {
					r->keylen = (p - r->token) + len + 1;
				}

				r->cur_parse_lenth = len;
				r->parsed_len += p - r->token;
				r->sec_parsed_len += p - r->token;
				r->token = NULL;
				state = ST_VALUE;
				break;
			}
			case ST_VALUE: {
				CValue tempval = { 0 };
				if (r->cur_parse_id == 1 || r->cur_parse_id == 4
						|| r->cur_parse_id == 5 || r->cur_parse_id == 9) {
					if (b->last - p < r->cur_parse_lenth) {
						log_debug(
								"parse result info resultcode | affectrow | totalrow | hitflag id:%d,len:%d,not in one buff!",
								r->cur_parse_id, r->cur_parse_lenth);
						p = b->last;
						break;
					}
					ret = dtc_decode_value(r->cur_parse_type,
							r->cur_parse_lenth, p, &tempval);
					if (ret < 0) {
						log_error(
								"decode result info resultcode | affectrow | totalrow id:%d,length:%d error",
								r->cur_parse_id, r->cur_parse_lenth);
						goto error;
					}
					if (r->cur_parse_id == 1) {
						r->resultcode = tempval.s64;
						log_debug(
								"parse result info id:%d success,resultcode %"PRId64"",
								r->cur_parse_id, r->resultcode);
					} else if (r->cur_parse_id == 4) {
						r->affectrows = tempval.s64;
						log_debug(
								"parse result info id:%d success,affectrows %"PRIu64"",
								r->cur_parse_id, r->affectrows);
					} else if (r->cur_parse_id == 9) {
						r->hitflag = tempval.s64;
						log_debug(
								"parse result info id:%d success,affectrows %"PRIi64"",
								r->cur_parse_id, r->hitflag);
					} else {
						r->totalrows = tempval.u64;
						log_debug(
								"parse result info id:%d success,totalrows %"PRIu64"",
								r->cur_parse_id, r->totalrows);
					}
					p += r->cur_parse_lenth;
					r->sec_parsed_len += r->cur_parse_lenth;
					r->parsed_len += r->cur_parse_lenth;
					r->token = NULL;
					state = ST_ID;
				} else {
					log_debug("buf len:%lu", b->last - p);
					if (b->last - p < r->cur_parse_lenth) {
						r->cur_parse_lenth -= b->last - p;
						r->parsed_len += b->last - p;
						r->sec_parsed_len += b->last - p;
						r->token = NULL;
						p = b->last;

					} else {
						p += r->cur_parse_lenth;
						r->parsed_len += r->cur_parse_lenth;
						r->sec_parsed_len += r->cur_parse_lenth;
						r->token = NULL;
						state = ST_ID;
					}
				}
				if (r->sec_parsed_len > r->seclen[3]) {
					log_error("parse version error,now parsed len:%d "
							"max than version info len:%d,state:%d",
							r->sec_parsed_len, r->seclen[3], state);
					goto error;
				}
				if (r->sec_parsed_len == r->seclen[3]) {
					field = next_field(r, FD_RESULTINFO);
					if (field == FD_TOTALFIELD) {
						log_debug("all fields has been parsed,parse done!");
						goto success;
					}
					log_debug("next field to be parse:%d", field);
					state = ST_ID;
					r->sec_parsed_len = 0;
					r->token = NULL;
				}
				break;
			}
			}
			break;
		}
		case FD_UPDATEINFO: {
			if (b->last - p < r->seclen[4] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[4] - r->sec_parsed_len;
				r->parsed_len += r->seclen[4] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[4] - r->sec_parsed_len;
				r->token = NULL;
			}

			if (r->sec_parsed_len == r->seclen[4]) {
				field = next_field(r, FD_UPDATEINFO);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_CONDITIONINFO: {
			if (b->last - p < r->seclen[5] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[5] - r->sec_parsed_len;
				r->parsed_len += r->seclen[5] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[5] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[5]) {
				field = next_field(r, FD_CONDITIONINFO);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_FIELDSET: {
			if (b->last - p < r->seclen[6] - r->sec_parsed_len) {
				r->parsed_len += b->last - p;
				r->sec_parsed_len += b->last - p;
				r->token = NULL;
				p = b->last;
			} else {
				p += r->seclen[6] - r->sec_parsed_len;
				r->parsed_len += r->seclen[6] - r->sec_parsed_len;
				r->sec_parsed_len += r->seclen[6] - r->sec_parsed_len;
				r->token = NULL;
			}
			if (r->sec_parsed_len == r->seclen[6]) {
				field = next_field(r, FD_FIELDSET);
				if (field == FD_TOTALFIELD) {
					log_debug("all fields has been parsed,parse done!");
					goto success;
				}
				log_debug("next field to be parse:%d", field);
				state = ST_ID;
				r->sec_parsed_len = 0;
				r->token = NULL;
			}
			break;
		}
		case FD_RESULTSET: {
			r->token = p;
			switch (state) {
			case ST_ID: {
				if (r->numrows != 0) {
					if (b->last - p < r->seclen[7] - r->sec_parsed_len) {
						r->parsed_len += b->last - p;
						r->sec_parsed_len += b->last - p;
						r->token = NULL;
						p = b->last;
					} else {
						p += r->seclen[7] - r->sec_parsed_len;
						r->parsed_len += r->seclen[7] - r->sec_parsed_len;
						r->sec_parsed_len += r->seclen[7] - r->sec_parsed_len;
						r->token = NULL;
					}
					if (r->sec_parsed_len == r->seclen[7]) {
						field = next_field(r, FD_RESULTSET);
						if (field == FD_TOTALFIELD) {
							log_debug("all fields has been parsed,parse done!");
							goto success;
						}
						log_debug("next field to be parse:%d", field);
						state = ST_ID;
						r->sec_parsed_len = 0;
						r->token = NULL;
					}
					break;
				} else {
					state = ST_LENTH;
					r->token = NULL;
					break;
				}
			}
			case ST_LENTH: {
				int len = dtc_decode_lenth(&p);
				if (p >= b->last) {
					p = b->last;
					break;
				}

				r->numrows = len;
				log_debug("parse result set numrows success,numrows %"PRIi64"",
						r->numrows);

				r->keycountendbuf = b;
				r->keycountendpos = r->token;
				r->keycountlen = p - r->token;

				r->parsed_len += p - r->token;
				r->sec_parsed_len += p - r->token;
				r->token = NULL;
				state = ST_VALUE;
				break;

			}
			case ST_VALUE: {
				if (r->fieldnums == 0) {
					r->fieldnums = *(uint8_t *) p++;
					log_debug("decode msg fieldnum:%d", r->fieldnums);
				}
				if (b->last - p < r->fieldnums) {
					r->fieldnums -= b->last - p;
					p = b->last;
					r->sec_parsed_len += p - r->token;
					r->parsed_len += p - r->token;
					r->token = NULL;
					break;
				} else {
					p += r->fieldnums;
					r->fieldnums = 0;

					r->setsplitbuf = b;
					r->setsplitpos = p;

					r->sec_parsed_len += p - r->token;
					r->parsed_len += p - r->token;
					r->token = NULL;
					state = ST_ID;

					if(b->last == p) {
						field = next_field(r, FD_RESULTSET);
						if(field == FD_TOTALFIELD) {
							log_debug("all fields has been parsed,parse done!");
							goto success;
						}
					}
					break;
				}
			}
			}
			break;
		}
		}

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

static uint8_t dtc_int_len_calc(int64_t val) {
	uint8_t n;
	if (val >= 0) {
		if (val < 0x80LL)
			n = 1;
		else if (val < 0x8000LL)
			n = 2;
		else if (val < 0x800000LL)
			n = 3;
		else if (val < 0x80000000LL)
			n = 4;
		else if (val < 0x8000000000LL)
			n = 5;
		else if (val < 0x800000000000LL)
			n = 6;
		else if (val < 0x80000000000000LL)
			n = 7;
		else
			n = 8;
	} else {
		if (val >= -0x80LL)
			n = 1;
		else if (val >= -0x8000LL)
			n = 2;
		else if (val >= -0x800000LL)
			n = 3;
		else if (val >= -0x80000000LL)
			n = 4;
		else if (val >= -0x8000000000LL)
			n = 5;
		else if (val >= -0x800000000000LL)
			n = 6;
		else if (val >= -0x80000000000000LL)
			n = 7;
		else
			n = 8;
	}
	return n;
}

static int dtc_encode_intval(struct msg *msg, struct mbuf *mbuf, uint8_t id,
		int64_t val) {
	char *t;
	uint8_t n;
	int i, count = 0;
	char tempbuf[12];
	char *pos = tempbuf;

	if (id >= 0) {
		*pos = id;
		pos += sizeof(id);
	}
	n = dtc_int_len_calc(val);
	*pos = n;
	pos += sizeof(n);
	t = (char *) &val;
#if __BYTE_ORDER == __BIG_ENDIAN
	for(int i=0; i<n; i++)
	{
		*pos=t[7-n+i];
		++pos;
	}
#else
	for (i = 0; i < n; i++) {
		*pos = t[n - 1 - i];
		++pos;
	}
#endif
	count = pos - tempbuf;
	uint8_t *wp = (uint8_t *) tempbuf;
	log_debug("count:%d", count);
	mbuf = msg_insert_mem_bulk(msg, mbuf, wp, count);
	if (mbuf == NULL) {
		return -1;
	}
	return count;
}

/*
 * encode a string length to mbuf
 */
static void dtc_encode_length(char **p, uint32_t len) {
	if (len < 240) {
		*(*p) = (uint8_t) len;
		*p += sizeof(uint8_t);
	} else if (len < (13 << 8)) {
		*(*p) = (uint8_t) (0xF0 + (len >> 8));
		*p += sizeof(uint8_t);
		*(*p) = (len & 0xFF);
		*p += sizeof(uint8_t);
	} else if (len < (1 << 16)) {
		*(*p) = (uint8_t) 253;
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len >> 8);
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len & 0xFF);
		*p += sizeof(uint8_t);
	} else if (len < (1 << 24)) {
		*(*p) = (uint8_t) 254;
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len >> 16);
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len >> 8);
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len & 0xFF);
		*p += sizeof(uint8_t);
	} else {
		*(*p) = (uint8_t) 255;
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len >> 24);
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len >> 16);
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len >> 8);
		*p += sizeof(uint8_t);
		*(*p) = (uint8_t) (len & 0xFF);
		*p += sizeof(uint8_t);
	}
	return;
}

static int dtc_encode_strval(struct msg *msg, struct mbuf *mbuf, uint8_t id,
		char *val, int len) {
	int count = 0;
	char tempbuf[10];
	char *pos = tempbuf;

	if (id >= 0) {
		*pos = id;
		pos += sizeof(id);
	}
	dtc_encode_length(&pos, len + 1);
	count = pos - tempbuf;
	uint8_t *wp = (uint8_t *) tempbuf;
	mbuf = msg_insert_mem_bulk(msg, mbuf, wp, count);
	if (mbuf == NULL) {
		return -1;
	}
	mbuf = msg_insert_mem_bulk(msg, mbuf, (uint8_t *) val, len);
	if (mbuf == NULL) {
		return -1;
	}
	tempbuf[0] = '\0';
	wp = (uint8_t *) tempbuf;
	mbuf = msg_insert_mem_bulk(msg, mbuf, wp, 1);
	if (mbuf == NULL) {
		log_error("append key value to msg error");
		return -1;
	}
	count += len + 1;
	return count;
}

static int dtc_encode_len(struct msg *msg, struct mbuf *mbuf, uint32_t len) {
	int count = 0;
	char tempbuf[10];
	char *pos = tempbuf;

	dtc_encode_length(&pos, len);
	count = pos - tempbuf;
	uint8_t *wp = (uint8_t *) tempbuf;
	mbuf = msg_insert_mem_bulk(msg, mbuf, wp, count);
	if (mbuf == NULL) {
		return -1;
	}

	return count;
}

static int dtc_encode_key_single(struct msg *msg, struct keypos *key,
		enum fieldtype keytype) {
	ASSERT(keytype != Float && keytype != None);

	uint8_t n;
	int64_t s64;
	int count = 0, i;
	char tempbuf[10];
	char *pos = tempbuf;
	char *t;
	struct mbuf *mbuf;

	*pos = (uint8_t) 0;
	pos += sizeof(uint8_t);
	mbuf = STAILQ_LAST(&msg->buf_q, mbuf, next);

	switch (keytype) {
	case Signed:
	case Unsigned: {
		if (key->end - key->start != sizeof(uint64_t)) {
			log_error(
					"key type is Signed or Unsigned,but key length is not %lu",
					sizeof(uint64_t));
			return -1;
		}
		s64 = *(int64_t *) key->start;
		n = dtc_int_len_calc(s64);
		*pos = n;
		pos += sizeof(n);

		t = (char *) key->start;
#if __BYTE_ORDER == __BIG_ENDIAN
		for(int i=0; i<n; i++)
		{
			*pos=t[7-n+i];
			++pos;
		}
#else
		for (i = 0; i < n; i++) {
			*pos = t[n - 1 - i];
			++pos;
		}
#endif
		count = pos - tempbuf;
		uint8_t *wp = (uint8_t *) tempbuf;
		mbuf = msg_insert_mem_bulk(msg, mbuf, wp, count);
		if (mbuf == NULL) {
			log_error("msg_insert_mem_bulk error");
			return -1;
		}
		break;
	}
	case String:
	case Binary: {
		dtc_encode_length(&pos, key->end - key->start + 1 - sizeof(uint32_t));
		count = pos - tempbuf;
		uint8_t *wp = (uint8_t *) tempbuf;
		mbuf = msg_insert_mem_bulk(msg, mbuf, wp, count);
		if (mbuf == NULL) {
			return -1;
		}
		mbuf = msg_insert_mem_bulk(msg, mbuf, key->start + sizeof(uint32_t),
				key->end - key->start - sizeof(uint32_t));
		if (mbuf == NULL) {
			return -1;
		}
		tempbuf[0] = '\0';
		wp = (uint8_t *) tempbuf;
		mbuf = msg_insert_mem_bulk(msg, mbuf, wp, 1);
		if (mbuf == NULL) {
			log_error("append key value to msg error");
			return -1;
		}
		count += key->end - key->start + 1 - sizeof(uint32_t);
		break;
	}
	default:
		return -1;
	}
	return count;
}

static int dtc_encode_key_multi(struct msg *msg, struct keypos *key, int keynum) {
	int i, count = 0, totalkeylen;
	int ret = 0;
	struct mbuf *buf;
	char tempbuf[10];
	char *pos = tempbuf;

	uint8_t id = 0;
	*pos = id;
	pos += sizeof(id);
	for (i = 0; i < keynum; i++) {
		totalkeylen += key[i].end - key[i].start;
	}
	dtc_encode_length(&pos, totalkeylen + 1);
	buf = STAILQ_LAST(&msg->buf_q, mbuf, next);
	count = pos - tempbuf;
	uint8_t *wp = (uint8_t *) tempbuf;
	buf = msg_insert_mem_bulk(msg, buf, wp, count);
	if (buf == NULL) {
		log_error("msg_insert_mem_bulk error");
		ret = -1;
		goto exit0;
	}
	for (i = 0; i < keynum; i++) {
		buf = msg_insert_mem_bulk(msg, buf, key[i].start,
				key[i].end - key[i].start);
		if (buf == NULL) {
			log_error("append key value to msg error");
			ret = -2;
			goto exit0;
		}
	}
	count += totalkeylen + 1;
	tempbuf[0] = '\0';
	wp = (uint8_t *) tempbuf;
	buf = msg_insert_mem_bulk(msg, buf, wp, 1);
	if (buf == NULL) {
		log_error("append key value to msg error");
		ret = -3;
		goto exit0;
	}
	ret = count;
	log_debug("count:%d", count);
	exit0: return ret;
}

static int dtc_encode_agentid(struct msg *r) {
	int status;
	struct mbuf *fbuf, *buf;

	fbuf = STAILQ_FIRST(&r->buf_q);
	buf = _mbuf_split(fbuf, fbuf->pos + HEADER_SIZE, NULL, NULL);
	if (buf == NULL) {
		log_debug("single key msg id:%"PRIu64" split error!", r->id);
		return -1;
	}
	STAILQ_INSERT_HEAD(&r->buf_q, buf, next);

	status = dtc_encode_intval(r, buf, 17, r->id);
	if (status < 0) {
		log_error("encode agent id into msg :%"PRIu64"error", r->id);
		return -1;
	}

	struct CPacketHeader *pheader;
	pheader = (struct CPacketHeader *) buf->pos;
	pheader->len[0] += status;
	return 0;
}

/*
 * get fragmentation function, temporarily not considering union components
 */
static int dtc_fragment_get(struct msg *r, uint32_t ncontinuum,
		struct msg_tqh *frag_msgq) {

	ASSERT(r->keyCount > 1);

	int i, j, ret, status;
	struct msg *sub_msg, *tmsg;
	struct mbuf *sbuf, *ebuf;
	CValue val;
	uint32_t idx = 0;
	int version_len = 0, requestinfo_len = 0;
	//Used to store all keys for grouping
	struct keypos keys[ncontinuum][r->keyCount];
	int keynum[ncontinuum];

	//memset(keynum, 0, sizeof(keynum));
	for (i = 0; i < ncontinuum; i++) {
		keynum[i] = 0;
	}

	for (i = 0; i < r->keyCount; i++) {
		struct keypos *kpos = &r->keys[i];
		if(r->keytype == Unsigned || r->keytype == Signed) {
			if(sizeof(uint64_t) == kpos->end - kpos->start) {
				//Endianness is not considered here due to SDK encoding
				val.u64 = *(uint64_t*)kpos->start;
				idx = msg_backend_idx(r, (uint8_t *)&val.u64, sizeof(uint64_t));
				log_debug("key is %lu, idx is %u, len is %ld",
					val.u64, idx, kpos->end - kpos->start);
			}
			else
				return -1;
		}
		else {
			//When multiple keys, each key is appended with 4-byte key length
			int len = kpos->end - kpos->start - sizeof(uint32_t);
			if(len > 0) {
				char temp[len + 2];
				*temp = len;
				for(j = 1; j < len + 1; j++) {
					temp[j] = lower((char)(kpos->start)[sizeof(uint32_t) + j - 1]);
				}
				temp[len+1] = 0;
				idx = msg_backend_idx(r, (uint8_t *)temp, len + 1);
				log_debug("debug,len :%d the packet key is %u '%s' the hash key :%d ",
					len, (uint32_t)(*temp), temp + 1, idx);
			}
			else
				return -1;
		}
		log_debug("idx:%d", idx);
		keys[idx][keynum[idx]] = *kpos;
		keynum[idx]++;
	}

	//no fragment happend
	if (keynum[idx] == r->keyCount) {
		//all fragment are in one idx
		r->idx = idx;
		log_debug("all key are in one server,idx:%d,keynum[idx]:%d", idx,
				keynum[idx]);
		ret = dtc_encode_agentid(r);
		return ret;
	}

	r->frag_id = msg_gen_frag_id();
	r->nfrag = 0;
	r->frag_owner = r;

	for (i = 0; i < ncontinuum; i++) {
		if (keynum[i] == 0) {
			continue;
		}
		log_debug("keynum[%d]:%d", i, keynum[i]);
		sub_msg = msg_get(r->owner, r->request);
		if (sub_msg == NULL) {
			goto frag_get_error;
		}
		log_debug("the data cp from source msg keycountstartlen:%"PRIu64"",
				r->keycountstartlen);
		sbuf = STAILQ_FIRST(&r->buf_q);
		ret = msg_append_buf(sub_msg, sbuf, sbuf->pos, r->keycountstartlen);
		if (ret < 0) {
			log_error(
					"cp buf from source msg to submsg error,source msg id:%"PRIu64"submsg id:%"PRIu64"",
					r->id, sub_msg->id);
			req_put(sub_msg);
			goto frag_get_error;
		}

		//encode keycount
		ebuf = STAILQ_LAST(&sub_msg->buf_q, mbuf, next);
		ret = dtc_encode_intval(sub_msg, ebuf, 11, keynum[i]);
		if (ret < 0) {
			req_put(sub_msg);
			goto frag_get_error;
		}
		version_len += ret - r->keycountlen;

		//encode agentid
		ebuf = STAILQ_LAST(&sub_msg->buf_q, mbuf, next);
		ret = dtc_encode_intval(sub_msg, ebuf, 17, sub_msg->id);
		if (ret < 0) {
			req_put(sub_msg);
			goto frag_get_error;
		}
		version_len += ret;
		log_debug(
				"size:%"PRIu64"keycountstartlen:%"PRIu64" keycountlen:%"PRIu64" ",
				HEADER_SIZE+r->seclen[0], r->keycountstartlen, r->keycountlen);
		ret = msg_append_buf(sub_msg, r->keycountendbuf, r->keycountendpos,
				HEADER_SIZE + r->seclen[0] + r->seclen[1] - r->keycountstartlen
						- r->keycountlen);
		if (ret < 0) {
			req_put(sub_msg);
			log_error(
					"cp buf from source msg to submsg error,source msg id:%"PRIu64"submsg id :%"PRIu64"",
					r->id, sub_msg->id);
			goto frag_get_error;
		}
		if (keynum[i] > 1) {
			log_debug("multi key");
			//multi key
			ret = dtc_encode_key_multi(sub_msg, keys[i], keynum[i]);
			if (ret < 0) {
				req_put(sub_msg);
				log_error(
						"ret:%d,msg id:%"PRIu64"submsg id :%"PRIu64" encode key multi error",
						ret, r->id, sub_msg->id);
				goto frag_get_error;
			}
			requestinfo_len += ret - r->keylen;
		} else {
			//single key
			log_debug("single key");
			ret = dtc_encode_key_single(sub_msg, &keys[i][0], r->keytype);
			if (ret < 0) {
				req_put(sub_msg);
				log_error(
						"ret:%d,msg id:%"PRIu64"submsg id :%"PRIu64" encode key single error",
						ret, r->id, sub_msg->id);
				goto frag_get_error;
			}
			requestinfo_len += ret - r->keylen;
		}

		ret = msg_append_buf(sub_msg, r->keyendbuf, r->keyendpos,
				r->seclen[7] + r->seclen[6] + r->seclen[5] + r->seclen[4]
						+ r->seclen[3] + r->seclen[2] - r->keylen);
		if (ret < 0) {
			req_put(sub_msg);
			log_error(
					"cp buf from source msg to submsg error,source msg id:%"PRIu64"submsg id :%"PRIu64"",
					r->id, sub_msg->id);
			goto frag_get_error;
		}
		sub_msg->keyCount = keynum[i];
		sub_msg->frag_id = r->frag_id;
		sub_msg->frag_owner = r;
		sub_msg->cmd = r->cmd;
		sub_msg->idx = i;

		sbuf = STAILQ_FIRST(&sub_msg->buf_q);
		struct CPacketHeader *pheader;
		pheader = (struct CPacketHeader *) sbuf->pos;
		pheader->len[0] += version_len;
		pheader->len[2] += requestinfo_len;
		//set the multikey tag
		if (sub_msg->keyCount <= 1) {
			pheader->flags &= ~MultiKeyValue;
		}
		TAILQ_INSERT_TAIL(frag_msgq, sub_msg, o_tqe);
		r->frag_seq[r->nfrag] = sub_msg;
		r->nfrag++;
		version_len = 0;
		requestinfo_len = 0;
	}

	return 0;

	frag_get_error:
	/*
	 *free msg in sub_msgq
	 */
	if (!TAILQ_EMPTY(frag_msgq)) {
		for (sub_msg = TAILQ_FIRST(frag_msgq); sub_msg != NULL; sub_msg =
				tmsg) {
			tmsg = TAILQ_NEXT(sub_msg, o_tqe);

			TAILQ_REMOVE(frag_msgq, sub_msg, o_tqe);
			msg_put(sub_msg);
		}
	}

	for (i = 0; i < r->nfrag; i++) {
		r->frag_seq[i] = NULL;
	}
	r->err = MSG_FRAGMENT_ERR;
	r->error = 1;
	return -1;
}

static uint64_t randomHashSeed = 1;

#if defined DA_COMPATIBLE_MODE && DA_COMPATIBLE_MODE == 1
int dtc_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq) {
	int status,i;
	struct keypos *temp_kpos;
	CValue val;

	if(r->cmd == MSG_NOP)
	{
		uint64_t randomkey=randomHashSeed++;
		r->idx = msg_backend_idx(r, (uint8_t *)&randomkey,sizeof(uint64_t));
		return 0;
	}
	else
	{
		if(r->keyCount == 0)
		{
			log_error(" request msg id: %"PRIu64" request without key",r->id);
			r->err= MSG_NOKEY_ERR;
			r->error = 1;
			return -1;
		}
		else if(r->keyCount == 1)
		{
			temp_kpos = &r->keys[0];
			switch (r->keytype)
			{
				case Signed:
				case Unsigned:
				{
					status=dtc_decode_value(Unsigned,temp_kpos->end - temp_kpos->start, temp_kpos->start,&val);
					if(status < 0)
					{
						return -1;
					}
					r->idx = msg_backend_idx(r,(uint8_t *)&val.u64, sizeof(uint64_t));
					break;
				}
				case String:
				{
					int len = temp_kpos->end - temp_kpos->start;
					char temp[len+1];
					*temp = len;
					for(i=1; i<len+1; i++)
					{
						temp[i] = lower((char)(temp_kpos->start)[i-1]);
					}
					r->idx = msg_backend_idx(r, (uint8_t *)temp,len+1);
					log_debug("debug,len :%d the packet key is %u   '%s' the hash key :%d ",len,*temp,temp+1,r->idx);
					break;
				}
				case Binary:
				{
					int len = temp_kpos->end - temp_kpos->start;
					char temp[len+1];
					*temp = len;
					memcpy(temp+1, temp_kpos->start,len);
					r->idx = msg_backend_idx(r, (uint8_t *)temp,len+1);
					log_debug("debug,len :%d the packet key is %u   '%s' the hash key :%d ",len,*temp,temp+1,r->idx);
					break;
				}
			}
			//Special place for cascade grayscale version
			status = dtc_encode_agentid(r);
			return status;
		}
		else
		{
			if(r->cmd == MSG_REQ_GET) {
				status = dtc_fragment_get(r, ncontinuum, frag_msgq);
			}
			else if(r->cmd == MSG_REQ_UPDATE) {
				//MSET is not supported
				status = -1;
			}
			else if(r->cmd == MSG_REQ_DELETE) {
				//MDEL is not supported
				status = -1;
			} else {
				//other multi operation is not supported
				status = -1;
			}
			return status;
		}
	}
}
#else
int dtc_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq) {

	int status;
	struct keypos *kpos;

	//ping msg
	if (r->cmd == MSG_NOP) {
		uint64_t randomkey = randomHashSeed++;
		r->idx = msg_backend_idx(r, (uint8_t *) &randomkey, sizeof(uint64_t));
		status = dtc_encode_agentid(r);
		return status;
	} else {
		if (r->keyCount == 0) {
			log_error(" request msg id: %"PRIu64" request without key", r->id);
			r->err = MSG_NOKEY_ERR;
			r->error = 1;
			return -1;
		} else if (r->keyCount == 1) {
			kpos = &r->keys[0];
			r->idx = msg_backend_idx(r, kpos->start, kpos->end - kpos->start);
			log_debug("key '%.*s' single request msg id: %"PRIu64" idx:%d",
					(int )(kpos->end - kpos->start), kpos->start, r->id,
					r->idx);
			status = dtc_encode_agentid(r);
			return status;

		} else {
			if (r->cmd == MSG_REQ_GET) {
				status = dtc_fragment_get(r, ncontinuum, frag_msgq);
			} else if (r->cmd == MSG_REQ_UPDATE) {
				//MSET is not supported
				status = -1;
			} else if (r->cmd == MSG_REQ_DELETE) {
				//MDEL is not supported
				status = -1;
			} else {
				//other multi operation is not supported
				status = -1;
			}
			return status;
		}
	}
}
#endif

static int dtc_coalesce_set(struct msg *r) {
	log_error("mset is not supported now");
	return -1;
}

static int dtc_coalesce_del(struct msg *r) {
	log_error("mdel is not supported now");
	return -1;
}

/*
 * affectrow and allrow is not used
 */
static int dtc_coalesce_get(struct msg *r) {

	struct msg *cmsg, *nmsg, *peermsg, *singleresmsg; /* current message */
	struct mbuf *buf, *cbuf, *nbuf, *sbuf;
	int32_t newlen, newresultinfolen = 0, newresultsetlen = 0;
	struct conn *c_conn;
	struct context *ctx;
	uint64_t id, totaltotalrows = 0, totalnumrows = 0, validmsgcount = 0;
	uint32_t nfragment = 0, errortag = 0;

	c_conn = r->owner;
	ctx = conn_to_ctx(c_conn);
	id = r->frag_id;

	singleresmsg = TAILQ_NEXT(r, c_i_tqe);

	for (cmsg = TAILQ_NEXT(r, c_i_tqe); cmsg != NULL && cmsg->frag_id == id;
			cmsg = nmsg) {
		nmsg = TAILQ_NEXT(cmsg, c_i_tqe);

		peermsg = cmsg->peer;

		if (peermsg->cmd == MSG_RSP_RESULTCODE && peermsg->resultcode != 0) {
			errortag = 1;
			break;
		} else {
			if (peermsg->numrows > 0) {
				log_debug("cmsg frag_id:%"PRIu64"numrow:%"PRIu64"",
						cmsg->frag_id, peermsg->numrows);
				totaltotalrows += peermsg->totalrows;
				totalnumrows += peermsg->numrows;
				++validmsgcount;
				singleresmsg = cmsg;
			}
		}
	}
	if (errortag == 1) { //There is a fragmented search error, one of the search result is in error
		cmsg->peer = NULL;
		cmsg->peerid = 0;
		peermsg->peer = NULL;
		peermsg->peerid = 0;
		r->peer = peermsg;
		r->peerid = peermsg->id;
		peermsg->peer = r;
		peermsg->id = r->id;
		goto coalesce_succ_err;
	} else if (validmsgcount == 0 || validmsgcount == 1) { //All results are in one package, only a package contain search resule
		peermsg = singleresmsg->peer;
		singleresmsg->peer = NULL;
		singleresmsg->peerid = 0;
		peermsg->peer = NULL;
		peermsg->peerid = 0;
		r->peer = peermsg;
		r->peerid = peermsg->id;
		peermsg->peer = r;
		peermsg->id = r->id;
		goto coalesce_succ_err;
	} else {
		//coalesce all sub msg
		cmsg = TAILQ_NEXT(r, c_i_tqe);
		//stolen a peer msg from first sub_msg
		r->peer = cmsg->peer;
		r->peer->peer = r;
		r->peerid = cmsg->peer->id;
		r->peer->peerid = r->id;
		cmsg->peer = NULL;
		cmsg->peerid = 0;

		buf = mbuf_split(r->peer->keycountendbuf, r->peer->keycountendpos, NULL,
		NULL);
		if (buf == NULL) {
			log_error("request msg:%"PRIu64" split msg error", r->id);
			return -1;
		}
		buf->pos += r->peer->keycountlen;
		r->peer->mlen -= r->keycountlen;
		STAILQ_INSERT_AFTER(&r->peer->buf_q, r->peer->keycountendbuf, buf,
				next);
		newlen = dtc_encode_len(r->peer, r->peer->keycountendbuf, totalnumrows);
		if (newlen < 0) {
			log_error("msg id:%"PRIu64"set numrows of coalesce msg error",
					r->id);
			goto coalesce_err;
		}
		newresultsetlen += newlen - r->peer->keycountlen;
		//split a msg
		buf = mbuf_split(r->peer->keyendbuf, r->peer->keyendpos, NULL, NULL);
		if (buf == NULL) {
			log_error("split rsp msg fail!");
			return -1;
		}
		buf->pos += r->peer->keylen;
		r->peer->mlen -= r->peer->keylen;
		STAILQ_INSERT_AFTER(&r->peer->buf_q, r->peer->keyendbuf, buf, next);

		//set total row of all msg
		newlen = dtc_encode_intval(r->peer, r->peer->keyendbuf, 5,
				totaltotalrows);
		if (newlen < 0) {
			log_error("msg id:%"PRIu64"set total row of coalesce msg error",
					r->id);
			goto coalesce_err;
		}
		newresultinfolen = newlen - r->peer->keylen;

		//stole other msg's buf
		for (nmsg = TAILQ_NEXT(cmsg, c_i_tqe);
				nmsg != NULL && nmsg->frag_id == id; nmsg = cmsg) {
			cmsg = TAILQ_NEXT(nmsg, c_i_tqe);
			nfragment++;
			peermsg = nmsg->peer;
			if (peermsg->numrows == 0) {
				continue;
			}

			cbuf = peermsg->setsplitbuf;
			cbuf->pos = peermsg->setsplitpos;
			for (; cbuf != NULL; cbuf = nbuf) {
				nbuf = STAILQ_NEXT(cbuf, next);

				//stolen buf from sub rsp msg
				STAILQ_REMOVE(&peermsg->buf_q, cbuf, mbuf, next);
				STAILQ_INSERT_TAIL(&r->peer->buf_q, cbuf, next);
				newresultsetlen += mbuf_length(cbuf);
				r->peer->mlen += mbuf_length(cbuf);
			}
		}
		log_debug("nragment:%"PRIu32"real frag:%"PRIu32"",nfragment,r->nfrag);
		ASSERT(nfragment==r->nfrag);
	}

	for (cmsg = TAILQ_NEXT(r, c_i_tqe); cmsg != NULL && cmsg->frag_id == id;
			cmsg = nmsg) {
		nmsg = TAILQ_NEXT(cmsg, c_i_tqe);
		c_conn->dequeue_inq(ctx, c_conn, cmsg);
		req_put(cmsg);
	}
	//set the peer msg header
	sbuf = STAILQ_FIRST(&r->peer->buf_q);
	struct CPacketHeader *pheader;
	pheader = (struct CPacketHeader *) sbuf->pos;
	pheader->len[3] += newresultinfolen;
	pheader->len[7] += newresultsetlen;
	pheader->flags &= MultiKeyValue;
	return 0;
	coalesce_succ_err:
	for (cmsg = TAILQ_NEXT(r, c_i_tqe);
			cmsg != NULL && cmsg->frag_id == id; cmsg = nmsg) {
		nmsg = TAILQ_NEXT(cmsg, c_i_tqe);
		c_conn->dequeue_inq(ctx, c_conn, cmsg);
		req_put(cmsg);
	}
	return 0;
	coalesce_err:
	for (cmsg = TAILQ_NEXT(r, c_i_tqe);
			cmsg != NULL && cmsg->frag_id == id; cmsg = nmsg) {
		nmsg = TAILQ_NEXT(cmsg, c_i_tqe);
		c_conn->dequeue_inq(ctx, c_conn, cmsg);
		req_put(cmsg);
	}
	r->err = MSG_COALESCE_ERR;
	r->error = 1; 
	return -1;
}

static int dtc_encode_client_reconn(struct msg *r) {
	int status;
	struct mbuf *fbuf, *buf;

	fbuf = STAILQ_FIRST(&r->buf_q);
	buf = _mbuf_split(fbuf, fbuf->pos + HEADER_SIZE, NULL, NULL);
	if (buf == NULL) {
		log_debug("msg id:%"PRIu64" split error!", r->id);
		return -1;
	}
	STAILQ_INSERT_HEAD(&r->buf_q, buf, next);

	status = dtc_encode_intval(r, buf, 19, 1);
	if (status < 0) {
		log_error("encode reconnect flag into msg :%"PRIu64"error", r->id);
		return -1;
	}

	struct CPacketHeader *pheader;
	pheader = (struct CPacketHeader *) buf->pos;
	pheader->len[0] += status;
	return 0;
}

int dtc_coalesce(struct msg *r) {

	ASSERT(r->request == 1);
	ASSERT(r->done == 1);
	ASSERT(r->swallow == 0);

	int status;
	if (r->frag_id == 0) {
		status = 0;
	} else {
		if (r->cmd == MSG_REQ_GET) {
			status = dtc_coalesce_get(r);
		} else if (r->cmd == MSG_REQ_UPDATE) {
			status = dtc_coalesce_set(r);
		} else if (r->cmd == MSG_REQ_DELETE) {
			status = dtc_coalesce_del(r);
		} else {
			status = -1;
		}
	}
	if (status == 0 && core_getinst_status() == EXITING) {
		log_debug("encode reconnect flag to rsp");
		dtc_encode_client_reconn(r->peer);
	}
	return status;
}

int dtc_error_reply(struct msg *smsg, struct msg *dmsg) {
	int ret, resultlen = 0, versionlen = 0;
	struct mbuf *start_buf, *end_buf;

	start_buf = mbuf_get();
	start_buf->last += HEADER_SIZE;
	dmsg->mlen += HEADER_SIZE;
	mbuf_insert(&dmsg->buf_q, start_buf);
	ret = dtc_encode_intval(dmsg, start_buf, 3, smsg->serialnr);
	if (ret < 0) {
		return -1;
	}
	versionlen += ret;
	if (core_getinst_status() == EXITING) {
		log_debug("encode reconnect flag to error rsp");
		ret = dtc_encode_intval(dmsg, start_buf, 19, 1);
		if (ret < 0) {
			return -1;
		}
		versionlen += ret;
	}
	end_buf = STAILQ_LAST(&dmsg->buf_q, mbuf, next);
	ret = dtc_encode_intval(dmsg, end_buf, 1, smsg->err);
	if (ret < 0) {
		return -1;
	}
	resultlen += ret;
	end_buf = STAILQ_LAST(&dmsg->buf_q, mbuf, next);
	char *errmsg = "dtcagent";
	ret = dtc_encode_strval(dmsg, end_buf, 2, errmsg, da_strlen(errmsg));
	if (ret < 0) {
		return -1;
	}
	resultlen += ret;
	end_buf = STAILQ_LAST(&dmsg->buf_q, mbuf, next);
	errmsg = GetMsgErrorCodeStr(smsg->err);
	ret = dtc_encode_strval(dmsg, end_buf, 3, errmsg, da_strlen(errmsg));
	if (ret < 0) {
		return -1;
	}
	resultlen += ret;
	end_buf = STAILQ_LAST(&dmsg->buf_q, mbuf, next);
	ret = dtc_encode_intval(dmsg, end_buf, 5, dmsg->totalrows);
	if (ret < 0) {
		return -1;
	}
	resultlen += ret;

	end_buf = STAILQ_LAST(&dmsg->buf_q, mbuf, next);
	ret = dtc_encode_intval(dmsg, end_buf, 8, now_ms);
	if (ret < 0) {
		return -1;
	}
	resultlen += ret;

	start_buf = STAILQ_FIRST(&dmsg->buf_q);
	struct CPacketHeader *pheader;
	pheader = (struct CPacketHeader *) start_buf->pos;
	pheader->version = 1;
	pheader->scts = 8;
	pheader->cmd = dmsg->cmd;
	pheader->len[0] = versionlen;
	pheader->len[1] = 0;
	pheader->len[2] = 0;
	pheader->len[3] = resultlen;
	pheader->len[4] = 0;
	pheader->len[5] = 0;
	pheader->len[6] = 0;
	pheader->len[7] = 0;

	log_debug("dmsg len:%d", dmsg->mlen);
	return 0;
}
