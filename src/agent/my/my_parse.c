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
#include "../da_server.h"
#include "../da_time.h"
#include "../da_core.h"
#include "my_comm.h"
#include "my_command.h"

#define MYSQL_HEADER_SIZE 4
#define MAXPACKETSIZE (64 << 20)
#define MultiKeyValue 32
#define __FLTFMT__ "%LA"
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
	ST_ID = 0,
	ST_LENTH = 1,
	ST_VALUE = 2,
};

/*
 * parse request msg
 */
void my_parse_req(struct msg *r)
{
	struct mbuf *b;
	uint8_t *p;
	uint32_t input_packet_length = 0;
	enum enum_server_command command;
	int rc;

	log_debug("my_parse_req entry.");

	b = STAILQ_LAST(&r->buf_q, mbuf, next);
	p = r->pos;

	if (p < b->last) {
		if (b->last - p < MYSQL_HEADER_SIZE) {
			log_error(
				"receive size small than package header. id:%d",
				r->id);
			p = b->last;
			goto end;
		}

		input_packet_length = uint_trans_3(p);
		log_debug("uint_trans_3:0x%x 0x%x 0x%x, len:%d", p[0], p[1], p[2],
			  input_packet_length);
		p += 3;
		r->pkt_nr = (uint8_t)(*p); // mysql sequence id
		p++;
		log_debug("pkt_nr:%d, packet len:%d", r->pkt_nr,
			  input_packet_length);

		if (p + input_packet_length > b->last) {
			p = b->last;
			goto end;
		}
log_debug("1111111111");
		if (r->owner->stage == CONN_STAGE_LOGGED_IN) {
			log_debug("1111111111");
			rc = my_get_command(p, input_packet_length, r,
					    &command);
			if (rc) {
				if (rc < 0) {
					if(rc == -6)
					{
						r->parse_res = MSG_PARSE_ERROR_NO_SELECTED_DB;
						goto custom;
					}

					log_error("parse command error:%d\n",
						  rc);
					goto error;
				}

				r->command = command;
				r->keyCount = 1;
				r->cmd = MSG_REQ_SVRADMIN;
			}
		}
		else if (r->owner->stage == CONN_STAGE_LOGGING_IN) 
		{
#if 0			
			//parse -D parameter(dbname)
			if(input_packet_length >= 34)
			{
				uint8_t* pp = p;
				uint8_t* dbstart = NULL;
				pp += 32;	//Client Cap + Extended Client Cap + Max Packet + Charset + Unused
				while(pp - p <= input_packet_length)	//Username
				{
					if(*pp == 0x0)
					{
						pp++;
						break;
					}
					else
						pp++;
				}

				if(*pp == 0x0) //Password
				{
					pp++;
				}
				else
				{
					pp += 20;
				}

				dbstart = pp;
				while(pp - dbstart <= input_packet_length)	//DB
				{
					if(*pp == 0x0)
					{
						break;
					}
					else
						pp++;
				}

				if(pp - dbstart > 0 && pp - dbstart < 250)
				{
					int len = pp - dbstart;
					int len_sha2 = strlen("caching_sha2_password");
					if(len != len_sha2 || 
					(len == len_sha2 && 
						(memcmp(dbstart, "caching_sha2_password", len_sha2) != 0 && memcmp(dbstart, "mysql_native_password", len_sha2) != 0)))
					{
						memcpy(r->owner->dbname, dbstart, len);
						r->owner->dbname[len] = '\0';
						log_debug("client set dbname: %s", r->owner->dbname);
					}
				}
			}
			else
			{
				log_error("parse login info error amid at packet length:%d\n", input_packet_length);
			}
#endif			
		}

		p += input_packet_length;

		goto success;
	}

end:
	ASSERT(p == b->last);
	r->pos = p;
	if (b->last == b->end) {
		r->parse_res = MSG_PARSE_REPAIR;
	} else {
		r->parse_res = MSG_PARSE_AGAIN;
	}
	return;

custom:
	r->pos = b->last;
	log_debug("parse msg:%" PRIu64 " error!", r->id);
	errno = EINVAL;
	log_debug("my_parse_req leave.");
	return ;

error:
	r->pos = b->last;
	r->parse_res = MSG_PARSE_ERROR;
	log_debug("parse msg:%" PRIu64 " error!", r->id);
	errno = EINVAL;
	log_debug("my_parse_req leave.");
	return;

success:
	r->pos = p;
	r->parse_res = MSG_PARSE_OK;
	log_debug("parse msg:%" PRIu64 " success!", r->id);
	log_debug("my_parse_req leave.");
	return;
}

void my_parse_rsp(struct msg *r)
{
	struct mbuf *b;
	uint8_t *p;
	int ret;

	log_debug("my_parse_rsp entry.");

	b = STAILQ_LAST(&r->buf_q, mbuf, next);
	p = r->pos;
	r->token = NULL;

	if (p < b->last) {
		if (b->last - p <
		    sizeof(struct DTC_HEADER_V2) + MYSQL_HEADER_SIZE) {
			log_error(
				"receive size small than package header. id:%d",
				r->id);
			p = b->last;
			r->parse_res = MSG_PARSE_ERROR;
			errno = EINVAL;
			return;
		}
		r->peerid = ((struct DTC_HEADER_V2 *)p)->id;
		r->admin = ((struct DTC_HEADER_V2 *)p)->admin;
		p = p + sizeof(struct DTC_HEADER_V2);

		r->pkt_nr = (uint8_t)(p[3]); // mysql sequence id
		log_debug("pkt_nr:%d, peerid:%d, id:%d, admin:%d", r->pkt_nr,
			  r->peerid, r->id, r->admin);

		p = p + MYSQL_HEADER_SIZE;

		p = b->last;
		r->pos = p;
		r->parse_res = MSG_PARSE_OK;
		log_debug("parse msg:%" PRIu64 " success!", r->id);
	} else {
		r->parse_res = MSG_PARSE_ERROR;
		log_debug("parse msg:%" PRIu64 " error!", r->id);
		errno = EINVAL;
	}

	log_debug("my_parse_rsp leave.");
	return;
}

int my_get_command(uint8_t *input_raw_packet, uint32_t input_packet_length,
		   struct msg *r, enum enum_server_command *cmd)
{
	*cmd = (enum enum_server_command)(uchar)input_raw_packet[0];
	log_debug("cmd: %d", *cmd);
	if (*cmd >= COM_END)
		*cmd = COM_END; // Wrong command

	if (parse_packet(input_raw_packet + 1, input_packet_length - 1, r,
			 *cmd))
		return 1;

	return -1;
}

int my_do_command(struct msg *msg)
{
	int rc = NEXT_RSP_ERROR;

	switch (msg->command) {
	case COM_INIT_DB:{
		rc = NEXT_RSP_OK;
		break;
	}
	case COM_REGISTER_SLAVE:
	case COM_RESET_CONNECTION:
	case COM_CLONE:
	case COM_CHANGE_USER: {
		rc = NEXT_RSP_OK;
		break;
	}
	case COM_STMT_EXECUTE:
	case COM_STMT_FETCH:
	case COM_STMT_SEND_LONG_DATA:
	case COM_STMT_PREPARE:
	case COM_STMT_CLOSE:
	case COM_STMT_RESET: {
		rc = NEXT_RSP_ERROR;
		break;
	}
	case COM_QUERY: {
		log_debug("COM_QUERY, admin: %d", msg->admin);
		if (msg->admin == CMD_SQL_PASS_OK)
			rc = NEXT_RSP_OK;
		else if (msg->admin == CMD_SQL_PASS_NULL)
			rc = NEXT_RSP_NULL;
		else
			rc = NEXT_FORWARD;
		break;
	}
	case COM_FIELD_LIST: // This isn't actually needed
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
	case COM_CONNECT: // Impossible here
	case COM_TIME: // Impossible from client
	case COM_DELAYED_INSERT: // INSERT DELAYED has been removed.
	case COM_END:
	default:
		log_error("error command:%d", msg->command);
		rc = NEXT_RSP_ERROR;
		break;
	}

	return rc;
}

static int dtc_decode_value(enum fieldtype type, int lenth, uint8_t *p,
			    CValue *val)
{
	uint8_t *q;
	switch (type) {
	case None:
		break;
	case Signed:
	case Unsigned:
		if (lenth == 0 || lenth > 8) {
			goto decode_value_error;
		}
		q = (uint8_t *)p + 1;
		int64_t s64;
		s64 = *(int8_t *)p;
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
		if (!strcmp((char *)p, "NAN"))
			val->flt = NAN;
		else if (!strcmp((char *)p, "INF"))
			val->flt = INFINITY;
		else if (!strcmp((char *)p, "-INF"))
			val->flt = -INFINITY;
		else {
			long double ldf;
			if (sscanf((char *)p, __FLTFMT__, &ldf) != 1)
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
			val->str.data = NULL;
			val->str.len = 0;
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
decode_value_error:
	return -1;
}

static uint64_t randomHashSeed = 1;

int my_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq)
{
	int status, i;
	struct keypos *temp_kpos;
	CValue val;
	log_debug("key count:%d, cmd:%d", r->keyCount, r->cmd);

	if (r->cmd == MSG_NOP || r->admin != CMD_NOP) {
		uint64_t randomkey = randomHashSeed++;
		r->idx = msg_backend_idx(r, (uint8_t *)&randomkey,
					 sizeof(uint64_t));
		return 0;
	}
	else if(r->layer == 2 || r->layer == 3) {
		r->idx = msg_backend_idx(r, NULL, 0);
		return 0;
	} else {
		if (r->keyCount == 0) {
			log_error(" request msg id: %" PRIu64
				  " request without key",
				  r->id);
			r->err = MSG_NOKEY_ERR;
			r->error = 1;
			return -1;
		} else if (r->keyCount == 1) {
			temp_kpos = &r->keys[0];
			switch (r->keytype) {
			case Signed:
			case Unsigned: {
				status = dtc_decode_value(
					Unsigned,
					temp_kpos->end - temp_kpos->start,
					temp_kpos->start, &val);
				if (status < 0) {
					log_error("decode value:%d", status);
					return -1;
				}
				log_debug("val.u64:%d", val.u64);
				r->idx = msg_backend_idx(r, (uint8_t *)&val.u64,
							 sizeof(uint64_t));
				log_debug("r->idx:%d", r->idx);
				break;
			}
			case String: {
				int len = temp_kpos->end - temp_kpos->start;
				char temp[len + 1];
				*temp = len;
				for (i = 1; i < len + 1; i++) {
					temp[i] = lower((
						char)(temp_kpos->start)[i - 1]);
				}
				r->idx = msg_backend_idx(r, (uint8_t *)temp,
							 len + 1);
				log_debug(
					"debug,len :%d the packet key is %u   '%s' the hash key(r->idx): %d ",
					len, *temp, temp + 1, r->idx);
				break;
			}
			case Binary: {
				int len = temp_kpos->end - temp_kpos->start;
				char temp[len + 1];
				*temp = len;
				memcpy(temp + 1, temp_kpos->start, len);
				r->idx = msg_backend_idx(r, (uint8_t *)temp,
							 len + 1);
				log_debug(
					"debug,len :%d the packet key is %u   '%s' the hash key(r->idx): %d ",
					len, *temp, temp + 1, r->idx);
				break;
			}
			}

			log_debug("return status:%d", status);
			return status;
		} else {
			if (r->cmd == MSG_REQ_GET) {
				//status = dtc_fragment_get(r, ncontinuum, frag_msgq);
				//GET is not supported
				status = -1;
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

int _check_condition(struct string *str)
{
	int i;
	char *condition = " WHERE ";
	char *insert_char = "INSERT INTO ";
	if (string_empty(str))
		return -1;

	for (i = 0; i < str->len; i++) {
		if (da_strncmp(str->data + i, condition,
			       da_strlen(condition)) == 0 &&
		    i + da_strlen(condition) < str->len)
			return i + da_strlen(condition);

		//if(da_strncmp(str->data+i, insert_char, da_strlen(insert_char)) == 0 && i + da_strlen(insert_char) < str->len)
		//	return i + da_strlen(insert_char);
	}

	return -2;
}

bool check_cmd_operation(struct string *str)
{
	int i = 0;
	char *condition_1 = "SELECT ";
	char *condition_2 = "DELETE FROM ";
	char *condition_3 = "UPDATE ";
	char *condition_4 = "INSERT INTO ";
	if (string_empty(str))
		return false;

	if (da_strncmp(str->data + i, condition_1, da_strlen(condition_1)) !=
		    0 &&
	    da_strncmp(str->data + i, condition_2, da_strlen(condition_2)) !=
		    0 &&
	    da_strncmp(str->data + i, condition_3, da_strlen(condition_3)) !=
		    0 &&
	    da_strncmp(str->data + i, condition_4, da_strlen(condition_4)) != 0)
		return true;
	else
		return false;
}

bool check_cmd_select(struct string *str)
{
	return false;
}

int get_mid_by_dbname(const char* dbname, const char* sql, struct msg* r)
{
	int mid = 0;
	struct context* ctx = NULL;
	struct conn *c_conn = NULL;
	int sql_len = 0;
	c_conn = r->owner;
	ctx = conn_to_ctx(c_conn);
	if(dbname && strlen(dbname) > 0)
	{
		char* cmp_dbname[250] = {0};
		sprintf(cmp_dbname, "%s.", dbname);
		struct array *pool = &(ctx->pool);
		int i;
		for (i = 0; i < array_n(pool); i++) {
			struct server_pool *p = (struct server_pool *)array_get(pool, i);
			if(string_empty(&p->name))
				continue;
			log_info("server pool module name: %s, cmp dbname: %s", p->name.data, cmp_dbname);
			if(da_strncmp(p->name.data, cmp_dbname, strlen(cmp_dbname)) == 0)
			{
				mid = p->mid;
			}
		}
	}

	if(sql)
	{
		sql_len = strlen(sql);
		if(sql_len > 0)
		{
			struct array *pool = &(ctx->pool);
			int i, j;
			for (i = 0; i < array_n(pool); i++) {
				struct string cmp_name; 
				struct server_pool *p = (struct server_pool *)array_get(pool, i);
				if(string_empty(&p->name))
					continue;
				
				string_copy(&cmp_name, p->name.data, p->name.len);
				string_upper(&cmp_name);
				for(j = 0; j < sql_len; j++)
				{
					if(sql_len - j > cmp_name.len && da_strncmp(sql + j, cmp_name.data, cmp_name.len) == 0)
					{
						mid = p->mid;
					}
				}
				log_info("server pool module name: %s, cmp sql: %s", cmp_name.data, sql);
			}
		}
		
	}

	log_info("mid result: %d", mid);
	return mid;
}

void get_tablename(struct msg* r, uint8_t* sql, int sql_len)
{
	char tablename[260] = {0};
	if(sql == NULL || sql_len <= 0)
		return ;

	log_debug("AAAAAAAAA 555555555555");
	int ret = sql_parse_table(sql, &tablename);
	if(ret > 0)
	{
		log_debug("AAAAAAAAA 666666666666");
		string_copy(&r->table_name, tablename, strlen(tablename));
	}
	log_debug("AAAAAAAAA 77777777777 %s", tablename);
}

int my_get_route_key(uint8_t *sql, int sql_len, int *start_offset,
		     int *end_offset, const char* dbname, struct msg* r)
{
	int i = 0;
	struct string str;
	int ret = 0;
	int layer = 0;
	string_init(&str);
	string_copy(&str, sql, sql_len);

	if (string_empty(&str))
		return -1;

	if (!string_upper(&str))
		return -9;

	log_debug("sql: %s", str.data);
	if(dbname && strlen(dbname))
	{
		log_debug("dbname len:%d, dbname: %s", strlen(dbname), dbname);
	}

	int mid = get_mid_by_dbname(dbname, str.data, r);
	char conf_path[260] = {0};
	if(mid != 0)
	{
		sprintf(conf_path, "../conf/dtc-conf-%d.yaml", mid);
		r->mid = mid;
	}

	get_tablename(r, str.data, str.len);
	if(r->table_name.len > 0)
		log_debug("table name: %s", r->table_name.data);

	char* res = NULL;
	char strkey[260] = {0};
	memset(strkey, 0, 260);
	if(strlen(conf_path) > 0)
	{
		res = rule_get_key(conf_path);
		if(res == NULL)
		{
			ret = -5;
			goto done;
		}
		else
		{
			strcpy(strkey, res);
			log_debug("strkey: %s", strkey);
		}
	}

	r->keytype = rule_get_key_type(conf_path);
	log_debug("strkey type: %d", r->keytype);

	//agent sql route, rule engine
	layer = rule_sql_match(str.data, dbname, strlen(conf_path) > 0 ? conf_path : NULL);
	log_debug("rule layer: %d", layer);

	if(layer != 1)
	{
		ret = layer;
		goto done;
	}

	if (check_cmd_operation(&str))
		return -2;

	if (check_cmd_select(&str))
		return -2;

	i = _check_condition(&str);
	if (i < 0) {
		ret = -2;
		log_error("check condition error code:%d", i);
		goto done;
	}

	for (; i < str.len; i++) {
		if (str.len - i >= strlen(strkey)) {
			log_debug(
				"key: %s, key len:%d, str.len:%d i:%d dtc_key_len:%d str.data + i:%s ", strkey, strlen(strkey),
				str.len, i, strlen(strkey), str.data + i);
			if (da_strncmp(str.data + i, strkey, strlen(strkey)) == 0) 
			{
				int j;
				for (j = i + strlen(strkey); j < str.len; j++) 
				{
					if (str.data[j] == '=') 
					{
						j++;
						//strip space.
						while (j < str.len && (str.data[j] == ' ' || str.data[j] == '\'' || str.data[j] == '\"'))
						{
							j++;
						}

						if (j < str.len) 
						{
							*start_offset = j;

							int k = 0;
							for (k = j; k < str.len;
							     k++) {
								if (sql[k + 1] == ' ' || sql[k + 1] == '\'' || sql[k + 1] == '\"' || sql[k + 1] == ';' || k + 1 == str.len) 
								{
									*end_offset = k + 1;
									ret = layer;
									goto done;
								}
							}

							ret = -4;
							goto done;
						} 
						else 
						{
							ret = -5;
							goto done;
						}
					}
				}

				ret = -2;
				goto done;
			}
		}
	}

	ret = -3;
	goto done;

done:
	string_deinit(&str);
	return ret;
}