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

#ifndef DA_MSG_H_
#define DA_MSG_H_

#include "compiler.h"
#include "da_array.h"
#include "da_buf.h"
#include "da_queue.h"
#include "da_rbtree.h"
#include "da_string.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include "my/my_com_data.h"
#include "my/my_command.h"
#include "my/my_comm.h"

struct msg;
struct conn;
struct msg_tqh;
struct context;

typedef void (*msg_parse_t)(struct msg *);
typedef int (*msg_fragment_t)(struct msg *, uint32_t, struct msg_tqh *);
typedef int (*msg_coalesce_t)(struct msg *r);

typedef enum msg_parse_result {
	MSG_PARSE_OK, /* parsing ok */
	MSG_PARSE_ERROR, /* parsing error */
	MSG_PARSE_REPAIR, /* more to parse -> repair parsed & unparsed data */
	MSG_PARSE_AGAIN, /* incomplete -> parse again */
	MSG_PARSE_ERROR_NO_SELECTED_DB,
} msg_parse_result_t;

typedef enum msg_my_error{
	MY_ERR_NO_DB_SELECTED = 1046,	//no database selected
	MY_ERR_UNKNOWN_DB = 1049, 	// Unknown database
} msg_my_error_t;

#define MSG_TYPE_CODEC(ACTION)                                                 \
	ACTION(NOP)                                                            \
	ACTION(RSP_RESULTCODE)                                                 \
	ACTION(RSP_RESULTSET)                                                  \
	ACTION(REQ_SVRADMIN)                                                   \
	ACTION(REQ_GET)                                                        \
	ACTION(REQ_PURGE)                                                      \
	ACTION(REQ_INSERT)                                                     \
	ACTION(REQ_UPDATE)                                                     \
	ACTION(REQ_DELETE)                                                     \
	ACTION(REQ_REPLACE)                                                    \
	ACTION(REQ_FLUSH)                                                      \
	ACTION(REQ_INVALIDATE)                                                 \
	ACTION(REQ_MONITOR)

#define DEFINE_ACTION(_name) MSG_##_name,
typedef enum msg_type { MSG_TYPE_CODEC(DEFINE_ACTION) } msg_type_t;
#undef DEFINE_ACTION

/*
 * start point and end point must within a mbuf
 */
struct keypos {
	uint8_t *start; /* key start pos */
	uint8_t *end; /* key end pos */
};

struct msg {
	TAILQ_ENTRY(msg) c_i_tqe; /*in client inmsg queue*/
	TAILQ_ENTRY(msg) c_o_tqe; /*in client omsg queue*/
	TAILQ_ENTRY(msg) s_i_tqe; /*in the server inmsg queue*/
	TAILQ_ENTRY(msg) o_tqe; /*in frag_msgq or send q*/

	uint64_t id; /* id for svr asyn operation*/
	int idx; /* index of server*/
	uint64_t serialnr; /* client serialnr*/
	msg_type_t cmd; /* msg type */
	struct string accesskey; /* accesskey for the msg*/
	uint64_t keytype; /* keytype of the primary key*/
	uint64_t keyCount; /* number of key count*/
	struct keypos keys[32]; /* array of keys */
	int64_t hitflag;
	struct conn *owner; /* message owner - client | server */
	struct conn *peer_conn; /* message peer(client | server) connection*/
	uint64_t peerid; /* id of msg peer*/
	struct msg *peer; /* msg peer*/

	int64_t affectrows; /* affect rows in result info*/
	uint64_t totalrows; /* total rows in result info*/
	uint64_t numrows; /* result rows*/
	uint32_t fieldnums; /* field nums*/
	int64_t resultcode; /* resultcode in result info*/

	struct msg *frag_owner; /* owner of fragment message */
	uint32_t nfrag; /* # fragment */
	uint32_t nfrag_done; /* # fragment done */
	uint64_t frag_id; /* id of fragmented message */
	struct msg
		*frag_seq[32]; /* sequence of fragment message, map from keys to
                               fragments*/

	struct buf_stqh buf_q; /* buff list in msg*/
	uint32_t mlen; /* message length */
	uint64_t start_ts; /* start timestamp in usec */
	struct rbnode tmo_rbe; /* entry in time rbtree */
	struct rbnode msg_rbe; /* entry in backwork rbtree*/

	msg_parse_t parser; /* msg parse */
	msg_parse_result_t parse_res; /* msg parse result*/

	msg_fragment_t fragment; /* message fragment */
	msg_coalesce_t coalesce; /* message post-coalesce */

	struct mbuf *keyendbuf; /* the buf that end key located*/
	uint8_t *keyendpos;
	struct mbuf *keycountendbuf; /* the buf that keycount end*/
	uint8_t *keycountendpos; /* end position in buff*/
	struct mbuf *setsplitbuf;
	uint8_t *setsplitpos;
	uint64_t keycountstartlen; /* the length until the start of keycount*/
	uint64_t keycountlen; /* keycount len*/
	uint64_t keylen; /* all key len include length and key value*/

	uint8_t flags; /* header flags*/
	uint32_t *seclen; /* header 8 section len in msg header*/
	uint8_t cur_parse_id; /* current parse id*/
	uint64_t cur_parse_type; /* current parse type*/
	int cur_parse_lenth; /* current parse lenth*/
	int state; /* current parse state*/
	int field; /* current parse field*/
	int sec_parsed_len;
	int parsed_len; /* len has been parsed*/
	int subkeylen; /* parsing subkey length*/
	int subkeycount; /* parsing subkey count */
	uint8_t *pos; /* parser position marker */
	uint8_t *token; /* token marker */

	uint8_t pkt_nr; /* mysql sequence id */
	enum enum_server_command command; /* mysql request command type */
	enum enum_agent_admin admin;
	uint8_t layer;
	int ismysql;
	union COM_DATA data;

	int err; /* errno on error? */
	unsigned error : 1; /* error? */
	unsigned ferror : 1; /* error? */
	unsigned request : 1; /* request? or response? */
	unsigned done : 1; /* done? */
	unsigned fdone : 1; /* all fragments are done? */
	unsigned swallow : 1; /* swallow response? */

	unsigned cli_inq : 1; /*msg in client in msgq*/
	unsigned cli_outq : 1; /*msg in client out msgq*/
	unsigned sev_inq : 1; /*msg in server in msgq*/
	unsigned sev_msgtree : 1; /*msg in server in msg tree*/
	unsigned sending : 1; /*msg is sending*/
};

TAILQ_HEAD(msg_tqh, msg);

void msg_tmo_delete(struct msg *msg);
void msg_tmo_insert(struct msg *msg, struct conn *conn);
struct msg *msg_tmo_min(void);
int msg_init();
struct msg *msg_get(struct conn *conn, bool request);
int msg_deinit();
void msg_put(struct msg *m);
struct msg *msg_get_error(struct msg *smsg);
void msg_dump(struct msg *msg, int level);
struct string *msg_type_string(msg_type_t type);
bool msg_empty(struct msg *msg);
int msg_recv(struct context *ctx, struct conn *conn);
int msg_send(struct context *ctx, struct conn *conn);
uint64_t msg_gen_frag_id(void);
int msg_append_buf(struct msg *msg, struct mbuf *sbuf, uint8_t *pos,
		   size_t len);
struct mbuf *msg_insert_mem_bulk(struct msg *msg, struct mbuf *mbuf,
				 uint8_t *pos, size_t len);
uint32_t msg_backend_idx(struct msg *msg, uint8_t *key, uint32_t keylen);
#endif /* DA_MSG_H_ */
