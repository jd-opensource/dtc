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

#include <limits.h>
#include "da_msg.h"
#include "da_mem_pool.h"
#include <stddef.h>
#include "da_time.h"
#include "da_server.h"
#include "da_time.h"
#include "da_protocal.h"
#include "da_request.h"
#include "my/my_parse.h"
#include "my/my_comm.h"
#include "da_core.h"
#include "limits.h"
#include "da_conn.h"
#include "da_stats.h"


#define __IOV_MAX	1024
# define IOV_MAX __IOV_MAX

#if (IOV_MAX > 128)
#define NC_IOV_MAX 128
#else
#define NC_IOV_MAX IOV_MAX
#endif

static uint64_t msg_id; /* message id counter */
static uint64_t frag_id; /* fragment id counter */
static struct rbtree tmo_rbt; /* timeout rbtree */
static struct rbnode tmo_rbs; /* timeout rbtree sentinel */
struct pool_head *pool2_msg = NULL;

#define DEFINE_ACTION(_name) string(#_name),
static struct string msg_type_strings[] = {
MSG_TYPE_CODEC( DEFINE_ACTION )
null_string };
#undef DEFINE_ACTION

static uint64_t get_msg_id() {
	if (msg_id > UINT64_MAX)
		msg_id = 0;
	return msg_id++;
}

inline uint64_t msg_gen_frag_id(void) {
	if (frag_id > UINT64_MAX)
		frag_id = 0;
	return ++frag_id;
}

static struct msg *msg_from_rbe(struct rbnode *node) {
	struct msg *msg;
	int offset;

	offset = offsetof(struct msg, tmo_rbe);
	msg = (struct msg *) ((char *) node - offset);

	return msg;
}

struct msg *msg_tmo_min(void) {
	struct rbnode *node;

	node = rbtree_min(&tmo_rbt);
	if (node == NULL) {
		return NULL;
	}

	return msg_from_rbe(node);
}

void msg_tmo_insert(struct msg *msg, struct conn *conn) {
	struct rbnode *node;
	int timeout;

	ASSERT(msg->request);
	ASSERT(!msg->error && !msg->swallow);

	timeout = server_timeout(conn);
	if (timeout <= 0) {
		return;
	}
	node = &msg->tmo_rbe;
	node->key = now_ms + timeout;
	node->data = conn;

	rbtree_insert(&tmo_rbt, node);
}

void msg_tmo_delete(struct msg *msg) {
	struct rbnode *node;

	node = &msg->tmo_rbe;

	/* already deleted */

	if (node->data == NULL) {
		return;
	}

	rbtree_delete(&tmo_rbt, node);
}

int msg_init() {
	pool2_msg = create_pool("msg", sizeof(struct msg), MEM_F_SHARED);
	if (pool2_msg == NULL) {
		return -1;
	}
	msg_id = 0;
	frag_id = 0;
	// Red-black tree, timeout settings
	rbtree_init(&tmo_rbt, &tmo_rbs);
	return 0;
}

int msg_deinit() {
	void *res = pool_destroy(pool2_msg);
	if (res == NULL) {
		log_debug("free msg pool success!");
		return 0;
	} else {
		log_error("pool is in use,can't be free!");
		return -1;
	}
}

static struct msg *_msg_get() {
	ASSERT(pool2_msg !=NULL);

	struct msg *m = pool_alloc(pool2_msg);
	if (m == NULL) {
		log_error("get msg from msg_2pool error!");
		return NULL;
	}

	STAILQ_INIT(&m->buf_q);
	m->done = 0;
	m->err = 0;
	m->error = 0;
	m->frag_id = 0;
	m->frag_owner = NULL;
	m->fragment = NULL;
	m->id = get_msg_id();
	m->mlen = 0;
	rbtree_node_init(&m->msg_rbe);
	m->nfrag = 0;
	m->nfrag_done = 0;
	m->owner = NULL;
	m->parse_res = MSG_PARSE_OK;
	m->parser = NULL;
	m->peer = NULL;
	m->coalesce = NULL;
	m->request = 0;
	m->start_ts = 0;
	m->swallow = 0;
	rbtree_node_init(&m->tmo_rbe);
	m->cmd = MSG_NOP;
	m->serialnr = 0;
	string_init(&m->accesskey);
	m->keyCount = 0;
	m->peerid = 0;
	m->flags = 0;
	m->cur_parse_id = 0;
	m->cur_parse_type = 0;
	m->cur_parse_lenth = 0;
	m->state = 0;
	m->field = 0;
	m->parsed_len = 0;
	m->pos = NULL;
	m->token = NULL;
	m->subkeylen = 0;
	m->subkeycount = 0;
	m->keylen = 0;
	m->keyendbuf = NULL;
	m->keycountendbuf = NULL;
	m->keycountendpos = NULL;
	m->keycountlen = 0;
	m->keycountstartlen = 0;
	m->peer_conn=NULL;
	m->idx = -1;
	m->numrows=0;
	m->affectrows=0;
	m->totalrows=0;
	m->keytype=0;
	m->resultcode=0;
	m->keyendpos=NULL;
	m->seclen=NULL;
	m->sec_parsed_len=0;
	m->ferror = 0;
	m->fdone = 0;
	m->cli_inq = 0;
	m->cli_outq = 0;
	m->sev_inq = 0;
	m->sev_msgtree = 0;
	m->fieldnums = 0;
	m->setsplitbuf =NULL;
	m->setsplitpos =NULL;
	m->hitflag = 0;
	m->sending = 0;

	m->pkt_nr = 0;
	m->ismysql = 0;

	return m;
}

struct msg *msg_get(struct conn *conn, bool request) {
	struct msg *msg;
	msg = _msg_get();
	if (msg == NULL) {
		log_error("_msg_get error,lack of memory");
		return NULL;
	}
	msg->owner = conn;
	msg->request = request ? 1 : 0;

	if (msg->request) {
		msg->parser = my_parse_req;
	} else {
		msg->parser = my_parse_rsp;
	}
	msg->fragment = my_fragment;
	msg->coalesce = dtc_coalesce;
	msg->start_ts = now_us;

	log_debug("get msg %p", msg);
	return msg;
}

/*
 * Release resources associated with msg, reclaim msg object
 */
void msg_put(struct msg *m) {

	ASSERT(pool2_msg !=NULL);
	if (m == NULL) {
		return;
	}
	while (!STAILQ_EMPTY(&m->buf_q)) {
		struct mbuf *mbuf = STAILQ_FIRST(&m->buf_q);
		mbuf_remove(&m->buf_q, mbuf);
		mbuf_put(mbuf);
	}
	pool_free(pool2_msg, m);
	log_debug("put msg %p", m);
}

struct string *
msg_type_string(msg_type_t type) {
	return &msg_type_strings[type];
}

bool msg_empty(struct msg *msg) {
	return msg->mlen == 0 ? true : false;
}

static int msg_parsed(struct context *ctx, struct conn *conn, struct msg *msg) {
	struct msg *nmsg;
	struct mbuf *mbuf, *nbuf;
	struct server *server;
	struct cache_instance *ins;
	
	mbuf = STAILQ_LAST(&msg->buf_q, mbuf, next);
	if (msg->pos == mbuf->last) {
		/* no more data to parse */
		log_debug("no more data to parse, parsed %d byte. recv done(%p %p %p)",
			  mbuf_length(mbuf), mbuf->pos, mbuf->last, msg->pos);
		conn->recv_done(ctx, conn, msg, NULL);
		return 0;
	}
	log_debug("has more data to parse....");
	if (conn->type & FRONTWORK)
		stats_pool_incr(ctx, conn->owner, pool_package_split);
	else {
		ins = conn->owner;
		server = ins->owner;
		stats_pool_incr(ctx, server->owner, pool_package_split);
	}
	/*
	 *	if split msg error because of lack of memory,
	 *	close connection,we will chose the part of small to
	 *	split
	 */
	nbuf = mbuf_split(mbuf,msg->pos, NULL, NULL);
	if (nbuf == NULL) {
		log_error("msg split error,because of mbuf split error");
		conn->error=1;
		conn->err=CONN_BUF_GET_ERR;
		return -1;
	}

	log_debug("msg request:%d", msg->request);
	nmsg = msg_get(msg->owner, msg->request);
	if (nmsg == NULL) {
		log_error("msg split error,because of get a new msg error");
		conn->error=1;
		conn->err=CONN_MSG_GET_ERR;
		return -1;
	}
	//set the start ts of new msg to the old msg for calculate
	//nmsg->start_ts = msg->start_ts;

	mbuf_insert(&nmsg->buf_q, nbuf);
	nmsg->pos = nbuf->pos;
	nmsg->mlen = mbuf_length(nbuf);
	msg->mlen -= nmsg->mlen;

	conn->recv_done(ctx, conn, msg, nmsg);
	return 0;
}

static int msg_repair(struct context *ctx, struct conn *conn, struct msg *msg) {
	struct mbuf *nbuf, *mbuf;
	mbuf=STAILQ_LAST(&msg->buf_q, mbuf, next);
	nbuf = mbuf_split(mbuf,msg->pos, NULL, NULL);
	if (nbuf == NULL) {
		log_error("msg_repair fail,because of mbuf split error");
		conn->error=1;
		conn->err=CONN_BUF_GET_ERR;
		return -1;
	}
	mbuf_insert(&msg->buf_q, nbuf);
	msg->pos = nbuf->pos;
	return 0;
}

static int msg_parse(struct context *ctx, struct conn *conn, struct msg *msg) {

	ASSERT(msg!=NULL);
	int status;
	if (msg_empty(msg)) {
		/* no data to parse */
		log_debug("empty msg,no data to parse");
		conn->recv_done(ctx, conn, msg, NULL);
		return 0;
	}
	msg->parser(msg);
	switch (msg->parse_res) {
	case MSG_PARSE_OK:
		log_debug("msg id:%"PRIu64" parsed ok!",msg->id);
		status = msg_parsed(ctx, conn, msg);
		break;

	case MSG_PARSE_REPAIR:
		log_debug("msg id:%"PRIu64" need repair!",msg->id);
		status = msg_repair(ctx, conn, msg);
		break;

	case MSG_PARSE_AGAIN:
		log_debug("msg id:%"PRIu64" need parse again!",msg->id);
		status = 0;
		break;
	
	case MSG_PARSE_ERROR_NO_SELECTED_DB:
		status = 0;
		error_reply(msg, conn, ctx, MY_ERR_NO_DB_SELECTED);
		break;

	default:
		log_error("parser get some trouble:%d", msg->parse_res);
		status = 0;
		error_reply(msg, conn, ctx, 0);
		break;
	}

	return status;
}

static int msg_recv_chain(struct context *ctx, struct conn *conn,
		struct msg *msg) {
	int status;
	struct msg *nmsg;
	struct mbuf *mbuf;
	size_t msize;
	ssize_t n;

	mbuf = STAILQ_LAST(&msg->buf_q, mbuf, next);
	if (mbuf == NULL || mbuf_full(mbuf)) {
		/*
		 * close connection,because of lack of memory,
		 * get mbuf error
		 */
		mbuf = mbuf_get();
		if (mbuf == NULL) {
			log_error("get mbuf for conn %d error,lack of memroy!", conn->fd);
			conn->error=1;
			conn->err=CONN_BUF_GET_ERR;
			return -1;
		}
		mbuf_insert(&msg->buf_q, mbuf);
		msg->pos = mbuf->pos;
	}

	ASSERT(mbuf->end - mbuf->last > 0);
	msize = mbuf_size(mbuf);

	log_debug("need recv %lu bytes data from system recv buffer",msize);
	n = conn_recv(conn, mbuf->last, msize);
	if (n < 0) {
		//-2==EGAIN
		if (n == -2) {
			return 0;
		} else {
			//close connection bucause of recv error
			conn->error=1;
			conn->err=CONN_RECV_ERR;
			return -1;
		}
	}

	ASSERT((mbuf->last + n) <= mbuf->end);
	mbuf->last += n;
	msg->mlen += (uint32_t) n;
	log_debug("mbuf recv %ld bytes data actually.(%p %p %p)", mbuf->last - mbuf->pos, mbuf->last, mbuf->pos, msg->pos);
	for (;;) {
		status = msg_parse(ctx, conn, msg);
		if (status != 0) {
			return status;
		}

		/* get next message to parse,but not allocated from memory pool */
		nmsg = conn->recv_next(ctx, conn, false);
		if (nmsg == NULL || nmsg == msg) {
			/* no more data to parse */
			break;
		}
		msg = nmsg;
	}
	return 0;
}

int msg_recv(struct context *ctx, struct conn *conn) {
	int status;
	struct msg *msg;

	ASSERT(conn->flag & RECV_ACTIVE);

	conn->flag |= RECV_READY;

	do {
		/*
		 * Get a msg from the msg pool. When getting msg fails due to memory reasons,
		 * set conn->err flag. Don't process empty returns due to client connection closure
		 */
		
		msg = conn->recv_next(ctx, conn, true);
		if (msg == NULL) {
			return 0;
		}
		
		status = msg_recv_chain(ctx, conn, msg);
		if (status != 0) {
			return status;
		}
		
	} while (conn->flag & RECV_READY);
	return 0;
}

/*
 * copy source msg mbuf to dist buf q's tail,can stride over multi mbufs
 */
int msg_append_buf(struct msg *msg, struct mbuf *sbuf, uint8_t *pos, size_t len) {
	int allowcplen, slen, dlen,tlen;
	struct mbuf *dis_buf;

	tlen=len;
	struct buf_stqh *dis_buf_q = &msg->buf_q;
	dis_buf = STAILQ_LAST(dis_buf_q, mbuf, next);
	if (dis_buf == NULL || mbuf_size(dis_buf) == 0) {
		dis_buf = mbuf_get();
		if (dis_buf == NULL) {
			return -1;
		}
		STAILQ_INSERT_TAIL(dis_buf_q, dis_buf, next);
	}
	while (len > 0) {
		dlen = mbuf_size(dis_buf);
		slen = sbuf->last - pos;
		allowcplen=MIN(MIN(slen,dlen),len);
		mbuf_copy(dis_buf, pos, allowcplen);
		len -= allowcplen;
		pos += allowcplen;
		if (dis_buf->last == dis_buf->end) {
			dis_buf = mbuf_get();
			if (dis_buf == NULL) {
				log_error("msg_append_buf get buf error");
				return -1;
			}
			STAILQ_INSERT_TAIL(dis_buf_q, dis_buf, next);
		}
		if (pos == sbuf->last && len>0) {
			sbuf = STAILQ_NEXT(sbuf, next);
			if (sbuf == NULL) {
				log_error("msg_append_buf error,the source buffer end");
				return -1;
			}
			pos = sbuf->pos;
		}
	}
	msg->mlen += tlen;
	return 0;
}

struct mbuf *msg_insert_mem_bulk(struct msg *msg,struct mbuf *mbuf,uint8_t *pos,size_t len){

	ASSERT(msg!=NULL);
	ASSERT(mbuf!=NULL);
	ASSERT(pos!=NULL);
	ASSERT(size > 0);

	size_t cp_len,tlen=len;
	struct mbuf *nbuf;
	while(tlen>0)
	{
		cp_len=MIN(tlen,mbuf_size(mbuf));
		mbuf_copy(mbuf,pos,cp_len);
		tlen=tlen-cp_len;
		if(tlen>0)
		{
			nbuf=mbuf_get();
			if(nbuf==NULL)
			{
				log_error("mbuf get error,copy mem bulk failed");
				return NULL;
			}
			STAILQ_INSERT_AFTER(&msg->buf_q, mbuf, nbuf, next);
			mbuf=nbuf;
		}
	}
	msg->mlen+=len;
	return mbuf;
}

uint32_t msg_backend_idx(struct msg *msg, uint8_t *key, uint32_t keylen) {
	struct conn *conn = msg->owner;
	struct server_pool *pool = conn->owner;

	return server_pool_idx(pool, key, keylen);
}

static int msg_send_chain(struct context *ctx, struct conn *conn,
		struct msg *msg) {
	struct msg_tqh send_msgq; /* send msg q */
	struct msg *nmsg; /* next msg */
	struct mbuf *mbuf, *nbuf; /* current and next mbuf */
	size_t mlen; /* current mbuf data length */
	struct iovec *ciov, iov[NC_IOV_MAX]; /* current iovec */
	struct array sendv; /* send iovec */
	size_t nsend, nsent; /* bytes to send; bytes sent */
	size_t limit; /* bytes to send limit */
	ssize_t n; /* bytes sent by sendv */

	TAILQ_INIT(&send_msgq);
	array_set(&sendv, iov, sizeof(iov[0]), NC_IOV_MAX);

	nsend = 0;

	limit = SSIZE_MAX;
	for (;;) {
		// msg is ths message which will be sent.
		TAILQ_INSERT_TAIL(&send_msgq, msg, o_tqe);
		msg->sending = 1;

		for (mbuf = STAILQ_FIRST(&msg->buf_q);
				mbuf != NULL && array_n(&sendv) < NC_IOV_MAX && nsend < limit;
				mbuf = nbuf) {
			nbuf = STAILQ_NEXT(mbuf, next);

			if (mbuf_empty(mbuf)) {
				continue;
			}

			mlen = mbuf_length(mbuf);
			log_debug("mbuf len, len:%zu, msg len:%d; %p %p", mlen, msg->mlen, mbuf->last, mbuf->pos);
			if ((nsend + mlen) > limit) {
				mlen = limit - nsend;
			}

			ciov = array_push(&sendv);
			ciov->iov_base = mbuf->pos;
			ciov->iov_len = mlen;

			nsend += mlen;
		}

		if (array_n(&sendv) >= NC_IOV_MAX || nsend >= limit) {
			break;
		}

		// build msg which will be sent by next loop.
		msg = conn->send_next(ctx, conn);
		if (msg == NULL) {
			break;
		}
	}
	log_debug("msg data need send nsend:%lu",nsend);
	conn->smsg = NULL;
	if (!TAILQ_EMPTY(&send_msgq) && nsend != 0) {
		//return -2 for EAGAIN
		n = conn_sendv(conn, &sendv, nsend);
	} else {
		n = 0;
	}
	log_debug("end conn_send");
	nsent = n > 0 ? (size_t) n : 0;
	for (msg = TAILQ_FIRST(&send_msgq); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, o_tqe);

		TAILQ_REMOVE(&send_msgq, msg, o_tqe);
		log_debug("send done msg len:%d",msg->mlen);
		if (nsent == 0) {
			if (msg->mlen == 0) {
				msg->sending = 0;
				conn->send_done(ctx, conn, msg);
			}
			// Because we need to execute TAILQ_REMOVE(&send_msgq, msg, m_tqe);
			continue;
		}

		/* adjust mbufs of the sent message */
		for (mbuf = STAILQ_FIRST(&msg->buf_q); mbuf != NULL; mbuf = nbuf) {
			nbuf = STAILQ_NEXT(mbuf, next);

			if (mbuf_empty(mbuf)) {
				continue;
			}

			mlen = mbuf_length(mbuf);
			if (nsent < mlen) {
				/* mbuf was sent partially; process remaining bytes later */
				mbuf->pos += nsent;
				ASSERT(mbuf->pos < mbuf->last);
				nsent = 0;
				break;
			}

			/* mbuf was sent completely; mark it empty */
			mbuf->pos = mbuf->last;
			nsent -= mlen;
		}
		/* message has been sent completely, finalize it */
		if (mbuf == NULL) {
			msg->sending = 0;
			if (conn->type == FRONTWORK){
				struct msg *pmsg = msg->peer;
				decr_instance_failure_time(pmsg);

			}
			conn->send_done(ctx, conn, msg);
		}
	}

	ASSERT(TAILQ_EMPTY(&send_msgq));
	if (n >= 0) {
		return 0;
	}
	//EAGAIN return 0;
	return (n == -2) ? 0 : -1;
}

int msg_send(struct context *ctx, struct conn *conn) {

	ASSERT(conn->flag & SEND_ACTIVE);

	int status;
	struct msg *msg;	
	conn->flag |= SEND_READY;
	do {
		msg = conn->send_next(ctx, conn);
		
		if (msg == NULL) {
			/* nothing to send */
			return 0;
		}
		status = msg_send_chain(ctx, conn, msg);		
		if (status != 0) {
			return status;
		}		
	} while (conn->flag & SEND_READY);
	
	return 0;
}

/*
 * make a response msg for error
 */
struct msg *msg_get_error(struct msg *smsg) {

	int status;
	struct msg *dmsg;
	dmsg=_msg_get();
	if(dmsg == NULL)
	{
		return NULL;
	}
	dmsg->state=0;
	dmsg->cmd=MSG_RSP_RESULTCODE;
	dmsg->totalrows=0;
	dmsg->affectrows=0;
	dmsg->serialnr=smsg->serialnr;

	status=dtc_error_reply(smsg,dmsg);
	if(status<0)
	{
		msg_put(dmsg);
		return NULL;
	}
	return dmsg;
}

void msg_dump(struct msg *msg, int level) {

}
