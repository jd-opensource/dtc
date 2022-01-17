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

#include<inttypes.h>
#include<stdbool.h>
#include "da_response.h"
#include "da_msg.h"
#include "da_conn.h"
#include "da_errno.h"
#include "da_request.h"
#include "da_util.h"
#include "da_event.h"
#include "da_core.h"
#include "da_stats.h"
#include "da_time.h"
#include "da_top_percentile.h"
#include "my/my_comm.h"

extern char g_dtc_key[DTC_KEY_MAX];
extern int g_dtc_key_type;

void rsp_put(struct msg *msg) {
	ASSERT(!msg->request);
	ASSERT(msg->peer == NULL);

	msg_put(msg);
}

struct msg *rsp_get(struct conn *conn) {
	ASSERT(conn !=NULL && conn->fd >0);
	struct msg *msg;

	msg = msg_get(conn, false);
	if (msg == NULL) {
		log_error("rsp enter,get msg from pool_2msg error,lack of memory");
		conn->error = 1;
		conn->err = CONN_MSG_GET_ERR;
	}

	return msg;
}

struct msg *rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc) {
	struct msg *msg;

	ASSERT(ctx!=NULL);
	ASSERT(conn!=NULL && conn->fd>0);

	if (conn->eof) {
		msg = conn->rmsg;
		if (msg != NULL) {
			conn->rmsg = NULL;

			ASSERT(msg->peer == NULL);
			ASSERT(!msg->request);

			log_error(
					"eof s %d discarding incomplete rsp %"PRIu64" len " "%"PRIu32"",
					conn->fd, msg->id, msg->mlen);

			rsp_put(msg);
		}
		/* no half connection */
		conn->done = 1;
		log_error("s %d active %d is done", conn->fd, conn->active(conn));

		return NULL;
	}
	msg = conn->rmsg;
	if (msg != NULL) {
		ASSERT(!msg->request);
		return msg;
	}

	if (!alloc) {
		return NULL;
	}

	msg = rsp_get(conn);
	if (msg != NULL) {
		conn->rmsg = msg;
	}

	return msg;
}

static bool rsp_filter_empty(struct context *ctx, struct conn *conn,
		struct msg *msg) {
	ASSERT(conn->type & BACKWORK);

	/*filter empty msg*/
	if (msg_empty(msg)) {
		ASSERT(conn->rmsg == NULL);
		log_debug("filter empty rsp %"PRIu64" on s %d", msg->id, conn->fd);
		rsp_put(msg);
		return true;
	}
	return false;
}

static bool rsp_filter_swallow(struct context *ctx, struct conn *conn,
		struct msg *msg) {
	ASSERT(conn->type & BACKWORK);
	struct msg *pmsg;

	pmsg = msg->peer;
	/*filter the msg must be swallow*/
	if (pmsg->swallow) {
		pmsg->done = 1;
		log_debug(
				"swallow rsp %"PRIu64" len %"PRIu32" of req " "%"PRIu64" on s %d",
				msg->id, msg->mlen, pmsg->id, conn->fd);
		req_put(pmsg);
		return true;
	}
	return false;
}

static void rsp_recv_done_stats(struct context *ctx, struct cache_instance *ci, struct msg *msg)
{
    ASSERT(!msg->request);

    stats_server_incr(ctx, ci, server_responses);
    stats_server_incr_by(ctx, ci, server_response_bytes, msg->mlen);
}


int dtc_header_remove(struct msg* msg)
{
	struct mbuf* mbuf = STAILQ_LAST(&msg->buf_q, mbuf, next);
	if(!mbuf)
		return -1;

	struct mbuf* new_buf = mbuf_get();
	if(new_buf == NULL)
		return -2;

	mbuf_copy(new_buf, mbuf->start + sizeof(struct DTC_HEADER_V2), mbuf_length(mbuf) - sizeof(struct DTC_HEADER_V2));

	mbuf_remove(&msg->buf_q, mbuf);
	mbuf_put(mbuf);
	mbuf_insert(&msg->buf_q, new_buf);

	msg->mlen = mbuf_length(new_buf);
	log_debug("msg->mlen:%d mbuf_length(mbuf):%d", msg->mlen, mbuf_length(mbuf));

	return 0;
}

int key_define_handle(struct msg* msg)
{
	struct mbuf* mbuf = STAILQ_LAST(&msg->buf_q, mbuf, next);
	int buf_len = 0;

	log_debug("key_define_handle entry.");

	if(!mbuf)
		return -1;

	buf_len = mbuf->last - mbuf->start;
	if(buf_len < sizeof(struct DTC_HEADER_V2))
		return -3;

	uint8_t* pos = mbuf->start + sizeof(struct DTC_HEADER_V2);
	uint8_t type = *pos;
	uint8_t key_len;
	int i = 0;
	g_dtc_key_type = type;
	pos++;

	key_len = *pos;
	pos++;
	if(key_len > DTC_KEY_MAX || key_len <= 0)
		return -2;
	
	if(mbuf->last - pos < key_len)
	{
		log_debug("key len:%d %d", mbuf->last - pos, key_len);
		return -4;
	}

	memcpy(g_dtc_key, pos, key_len);
	g_dtc_key[key_len] = '\0';

	for(i = 0; i < key_len; i++)
		g_dtc_key[i] = upper(g_dtc_key[i]);
	
	log_info("dtc key:%s", g_dtc_key);

	log_debug("key_define_handle leave.");

	return 0;
}

void rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
		struct msg *nmsg) {
	log_debug("rsp_recv_done entry.");
	ASSERT(conn->type & BACKWORK);
	ASSERT(msg != NULL && conn->rmsg == msg);
	ASSERT(!msg->request && msg->peerid > 0);
	ASSERT(msg->owner == conn);
	ASSERT(nmsg == NULL || !nmsg->request);
	ASSERT(msg->peerid > 0);

	struct rbnode *tarnode;
	struct msg *req;
	struct conn *c_conn;
	int ret = 0;

	/* enqueue next message (response), if any */
	conn->rmsg = nmsg;
	/*filter the empty message*/
	
	if (rsp_filter_empty(ctx, conn, msg)) {
		ASSERT(conn->rmsg==NULL);
		return;
	}
	
	//include timeout msg
	rsp_recv_done_stats(ctx,conn->owner,msg);

	struct rbnode tnode;
	tnode.key = msg->peerid; //peer msg_id for search,get from package
	log_debug("rbtree node key: %"PRIu64", id:%d, peerid:%d, tree:%p %d %d",tnode.key, msg->id, msg->peerid, &conn->msg_tree, conn->msg_tree.root->key, conn->msg_tree.sentinel->key);
	tarnode = rbtree_search(&conn->msg_tree, &tnode);
	if (tarnode == NULL) { //node has been deleted by timeout
		log_debug("rsp msg id: %"PRIu64" peerid :%"PRIu64" search peer msg error,msg is not in the tree",
				msg->id, msg->peerid);
		rsp_put(msg);
		return;
	}
	log_debug("rsp msg id: %"PRIu64" peerid :%"PRIu64" search peer msg success,msg is in the tree",
					msg->id, msg->peerid);
	//set req and rsp
	req = (struct msg *) tarnode->data;
	c_conn = req->owner;
	req->peer = msg;
	req->peerid = msg->id;
	msg->peer = req;
	msg->peerid = req->id;
	msg->peer_conn = c_conn;
	
	log_debug("set peer msg success msg id: %"PRIu64" peerid :%"PRIu64"",msg->id, msg->peerid);
	/*delete node from msg rbtree*/
	conn->de_msgtree(ctx, conn, req);
	
	
	/*filter swallow msg,swallow the message*/
	if (rsp_filter_swallow(ctx, conn, msg)) {
		return;
	}

	//Get请求信息放在此处进行统计
	if(req->cmd == MSG_REQ_GET)
	{
		stats_pool_incr(ctx, c_conn->owner, pool_requests_get);
	}
	
	if(msg->hitflag)
	{
		stats_pool_incr_by(ctx,c_conn->owner,pool_tech_hit,msg->hitflag&0xffff);
		if(msg->cmd == MSG_RSP_RESULTSET)
		{
			if(req->frag_id == 0)
				stats_pool_incr(ctx,c_conn->owner,pool_logic_hit);
			else
				stats_pool_incr_by(ctx,c_conn->owner,pool_logic_hit,(msg->hitflag>>16)&0xffff);
		}
	}

	//set the req to be done
	req->done = 1;

	//pre_coalesce the sub rsp
	if (req->frag_owner != NULL) {
		req->frag_owner->nfrag_done++;
	}

	if (req_done(c_conn, req)) {
		log_debug("msg is done , rsp msg id: %"PRIu64"",req->id);
		
		switch(msg->admin)
		{
			case CMD_NOP:
				dtc_header_remove(msg);
				rsp_forward(ctx, c_conn, req);
				break;
			case CMD_KEY_DEFINE:
				ret = key_define_handle(msg);
				if(ret < 0)
				{
					log_error("get dtc key error:%d", ret);
				}
				c_conn->dequeue_inq(ctx, c_conn, req);
				req_put(req);
				break;
			default:
				log_error("msg admin error:%d", msg->admin);
		}
			
	}

	log_debug("rsp_recv_done leave.");
	return;
}

static void rsp_make_error(struct context *ctx, struct conn *conn,
		struct msg *msg) {
	struct msg *cmsg, *nmsg; /* current and next message (request) */
	uint64_t id;
	int err;

	ASSERT(conn->type & FRONTWORK);
	ASSERT(msg->request);
	ASSERT(msg->owner == conn);

	id = msg->frag_id;
	if (id != 0) {
		for (err = 0, cmsg = TAILQ_NEXT(msg, c_i_tqe);
				cmsg != NULL && cmsg->frag_id == id; cmsg = nmsg) {
			nmsg = TAILQ_NEXT(cmsg, c_i_tqe);

			/* dequeue request (error fragment) from client outq ,record the first errmsg*/
			conn->dequeue_inq(ctx, conn, cmsg);
			if (err == 0 && cmsg->err != 0) {
				err = cmsg->err;
			}
			req_put(cmsg);
		}
	} else {
		err = msg->err;
	}
	msg->error = 1;
	msg->err = err;
	return;
}

static void rsp_forward_stats(struct context *ctx, struct server_pool *pool, struct msg *msg)
{
	stats_pool_incr(ctx, pool, pool_responses);
	stats_pool_incr_by(ctx, pool, pool_response_bytes, msg->mlen);
}

/*
 * no msg with swallow tag and all sub msgs has been finished
 */
void rsp_forward(struct context *ctx, struct conn *c_conn, struct msg *msg) {

	ASSERT(ctx !=NULL);
	ASSERT(c_conn !=NULL && c_conn->fd >0);
	ASSERT(msg !=NULL && msg->request == 1);

	int status;
	struct msg *req;

	if (msg->frag_owner != NULL) {
		req = msg->frag_owner;
	} else {
		req = msg;
	}

	//msg is in error
	if (req_error(c_conn, msg)) {
		rsp_make_error(ctx, c_conn, req);
		c_conn->dequeue_inq(ctx, c_conn, req);
		/*open the epoll out and add the rsp msg to out msgq*/
		if(c_conn->writecached == 0 && c_conn->connected == 1)
		{
			cache_send_event(c_conn);
		}
		c_conn->enqueue_outq(ctx, c_conn, req);
		return;
	}
	//msg is normal,request is getted
	status=msg->coalesce(req);
	if(status<0)
	{
		stats_pool_incr(ctx, c_conn->owner, coalesce_error);
	}
	log_debug("req peer mlen:%d",req->peer->mlen);
	c_conn->dequeue_inq(ctx, c_conn, req);
	if(c_conn->writecached == 0 && c_conn->connected == 1)
	{
		cache_send_event(c_conn);
	}

	rsp_forward_stats(ctx,c_conn->owner,req->peer);

	c_conn->enqueue_outq(ctx, c_conn, req);
	return;
}

struct msg *rsp_send_next(struct context *ctx, struct conn *conn)
{
	int status;
	struct msg *msg, *pmsg; /* current and next message */

	pmsg = TAILQ_FIRST(&conn->omsg_q);
	if (pmsg == NULL) {
		log_debug("del epoll out when to client send done");
		/* nothing to send as the server inq is empty */
		status = event_del_out(ctx->evb, conn);
		if (status != 0) {
			conn->error = 1;
			conn->err = CONN_EPOLL_DEL_ERR;
		}
		return NULL;
	}

	//smsg is response msg
	msg = conn->smsg;
	if (msg != NULL) {
		ASSERT(msg->request && !msg->peer !=NULL);
		pmsg = TAILQ_NEXT(msg->peer, c_o_tqe);
	}

	if (pmsg == NULL) {
		conn->smsg = NULL;
		return NULL;
	}

	ASSERT(pmsg->request && !pmsg->swallow);
	//msg with error
	if(pmsg->error ==1)
	{
		msg=msg_get_error(pmsg);
		log_debug("error:%d %p %p",pmsg->err, pmsg, msg);
		if(msg==NULL)
		{
			conn->error = 1;
			conn->err = CONN_MSG_GET_ERR;
			return NULL;
		}
		msg->peer=pmsg;
		msg->peerid = pmsg->id;
		pmsg->peer=msg;
		pmsg->peerid=msg->id;
	}
	else
	{
		msg=pmsg->peer;
	}

	conn->smsg = msg;

	log_debug("send next rsp %"PRIu64" len %"PRIu32" type %d on " "c %d",
				msg->id, msg->mlen, msg->cmd, conn->fd);
	return msg;
}

void rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{

	 struct msg *pmsg; /* peer message (request) */
	 ASSERT(conn->type & FRONTWORK);
	 ASSERT(conn->smsg == NULL);

	 log_debug("send done rsp %"PRIu64" on c %d", msg->id, conn->fd);

	 pmsg = msg->peer;

	 ASSERT(!msg->request && pmsg->request);
	 ASSERT(pmsg->peer == msg);
	 ASSERT(pmsg->done && !pmsg->swallow);

	 /* dequeue request from client outq */
	 conn->dequeue_outq(ctx, conn, pmsg); 
	 /* count the elapse time */
	 tv_update_date(0,1);
	 uint64_t elaspe_time = now_us - pmsg->start_ts;
	 stats_pool_incr_by(ctx, conn->owner, pool_elaspe_time, elaspe_time);
	 top_percentile_report(ctx, conn->owner, elaspe_time, msg->resultcode, RT_SHARDING);
	 top_percentile_report(ctx, conn->owner, elaspe_time, msg->resultcode, RT_ALL);
	 log_debug("send rsp %"PRIu64", peer id: %"PRIu64", len %"PRIu32", len %"PRIu32", type %d on " "c %d success use %"PRIu64"us",
	 				msg->id, pmsg->id, msg->mlen, pmsg->mlen, msg->cmd, conn->fd,now_us - pmsg->start_ts);
	 req_put(pmsg);
}
