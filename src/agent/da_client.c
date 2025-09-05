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
#include "da_client.h"
#include "da_core.h"
#include "da_event.h"
#include "da_queue.h"
#include "da_util.h"
#include "da_server.h"
#include "da_msg.h"
#include "da_request.h"
#include "da_conn.h"
#include "da_stats.h"
#include "da_time.h"
#include "da_top_percentile.h"

void client_ref(struct conn *conn, void *owner) {
	ASSERT((conn->type & FRONTWORK) && conn->owner==NULL);

	struct server_pool *pool = owner;
	conn->family = 0;
	conn->addrlen = 0;
	conn->addr = NULL;

	pool->c_conn_count++;
	conn->owner = owner;

	TAILQ_INSERT_TAIL(&pool->c_conn_q, conn, conn_tqe);
	log_debug("ref conn %p owner %p into pool '%.*s'", conn, pool,
			pool->name.len, pool->name.data);
}

void client_unref(struct conn *conn) {
	ASSERT((conn->type & FRONTWORK) && conn->owner!=NULL);
	struct server_pool *pool;

	pool = conn->owner;
	conn->owner = NULL;

	ASSERT(pool->c_conn_count != 0);

	pool->c_conn_count--;
	TAILQ_REMOVE(&pool->c_conn_q, conn, conn_tqe);

	log_debug("unref conn %p owner %p from pool '%.*s'", conn, pool,
			pool->name.len, pool->name.data);
}

int client_active(struct conn *conn) {
	ASSERT(conn->type & FRONTWORK);

	if (!TAILQ_EMPTY(&conn->imsg_q)) {
		return true;
	}
	if (!TAILQ_EMPTY(&conn->omsg_q)) {
		return true;
	}
	if (conn->rmsg != NULL) {
		return true;
	}
	if (conn->smsg != NULL) {
		return true;
	}
	return false;
}

static void client_close_stats(struct context *ctx, struct server_pool *pool, int err,
        unsigned eof)
{
	 stats_pool_decr(ctx, pool, client_connections);
	 if(eof)
	 {
		 stats_pool_incr(ctx, pool, client_eof);
		 return;
	 }
	 if(err)
	 {
		 stats_pool_incr(ctx, pool, client_err);
	 }
	 return;
}

void client_close(struct context *ctx, struct conn *conn) {
	ASSERT(ctx!=NULL);
	ASSERT(conn->type & FRONTWORK);

	int status;
	struct msg *msg, *nmsg;
	client_close_stats(ctx,conn->owner, conn->err,conn->eof);

	if (conn->fd < 0) {
		conn->unref(conn);
		conn_put(conn);
		return;
	}

	for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, c_i_tqe);
		conn->dequeue_inq(ctx, conn, msg);
		/* count the elapse time */
		if(msg->frag_id == 0)
		{
			int64_t elaspe_time = now_us - msg->start_ts;
			stats_pool_incr_by(ctx, conn->owner, pool_elaspe_time, elaspe_time);
			top_percentile_report(ctx, conn->owner, elaspe_time, 1, RT_SHARDING);
			top_percentile_report(ctx, conn->owner, elaspe_time, 1, RT_ALL);
		}

		if (msg->done) {			
			req_put(msg);
		} else {
			//set swallow flag
			msg->swallow = 1;
		}
	}

	for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, c_o_tqe);

		int64_t elaspe_time = now_us - msg->start_ts;
		/* count the elapse time */
		stats_pool_incr_by(ctx, conn->owner, pool_elaspe_time, elaspe_time);
		top_percentile_report(ctx, conn->owner, elaspe_time, 1, RT_SHARDING);
		top_percentile_report(ctx, conn->owner, elaspe_time, 1, RT_ALL);

		conn->dequeue_outq(ctx, conn, msg);
		req_put(msg);
	}

	msg = conn->rmsg;
	if (msg != NULL) {
		conn->rmsg = NULL;

		ASSERT(msg->request);

		req_put(msg);

		/* count the elapse time */
		int64_t elaspe_time = now_us - msg->start_ts;
		stats_pool_incr_by(ctx, conn->owner, pool_elaspe_time, elaspe_time);
		top_percentile_report(ctx, conn->owner, elaspe_time, 1, RT_SHARDING);
		top_percentile_report(ctx, conn->owner, elaspe_time, 1, RT_ALL);
		log_debug("close s %d discarding req %"PRIu64" len %"PRIu32" "
		                  "in error", conn->fd, msg->id, msg->mlen);
	}

	conn->unref(conn);

	status = close(conn->fd);
	if (status < 0) {
		log_error("close c %d failed, ignored: %s", conn->fd, strerror(errno));
	}
	conn->fd = -1;
	conn_put(conn);
}
