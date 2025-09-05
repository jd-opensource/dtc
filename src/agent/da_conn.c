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
#include <sys/uio.h>
#include "da_mem_pool.h"
#include "da_conn.h"
#include "da_listener.h"
#include "da_client.h"
#include "da_request.h"
#include "da_response.h"
#include "da_server.h"
#include "da_util.h"
#include "da_array.h"
#include "da_errno.h"
#include "da_time.h"

static uint64_t ntotal_conn; /* total # connections counter from start */
static uint32_t ncurr_conn; /* current # connections */
static uint32_t ncurr_cconn; /* current # client connections */

struct pool_head *pool2_conn = NULL;

int conn_init() {
	pool2_conn = create_pool("conn", sizeof(struct conn), MEM_F_SHARED);
	if (pool2_conn == NULL) {
		return -1;
	}
	return 0;
}

int conn_deinit() {
	void *res = pool_destroy(pool2_conn);
	if (res == NULL) {
		log_debug("free conn pool success!");
		return 0;
	} else {
		log_error("pool is in use,can't be free!");
		return -1;
	}
}

static struct conn *_conn_get() {

	ASSERT(pool2_conn !=NULL);

	struct conn *c = pool_alloc(pool2_conn);
	if (c == NULL) {
		return NULL;
	}
	c->active = NULL;
	c->addr = NULL;
	c->addrlen = 0;
	c->close = NULL;
	c->connected = 0;
	c->connecting =0;
	c->dequeue_outq = NULL;
	c->eof = 0;
	c->done = 0;
	c->enqueue_outq = NULL;
	c->enqueue_inq = NULL;
	c->dequeue_inq = NULL;
	c->en_msgtree = NULL;
	c->de_msgtree = NULL;
	c->err = 0;
	c->events = 0;
	c->family = -1;
	c->fd = -1;
	c->flag = 0;
	rbtree_init(&c->msg_tree, &c->msg_rbs);
	TAILQ_INIT(&c->omsg_q);
	TAILQ_INIT(&c->imsg_q);
	c->owner = NULL;
	c->recv = NULL;
	c->recv_bytes = 0;
	c->recv_done = NULL;
	c->recv_next = NULL;
	c->ref = NULL;
	c->rmsg = NULL;
	c->send = NULL;
	c->send_bytes = 0;
	c->send_done = NULL;
	c->send_next = NULL;
	c->smsg = NULL;
	c->type = 0;
	c->unref = NULL;
	c->error=0;
	c->writecached=0;
	c->isvalid = 0;
	memset(c->dbname, 0, 250);

	ntotal_conn++;
	ncurr_conn++;
	return c;
}

void conn_put(struct conn *c) {
	if (c == NULL) {
		return;
	}
	ncurr_conn--;
	if (c->type & FRONTWORK) {
		ncurr_cconn--;
	}
	pool_free(pool2_conn, c);
}

struct conn *get_listener(void *owner) {

	ASSERT(owner != NULL);

	struct conn *c = _conn_get();
	if (c == NULL) {
		return NULL;
	}
	c->type |= LISTENER;
	c->recv = listener_recv;
	c->close = listener_close;
	c->ref = listener_ref;
	c->unref = listener_unref;

	c->ref(c, owner);
	return c;
}

struct conn *get_client_conn(void *pool) {

	ASSERT(pool !=NULL);

	struct conn *c = _conn_get();
	if (c == NULL) {
		return NULL;
	}
	c->type |= FRONTWORK;
	c->active = client_active;
	c->close = client_close;
	c->ref = client_ref;
	c->unref = client_unref;

	c->recv = msg_recv;
	c->recv_next = req_recv_next;
	c->recv_done = req_recv_done;

	c->send = msg_send;
	c->send_next = rsp_send_next;
	c->send_done = rsp_send_done;

	c->enqueue_outq = req_client_enqueue_omsgq;
	c->dequeue_outq = req_client_dequeue_omsgq;
	c->enqueue_inq = req_client_enqueue_imsgq;
	c->dequeue_inq = req_client_dequeue_imsgq;

	c->ref(c, pool);
	ncurr_cconn++;
	return c;
}

struct conn *get_instance_conn(void *pool) {
	struct conn *c = _conn_get();
	if (c == NULL) {
		return NULL;
	}

	c->type |= BACKWORK;
	c->active = server_active;
	c->ref = instance_ref;
	c->unref = instance_unref;
	c->close = server_close;

	c->send = msg_send;
	c->send_next = req_send_next;
	c->send_done = req_send_done;
	c->recv = msg_recv;
	c->recv_next = rsp_recv_next;
	c->recv_done = rsp_recv_done;

	c->enqueue_inq = req_server_enqueue_imsgq;
	c->dequeue_inq = req_server_dequeue_imsgq;

	c->en_msgtree = req_server_en_msgtree;
	c->de_msgtree = req_server_de_msgtree;

	c->ref(c, pool);
	return c;
}

uint64_t get_ntotal_conn() {
	return ntotal_conn;
}
uint32_t get_ncurr_conn() {
	return ncurr_conn;
}
uint32_t get_ncurr_cconn() {
	return ncurr_cconn;
}

/*
 * Return the context associated with this connection.
 */
struct context *conn_to_ctx(struct conn *conn) {
	struct server_pool *pool;
	if ((conn->type & FRONTWORK) || (conn->type & LISTENER)) {
		pool = conn->owner;
	} else {
		struct cache_instance *ins = conn->owner;
		struct server *server = ins->owner;
		pool = server->owner;
	}
	return pool->ctx;
}

ssize_t conn_recv(struct conn *conn, void *buf, size_t size) {
	ssize_t n;

	ASSERT(buf != NULL);
	ASSERT(size > 0);
	ASSERT(conn->flag & RECV_READY);

	for (;;) {
		n = da_read(conn->fd, buf, size);

		log_debug("recv on fd %d %zd of %zu", conn->fd, n, size);

		if (n > 0) {
			if (n < (ssize_t) size) {
				conn->flag &= ~RECV_READY;
			}
			conn->recv_bytes += (size_t) n;
			return n;
		}

		if (n == 0) {
			conn->flag &= ~RECV_READY;
			conn->eof = 1;
			log_debug("recv on fd %d eof rb %zu sb %zu", conn->fd,
					conn->recv_bytes, conn->send_bytes);
			return n;
		}

		if (errno == EINTR) {
			log_debug("recv on sd %d not ready - eintr", conn->fd);
			continue;
		} else if (errno == EAGAIN || errno == EWOULDBLOCK) {
			conn->flag &= ~RECV_READY;
			log_debug("recv on sd %d not ready - eagain", conn->fd);
			return -2;
		} else {
			conn->flag &= ~RECV_READY;
			conn->error = 1;
			conn->err = CONN_RECV_ERR;
			log_error("recv on sd %d failed: %s", conn->fd, strerror(errno));
			return -1;
		}
	}

	return -1;
}

ssize_t conn_sendv(struct conn *conn, struct array *sendv, size_t nsend) {
	ssize_t n;

	ASSERT(array_n(sendv) > 0);
	ASSERT(nsend != 0);
	ASSERT(conn->flag & SEND_READY);
	log_debug("enter conn_sendv");
	for (;;) {
		n = da_writev(conn->fd, sendv->elem, sendv->nelem);

		log_debug("sendv on sd %d %zd of %zu in %"PRIu32" buffers", conn->fd, n,
				nsend, sendv->nelem);

		if (n > 0) {
			if (n < (ssize_t) nsend) {
				conn->flag &= ~SEND_READY;
			}
			conn->send_bytes += (size_t) n;
			return n;
		}

		if (n == 0) {
			log_warning("sendv on sd %d returned zero", conn->fd);
			conn->flag &= ~SEND_READY;
			return 0;
		}

		if (errno == EINTR) {
			log_debug("sendv on sd %d not ready - eintr", conn->fd);
			continue;
		} else if (errno == EAGAIN || errno == EWOULDBLOCK) {
			conn->flag &= ~SEND_READY;		
			log_debug("sendv on sd %d not ready - eagain", conn->fd);
			return -2;
		} else {
			conn->flag &= ~SEND_READY;			
			conn->error = 1;
			conn->err = CONN_SEND_ERR;
			log_error("sendv on sd %d failed: %s", conn->fd, strerror(errno));
			return -1;
		}
	}
	log_debug("conn->flag:%d",conn->flag);
	return -1;
}
