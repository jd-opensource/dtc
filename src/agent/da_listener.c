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

#include "da_listener.h"
#include "da_array.h"
#include "da_server.h"
#include "da_conn.h"
#include "da_log.h"
#include "da_core.h"
#include "da_event.h"
#include "da_stats.h"
#include "my/my_net_send.h"
#include "da_msg.h"

void listener_ref(struct conn *l, void *owner)
{
	struct server_pool *pool = owner;
	ASSERT(owner == NULL);

	l->owner = pool;
	l->family = pool->family;
	l->addrlen = pool->addrlen;
	l->addr = pool->addr;
	pool->listener = l;
}

void listener_unref(struct conn *l)
{
	ASSERT(l->type & LISTENER);
	ASSERT(l->owner != NULL);

	struct server_pool *pool;

	pool = l->owner;
	l->owner = NULL;
	pool->listener = NULL;
}

void listener_close(struct context *ctx, struct conn *l)
{
	ASSERT(ctx != NULL);
	ASSERT(l->flag & LISTENER);

	int status;
	if (l->fd < 0) {
		l->unref(l);
		conn_put(l);
		return;
	}

	ASSERT(l->rmsg == NULL);
	ASSERT(l->smsg == NULL);

	l->unref(l);
	status = event_del_conn(ctx->evb, l);
	if (status < 0) {
		log_error("event del conn p %d failed, ignored: %s", l->fd,
			  strerror(errno));
	}

	status = close(l->fd);
	if (status < 0) {
		log_error("close p %d failed, ignored: %s", l->fd,
			  strerror(errno));
	}
	l->fd = -1;
	conn_put(l);
}

static int listener_reuse(struct conn *l)
{
	int status;
	struct sockaddr_un *un;

	switch (l->family) {
	case AF_INET:
	case AF_INET6:
		set_reuseaddr(l->fd);
		break;
	case AF_UNIX:
		un = (struct sockaddr_un *)l->addr;
		unlink(un->sun_path);
		status = 0;
		break;
	default:
		status = -1;
	}
	return status;
}

static int listener_listen(struct context *ctx, struct conn *l)
{
	int status;
	struct server_pool *pool = l->owner;

	ASSERT(l->type & LISTENER);
	l->fd = socket(l->family, SOCK_STREAM, 0);
	if (l->fd < 0) {
		log_error("socket failed: %s", strerror(errno));
		return -1;
	}
	status = listener_reuse(l);
	if (status < 0) {
		log_error(
			"reuse of addr '%.*s' for listening on p %d failed: %s",
			pool->addrstr.len, pool->addrstr.data, l->fd,
			strerror(errno));
		return status;
	}
	status = bind(l->fd, pool->addr, pool->addrlen);
	if (status < 0) {
		log_error("bind on p %d to addr '%.*s' failed: %s", l->fd,
			  pool->addrstr.len, pool->addrstr.data,
			  strerror(errno));
		return -1;
	}
	status = listen(l->fd, pool->backlog);
	if (status < 0) {
		log_error("listen on p %d on addr '%.*s' failed: %s", l->fd,
			  pool->addrstr.len, pool->addrstr.data,
			  strerror(errno));
		return -1;
	}
	status = set_nonblocking(l->fd);
	if (status < 0) {
		log_error("set nonblock on p %d on addr '%.*s' failed: %s",
			  l->fd, pool->addrstr.len, pool->addrstr.data,
			  strerror(errno));
		return -1;
	}

	return 0;
}

static int listener_inherited_listen(struct context *ctx, struct conn *l)
{
	int status;
	int fd;
	struct server_pool *pool = l->owner;
	ASSERT(p->proxy);

	fd = core_inherited_socket(da_unresolve_addr(l->addr, l->addrlen));
	if (fd > 0) {
		l->fd = fd;
	} else {
		status = listener_listen(ctx, l);
		if (status != 0) {
			return status;
		}
	}
	status = event_add_conn(ctx->evb, l);
	if (status < 0) {
		log_error("event add conn p %d on addr '%.*s' failed: %s",
			  l->fd, pool->addrstr.len, pool->addrstr.data,
			  strerror(errno));
		return -1;
	}
	status = event_del_out(ctx->evb, l);
	if (status < 0) {
		log_error("event del out p %d on addr '%.*s' failed: %s", l->fd,
			  pool->addrstr.len, pool->addrstr.data,
			  strerror(errno));
		return -1;
	}
	return 0;
}

int listener_each_init(void *elem, void *data)
{
	int status;
	struct server_pool *pool = elem;
	struct conn *l;

	l = get_listener(pool);
	if (l == NULL) {
		log_error("init listener fail,get conn fail!");
		return -1;
	}
	//status = listener_listen(pool->ctx, l);
	status = listener_inherited_listen(pool->ctx, l);
	if (status < 0) {
		l->close(pool->ctx, l);
		log_error("listener %d listen fail!", l->fd);
		return -1;
	}
	log_debug("pool:%p addr '%.*s' ,listen success", pool,
		  pool->addrstr.len, pool->addrstr.data);
	return 0;
}

int listener_each_deinit(void *elem, void *data)
{
	struct server_pool *pool = elem;
	struct conn *l;

	l = pool->listener;
	if (l != NULL) {
		l->close(pool->ctx, l);
	}
	log_debug(" addr '%.*s' ,deinit listener success", pool->addrstr.len,
		  pool->addrstr.data);
	return 0;
}

int listener_init(struct context *ctx)
{
	int status;
	ASSERT(array_n(&ctx->pool) != 0);
	status = array_each(&ctx->pool, listener_each_init, NULL);
	if (status != 0) {
		listener_deinit(ctx);
		return status;
	}
	log_debug("init all pool's listener");
	return 0;
}

void listener_deinit(struct context *ctx)
{
	int status;
	ASSERT(array_n(&ctx->pool) != 0);
	status = array_each(&ctx->pool, listener_each_deinit, NULL);
	if (status != 0) {
		return;
	}
	log_debug("deinit all listener success");
	return;
}

static int listener_accept(struct context *ctx, struct conn *l)
{
	int status;
	int fd;
	struct conn *c;

	ASSERT(l->type & LISTENER);
	ASSERT(l->fd > 0);
	ASSERT((l->flag & RECV_ACTIVE) && (l->flag & RECV_READY));

	for (;;) {
		fd = accept(l->fd, NULL, NULL);
		if (fd < 0) {
			if (errno == EINTR) {
				log_debug(
					"accept on listener %d not ready - eintr",
					l->fd);
				continue;
			}
			/*
			 * In multi-process scenario, simultaneous accept will cause error, errno=11, program swallows this error
			 */
			if (errno == EAGAIN || errno == EWOULDBLOCK ||
			    errno == ECONNABORTED) {
				log_debug("accept on l %d not ready - eagain",
					  l->fd);
				l->flag &= ~RECV_READY;
				return 0;
			}

			if (errno == EMFILE || errno == ENFILE) {
				log_debug(
					"accept on listener :%d fail :no enough fd for use",
					l->fd);
				l->flag &= ~RECV_READY;
				return 0;
			}
			log_error("accept on listener %d failed: %s", l->fd,
				  strerror(errno));
			return -1;
		}
		break;
	}

	/*
	 * Limit global FD resources, each process has separate resources
	 */
	if (get_ncurr_cconn() >= ctx->max_ncconn ||
	    get_ncurr_cconn() >
		    ((struct server_pool *)(l->owner))->client_connections) {
		log_error(
			"current conn:%d is biger than max client connection for ctx:%d",
			get_ncurr_cconn(), ctx->max_ncconn);
		status = close(fd);
		if (status < 0) {
			log_error("close client %d failed, ignored: %s", fd,
				  strerror(errno));
		}
		return 0;
	}

	c = get_client_conn(l->owner);
	if (c == NULL) {
		log_error("get conn for client %d from p %d failed: %s", fd,
			  l->fd, strerror(errno));
		status = close(fd);
		if (status < 0) {
			log_error("close client %d failed, ignored: %s", fd,
				  strerror(errno));
		}
		return -1;
	}
	c->fd = fd;

	status = set_nonblocking(c->fd);
	if (status < 0) {
		log_error("set nonblock on client %d from p %d failed: %s",
			  c->fd, l->fd, strerror(errno));
		c->close(ctx, c);
		return status;
	}

	/*All client connections are closed after exec*/
	status = fcntl(c->fd, F_SETFD, FD_CLOEXEC);
	if (status < 0) {
		log_error("fcntl FD_CLOEXEC on c %d from p %d failed: %s",
			  c->fd, l->fd, strerror(errno));
		c->close(ctx, c);
		return status;
	}

	/*
	 * Disable Nagle algorithm for ipv4 and ipv6 protocols
	 */
	if (l->family == AF_INET || l->family == AF_INET6) {
		status = set_tcpnodelay(c->fd);
		if (status < 0) {
			log_error(
				"set tcpnodelay on client %d from listener %d failed, ignored: %s",
				c->fd, l->fd, strerror(errno));
		}
	}

	stats_pool_incr(ctx, c->owner, client_connections);
	status = event_add_conn(ctx->evb, c);
	if (status < 0) {
		log_error("event add conn from client %d failed: %s", c->fd,
			  strerror(errno));
		c->close(ctx, c);
		return status;
	}
	// set connected tag for client conn
	c->connected = 1;

	log_debug("accepted client %d on listener %d", c->fd, l->fd);

	/* send mysql server welcome info after tcp connected. */
	struct msg *smsg;
	if (c->writecached == 0 && c->connected == 1) {
		c->stage = CONN_STAGE_LOGGING_IN;

		smsg = msg_get(c, true);
		if (smsg == NULL) {
			c->error = 1;
			c->err = CONN_MSG_GET_ERR;
			return -1;
		}
		status = net_send_server_greeting(c, smsg);
		if (status < 0) {
			log_error("server greeting info build error:%d",
				  status);
			c->error = 1;
			c->err = CONN_MSG_GET_ERR;
			msg_put(smsg);
			return -1;
		}

		if (c->writecached == 0 && c->connected == 1) {
			log_debug("writecached & connected");
			cache_send_event(c);
		}
	}
	c->enqueue_outq(ctx, c, smsg);

	return 0;
}

int listener_recv(struct context *ctx, struct conn *conn)
{
	int status;

	ASSERT(conn->type & LISTENER);
	ASSERT(conn->flag & RECV_ACTIVE);

	conn->flag |= RECV_READY;
	do {
		status = listener_accept(ctx, conn);
		if (status != 0) {
			return status;
		}
	} while (conn->flag & RECV_READY);

	return 0;
}
