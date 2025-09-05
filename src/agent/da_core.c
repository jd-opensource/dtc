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

#include <sys/resource.h>
#include <inttypes.h>
#include <fcntl.h>
#include "da_core.h"
#include "da_event.h"
#include "da_conn.h"
#include "da_conf.h"
#include "da_server.h"
#include "da_listener.h"
#include "da_request.h"
#include "da_time.h"
#include "da_signal.h"
#include "da_stats.h"

static enum core_status inst_status = NORMAL;
static uint32_t ctx_id; /* context generation */
int write_send_queue_len = 0;
struct conn **wait_send_queue; /*conn*/

void cache_send_event(struct conn *conn) {
	struct context * ctx = conn_to_ctx(conn);

	if(write_send_queue_len < ctx->sum_nconn) {
		wait_send_queue[write_send_queue_len] = conn;
		conn->writecached = 1;
		++write_send_queue_len;
	}
}

/*
 * Calculate available frontend connections based on available connections and backend connection count
 */
static int core_calc_connections(struct context *ctx) {
	int status;
	struct rlimit limit;

	status = getrlimit(RLIMIT_NOFILE, &limit);
	if (status < 0) {
		log_error("getrlimit failed: %s", strerror(errno));
		return -1;
	}

	ctx->max_nfd = (uint32_t) limit.rlim_cur;
	ctx->max_ncconn = ctx->max_nfd - ctx->max_nsconn - RESERVED_FDS;
	log_debug(
			"max fds %"PRIu32" max client conns %"PRIu32" " "max server conns %"PRIu32"",
			ctx->max_nfd, ctx->max_ncconn, ctx->max_nsconn);

	return 0;
}

static struct context *core_ctx_create(struct instance *dai) {
	int status;
	struct context *ctx;

	ctx = malloc(sizeof(*ctx));
	if (ctx == NULL) {
		return NULL;
	}
	ctx->id = ++ctx_id;
	ctx->cf = NULL;
	ctx->evb = NULL;
	array_null(&ctx->pool);
	ctx->max_timeout = dai->event_max_timeout;
	ctx->timeout = ctx->max_timeout;
	ctx->max_nfd = 0;
	ctx->max_ncconn = 0;
	ctx->max_nsconn = 0;
	ctx->sum_nconn = 0;

	/* parse and create configuration */
	ctx->cf = conf_create(dai->conf_filename);
	if (ctx->cf == NULL) {
		free(ctx);
		return NULL;
	}

	/* initialize server pool from configuration */
	status = server_pool_init(&ctx->pool, &ctx->cf->pool, ctx);
	if (status != 0) {
		conf_destroy(ctx->cf);
		free(ctx);
		return NULL;
	}

	
	/*
	 * Get rlimit and calculate max client connections after we have
	 * calculated max server connections
	 */
	status = core_calc_connections(ctx);
	if (status != 0) {
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		free(ctx);
		return NULL;
	}

	/* create stats per server pool */
	ctx->stats = stats_create(dai->stats_interval, ctx->cf->localip, &ctx->pool);
	if (ctx->stats == NULL) {
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		free(ctx);
		return NULL;
	}

	/* initialize event handling for client, proxy and server */
	ctx->evb = event_base_create(EVENT_SIZE, &core_core);
	if (ctx->evb == NULL) {
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		free(ctx);
		return NULL;
	}

	
	/* preconnect? servers in server pool */
	status = server_pool_preconnect(ctx);
	if (status != 0) {
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		free(ctx);
		return NULL;
	}
	
	/* initialize listener per server pool */
	status = listener_init(ctx);
	if (status != 0) {
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		free(ctx);
		return NULL;
	}

	log_debug("context sum_nconn:%d", ctx->sum_nconn);
	wait_send_queue = malloc(ctx->sum_nconn*sizeof(struct conn *));
	if(NULL == wait_send_queue) {
		listener_deinit(ctx);
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		free(ctx);
		return NULL;
	}

	log_debug("created ctx %p id %"PRIu32"", ctx, ctx->id);

	return ctx;
}

struct context *core_start(struct instance *dai) {
	struct context *ctx;

	mbuf_init(dai);
	msg_init();
	conn_init();

	ctx = core_ctx_create(dai);
	if (ctx != NULL) {
		dai->ctx = ctx;
		return ctx;
	}

	conn_deinit();
	msg_deinit();
	mbuf_deinit();

	return NULL;
}

static void core_ctx_destroy(struct context *ctx) {
	log_debug("destroy ctx %p id %"PRIu32"", ctx, ctx->id);
	listener_deinit(ctx);
	server_pool_disconnect(ctx);
	event_base_destroy(ctx->evb);
	stats_destroy(ctx->stats);
	server_pool_deinit(&ctx->pool);
	conf_destroy(ctx->cf);
	free(wait_send_queue);
	free(ctx);
}

void core_stop(struct context *dai) {
	core_ctx_destroy(dai);
	conn_deinit();
	msg_deinit();
	mbuf_deinit();
}

static int core_recv(struct context *ctx, struct conn *conn) {
	int status;
	status = conn->recv(ctx, conn);
	if (status != 0) {
		log_error("recv on %d failed: %s", conn->fd, strerror(errno));
	}
	return status;
}

static int core_send(struct context *ctx, struct conn *conn) {
	int status;

	status = conn->send(ctx, conn);
	if (status != 0) {
		log_error("send on %d failed: status: %d errno: %d %s", conn->fd,
				status, errno, strerror(errno));
	}
	return status;
}

static void core_close(struct context *ctx, struct conn *conn) {
	int status;
	char *type, *addrstr;

	ASSERT(conn->sd > 0);

	if (conn->type & FRONTWORK) {
		type = "frontwork";
		addrstr = da_unresolve_peer_desc(conn->fd);
	} else {
		type = conn->type & BACKWORK ? "backwork" : "listener";
		addrstr = da_unresolve_addr(conn->addr, conn->addrlen);
	}

	log_debug(
		"close %s %d '%s' on event %04"PRIX32" eof %d done " "%d rb %zu sb %zu%c %s",
		type, conn->fd, addrstr, conn->events, conn->eof, conn->done,
		conn->recv_bytes, conn->send_bytes, conn->err ? ':' : ' ',
		conn->err ? strerror(conn->err) : "");

	status = event_del_conn(ctx->evb, conn);
	if (status < 0) {
		log_warning("event del conn %s %d failed, ignored: %s", type, conn->fd,
				strerror(errno));
	}
	conn->close(ctx, conn);
}

static void core_error(struct context *ctx, struct conn *conn) {
	int status;
	char *type =
			conn->type & FRONTWORK ?
					"frontwork" :
					(conn->type & BACKWORK ? "backwork" : "listener");

	status = get_soerror(conn->fd);
	if (status < 0) {
		log_warning("get soerr on %s %d failed, ignored: %s", type, conn->fd,
				strerror(errno));
	}
	conn->err = errno;
	core_close(ctx, conn);
}

int core_core(void *arg, uint32_t events) {
	int status;
	struct conn *conn = arg;
	struct context *ctx = conn_to_ctx(conn);
	conn->events = events;
	log_debug("enter core_core,fd:%d,events :%d conn_type %d", conn->fd, events,conn->type);
	if (events & EVENT_ERR) {
		core_error(ctx, conn);
		return -1;
	}
	if (events & EVENT_READ) {
		status = core_recv(ctx, conn);
		if (status != 0 || conn->done || conn->err) {
			core_close(ctx, conn);
			return -1;
		}
	}
	if (events & EVENT_WRITE) {
		status = core_send(ctx, conn);
		if (status < 0 || conn->done || conn->err) {
			core_close(ctx, conn);
			return -1;
		}
	}
	return 0;
}

/*
 * reclaim msg from server and client q
 */
static void reclaim_msg(struct context *ctx, struct msg *msg) {
	struct conn *c_conn, *s_conn;

	c_conn = msg->owner;
	s_conn = msg->peer_conn;

	if (msg->cli_inq) {
		c_conn->dequeue_inq(ctx, c_conn, msg);
	}
	if (msg->cli_outq) {
		c_conn->dequeue_outq(ctx, c_conn, msg);
	}
	if (msg->sev_inq) {
		s_conn->dequeue_inq(ctx, s_conn, msg);
	}
	if (msg->sev_msgtree) {
		s_conn->de_msgtree(ctx, s_conn, msg);
	}
	req_put(msg);
	return;
}

/*
 * reclaim timeout msg
 */
static void reclaim_timeout_msg(struct context *ctx, struct msg *msg) {
	struct conn *c_conn;
	struct conn *s_conn;

	c_conn = (struct conn *) msg->owner;
	s_conn = (struct conn *) msg->peer_conn;

	reclaim_msg(ctx, msg);
	return;
}

static void reclaim_sending_msg(struct context *ctx, struct msg *msg) {
	struct conn *c_conn, *s_conn;

	c_conn = msg->owner;
	s_conn = msg->peer_conn;

	if (msg->cli_inq) {
		c_conn->dequeue_inq(ctx, c_conn, msg);
	}
	if (msg->cli_outq) {
		c_conn->dequeue_outq(ctx, c_conn, msg);
	}
	msg->swallow = 1;
}

/*
 * timeout process,not close the connection
 */
static void core_timeout(struct context *ctx) {
	for (;;) {
		struct msg *msg;
		struct conn *conn;
		uint64_t then;

		msg = msg_tmo_min();
		if (msg == NULL) {
			//set epoll wait time
			ctx->timeout = ctx->max_timeout;
			return;
		}
		if (msg->error | msg->done) {
			msg_tmo_delete(msg);
			continue;
		}
		conn = msg->tmo_rbe.data;
		then = msg->tmo_rbe.key;
		if (now_ms < then) {
			int delta = (int) (then - now_ms);
			ctx->timeout = MIN(delta, ctx->max_timeout);
			return;
		}

		log_error("req %"PRIu64" on s %d timedout,fragment:%"PRIu32".", msg->id,
				conn->fd, msg->nfrag);
		
		//if(msg->swallow == 0)
		//{
			/* count the elapse time */
		//    stats_pool_incr_by(ctx, conn->owner, pool_elaspe_time, now_us - msg->start_ts);
		//}

		//reclaim time out msg
		if(!msg->sending)
			reclaim_timeout_msg(ctx, msg);
		else{
			reclaim_sending_msg(ctx, msg);
			msg_tmo_delete(msg);
			msg_tmo_insert(msg, conn);
		}
	}
	return;
}

static void process_cached_write_event(struct context *ctx) {
	int i, status;
	struct conn *conn;
	struct cache_instance *ci;
	for (i = 0; i < write_send_queue_len; i++) {
		conn = wait_send_queue[i];
		ci = conn->owner;
		core_send(ctx, conn);
		conn->writecached = 0;	
		if((((conn->type & FRONTWORK) && !TAILQ_EMPTY(&conn->omsg_q)) ||
			((conn->type & BACKWORK) && !TAILQ_EMPTY(&conn->imsg_q)))
			&& conn->connected && !(conn->done || conn -> error)){ 
			status = event_add_out(ctx->evb, conn);			
			if (status < 0) {
				conn->error = 1;
				conn->err = CONN_EPOLLCTL_ERR;
			}
		}
	}
	write_send_queue_len = 0;
	return;
}

int core_loop(struct context *ctx) {
	int nsd;
	signal_process_queue();

	nsd = event_wait(ctx->evb, ctx->timeout);
	if (nsd < 0) {
		return nsd;
	}
	process_cached_write_event(ctx);
	core_timeout(ctx);
	stats_swap(ctx->stats);
	return 0;
}

int core_exec_new_binary(struct instance *dai) {
	int32_t size, len;
	uint32_t i;
	char *envp[] = { NULL, NULL };
	char *fds = NULL;
	struct context *ctx = dai->ctx;
	struct array *pool = &(ctx->pool);
	/*
	 * 1. fork
	 */
	int pid = fork();

	switch (pid) {
	case -1:
		log_error("fork in core_exec_new_binary got error");
		return -1;

	case 0: /* child */
		break;

	default: /* parent */
		return 0;
	}

	/* this is in child if we got here*/
	/*
	 * 2. put all listen fds to NC_ENV_FDS:
	 * NC_ENV_FDS=4;5;10;12;
	 */
	size = (int32_t) (sizeof(NC_ENV_FDS)
			+ (array_n(pool)) * (1 + DA_UINT32_MAXLEN));
	len = 0;

	fds = malloc(size);
	if (fds == NULL) {
		return -1;
	}
	len += da_scnprintf(fds + len, size - len, NC_ENV_FDS "=");
	//len += nc_scnprintf(fds + len, size - len, "%u;", ctx->stats->sd);

	for (i = 0; i < array_n(pool); i++) {
		struct server_pool *p = array_get(pool, i);
		int fd = p->listener->fd;
		if (fd <= 0) {
			continue;
		}
		len += da_scnprintf(fds + len, size - len, "%u;", fd);
	}
	fds[len] = '\0';

	log_debug("exec new binary with env: %s", fds);

	/*
	 * 3. exec,set envp
	 */
	envp[0] = fds;
	execve(dai->argv[0], dai->argv, envp);

	return 0;
}

int core_inherited_socket(char *listen_address) {
	int sock = 0;
	char *inherited;
	char *p, *q;
	/* we will use nc_unresolve_desc and overwrite input listen_address */
	char address[NI_MAXHOST + NI_MAXSERV];

	inherited = getenv(NC_ENV_FDS);
	if (inherited == NULL) {
		/* not found */
		return 0;
	}

	strncpy(address, listen_address, sizeof(address));

	log_debug("trying to get inherited socket '%s' from '%s'", address,
			inherited);

	for (p = inherited, q = inherited; *p; p++) {
		if (*p == ';') {
			sock = da_atoi(q, p - q);
			if (strcmp(address, da_unresolve_desc(sock)) == 0) {
				log_debug("get inherited socket %d for '%s' from '%s'", sock,
						address, inherited);
				sock = dup(sock);
				log_debug("dup inherited socket as %d", sock);
				return sock;
			}
			q = p + 1;
		}
	}
	log_debug("can not inherited socket '%s'", address);
	return 0;
}

void core_cleanup_inherited_socket(void) {
	int sock = 0;
	char *inherited;
	char *p, *q;

	inherited = getenv(NC_ENV_FDS);
	if (inherited == NULL) {
		return;
	}

	for (p = inherited, q = inherited; *p; p++) {
		if (*p == ';') {
			sock = da_atoi(q, p - q);
			close(sock);
			q = p + 1;
		}
	}
}

void core_setinst_status(enum core_status status)
{
	inst_status = status;
}

enum core_status core_getinst_status()
{
	return inst_status;
}
