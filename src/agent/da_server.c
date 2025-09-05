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
#include "da_server.h"
#include "da_hashkit.h"
#include "da_errno.h"
#include "da_log.h"
#include "da_event.h"
#include "da_request.h"
#include "da_response.h"
#include "da_core.h"
#include "da_conf.h"
#include "da_stats.h"
#include "da_time.h"

static int keep_alive = 1;   // Enable keepalive attribute. Default: 0(disabled)  
static int keep_idle = 5;   // If there is no data interaction within 60 seconds, probe. Default: 7200(s)  
static int keep_interval = 5;   // Time interval for sending probe packets during probing is 5 seconds. Default: 75(s)  
static int keep_count = 1;   // Number of probe retries. Connection is considered failed if all timeout. Default: 9(times)  


void instance_ref(struct conn *conn, void *owner) {
	struct cache_instance *ins = owner;

	ASSERT(conn->type & BACKWORK);
	ASSERT(conn->owner == NULL);

	conn->family = ins->family;
	conn->addr = ins->addr;
	conn->addrlen = ins->addrlen;
	conn->owner = owner;

	ins->ns_conn_q++;
	TAILQ_INSERT_TAIL(&ins->s_conn_q, conn, conn_tqe);

	log_debug("ref conn %p owner %p into '%.*s", conn, ins, ins->pname.len,ins->pname.data);
}

void instance_unref(struct conn *conn) {
	struct cache_instance *ins = conn->owner;
	

	ASSERT(conn->type & BACKWORK);
	ASSERT(conn->owner == NULL);

	conn->owner = NULL;

	
	ASSERT(server->ns_conn_q != 0);
	ins->ns_conn_q--;
	TAILQ_REMOVE(&ins->s_conn_q, conn, conn_tqe);

	log_debug("unref conn %p owner %p from '%.*s'", conn, ins, ins->pname.len, ins->pname.data);
}

int server_active(struct conn *conn) {

	ASSERT(conn->type & BACKWORK);
	if (!TAILQ_EMPTY(&conn->imsg_q)) {
		log_debug("s %d is active", conn->fd);
		return true;
	}

	if (rbtree_min(&conn->msg_tree) != NULL) {
		log_debug("s %d is active", conn->fd);
		return true;
	}

	if (conn->rmsg != NULL) {
		log_debug("s %d is active", conn->fd);
		return true;
	}

	if (conn->smsg != NULL) {
		log_debug("s %d is active", conn->fd);
		return true;
	}

	log_debug("s %d is inactive", conn->fd);

	return false;
}

static uint32_t server_pool_hash(struct server_pool *pool, uint8_t *key,
		uint32_t keylen) {
	ASSERT(array_n(&pool->server) != 0);

	if (array_n(&pool->server) == 1) {
		return 0;
	}

	ASSERT(key != NULL && keylen != 0);

	return pool->key_hash((char *) key, keylen);
}

uint32_t server_pool_idx(struct server_pool *pool, uint8_t *key,
		uint32_t keylen) {
	ASSERT(array_n(&pool->server) != 0);
	ASSERT(key != NULL && keylen != 0);
	uint32_t hash, idx;
	
	hash = server_pool_hash(pool, key, keylen);
	idx = ketama_dispatch(pool->continuum, pool->ncontinuum, hash);

	log_debug("server_pool_idx keylen:%d, key:%p, server num: %d, idx: %d\n", keylen, key, array_n(&pool->server), idx);
	ASSERT(idx < array_n(&pool->server));
	return idx;
}

static struct server *server_pool_server(struct server_pool *pool,
		struct msg *msg) {
	struct server *server;
	uint32_t idx;

	idx = msg->idx;
	log_debug("server pool idx: %d", idx);
	server = array_get(&pool->server, idx);

	return server;
}

static int instance_connect(struct context *ctx, struct cache_instance *instance,
		struct conn *conn) {

	ASSERT(ctx != NULL);
	ASSERT(server != NULL);
	ASSERT(conn != NULL);

	int status;
	if (conn->fd > 0) {
		return 0;
	}
	log_debug("connect to server '%.*s' family:%d", instance->pname.len, instance->pname.data, conn->family);
	conn->fd = socket(conn->family, SOCK_STREAM, 0);
	if (conn->fd < 0) {
		log_error("socket for server '%s' failed: %s", instance->addr->sa_data, strerror(errno));
		status = CONN_CREATSOCK_ERR;
		goto conn_error;
	}

	status = fcntl(conn->fd, F_SETFD, FD_CLOEXEC);
	if (status < 0) {
		log_error("fcntl FD_CLOEXEC on s %d for server '%.*s' failed: %s",
				conn->fd, instance->pname.len,instance->pname.data, strerror(errno));
		goto conn_error;
	}

	status = set_nonblocking(conn->fd);
	if (status < 0) {
		log_error("set nonblock on s %d for server '%.*s' failed: %s", conn->fd,
			instance->pname.len, instance->pname.data, strerror(errno));
		status = CONN_SETNOBLOCK_ERR;
		goto conn_error;
	}

	//if (instance->pname.data[0] != '/') {
		status = set_tcpnodelay(conn->fd);
		if (status != 0) {
			log_warning(
					"set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
					conn->fd, instance->pname.len, instance->pname.data,
					strerror(errno));
		}
	//}
	status = event_add_conn(ctx->evb, conn);
	if (status < 0) {
		log_error("event add conn s %d for server '%.*s' failed: %s", conn->fd,
			instance->pname.len, instance->pname.data, strerror(errno));
		status = CONN_EPOLLADD_ERR;
		goto conn_error;
	}

	status = connect(conn->fd, conn->addr, conn->addrlen);
	if (status != 0) {
		if (errno == EINPROGRESS) {
			conn->connecting = 1;
			log_debug("connecting on s %d to server '%.*s'", conn->fd,
				instance->pname.len, instance->pname.data);
			return 0;
		}

		log_error("connect on s %d to server '%.*s' failed: %s", conn->fd,
			instance->pname.len, instance->pname.data, strerror(errno));
		status = CONN_CONNECT_ERR;
		goto conn_error;
	}

	struct server *server;
	struct server_pool *pool;

	server = instance->owner;
	pool = server->owner;
	keep_alive = pool->auto_remove_replica ? 1 : 0;

	status = setsockopt(conn->fd, SOL_SOCKET, SO_KEEPALIVE, (void*)&keep_alive, sizeof(keep_alive));
	status = setsockopt(conn->fd, SOL_TCP, TCP_KEEPIDLE, (void*)&keep_idle, sizeof(keep_idle));
	status = setsockopt(conn->fd, SOL_TCP, TCP_KEEPINTVL, (void*)&keep_interval, sizeof(keep_interval));
	status = setsockopt(conn->fd, SOL_TCP, TCP_KEEPCNT, (void*)&keep_count, sizeof(keep_count));
	if (status < 0)
	{
		goto conn_error;
	}

	ASSERT(conn->connecting ==0 && conn->connected ==0);
	conn->connected = 1;
	log_debug("connected on s %d to server '%.*s'", conn->fd, instance->pname.len, instance->pname.data);

	return 0;

	conn_error:
	conn->error = 1;
	conn->err = status;
	return -1;
}

static struct conn *instance_conn(struct cache_instance *ins){
	struct server_pool *pool;
	struct server *server;
	struct conn *conn;
	server = ins->owner;	
	pool = server->owner;
	

	/*
	 * FIXME: handle multiple server connections per server and do load
	 * balancing on it. Support multiple algorithms for
	 * 'server_connections:' > 0 key
	 */
	
	if (ins->ns_conn_q < pool->server_connections) {
		conn =  get_instance_conn(ins);	
		return conn;
	}
	ASSERT(server->ns_conn_q == pool->server_connections);
	/*
	 * Pick a server connection from the head of the queue and insert
	 * it back into the tail of queue to maintain the lru order
	 */

	conn = TAILQ_FIRST(&ins->s_conn_q);
	ASSERT(!conn->client && !conn->proxy);

	TAILQ_REMOVE(&ins->s_conn_q, conn, conn_tqe);
	TAILQ_INSERT_TAIL(&ins->s_conn_q, conn, conn_tqe);

	return conn;
}

struct cache_instance *get_instance_from_array(struct array replica_array, uint16_t *array_idx, uint16_t *cnt)
{
	int i, idx = 0,w,t;
	int nreplica = array_n(&replica_array);
	struct cache_instance *ci = NULL;
	if (nreplica != 0)
	{
		for (i = 0; i < nreplica + 1; i++) {
			idx = (i + (*array_idx) + nreplica) % nreplica;
			ci = array_get(&replica_array, idx);
			w = ci->weight;
			if ((*cnt) < w && ci->nerr < ci->ns_conn_q) {
				(*cnt)++;
				*array_idx = idx;
				//printf("FFF i :%.*s nerr%d failtime%d  \n", ci->pname.len, ci->pname.data, ci->nerr, ci->failure_num);
				return ci;
			}
			else if (ci->nerr >= ci->ns_conn_q && ci->failure_num < FAIL_TIME_LIMIT) {
				t = 1;
				t = t << ci->failure_num;
				t = t * 1000;
				printf("i :%.*s now_ms%"PRIu64" failtime%"PRIu64"  t%d \n", ci->pname.len, ci->pname.data, now_ms, ci->last_failure_ms, t);
				if ((now_ms - ci->last_failure_ms) > t) {
					(*cnt) = 0;
					(*array_idx) = idx + 1;
				
					ci->nerr = 0;
					return ci;
				}
				else {
					(*cnt) = 0;
					continue;
				}

			}
			else {
				(*cnt) = 0;
				continue;
			}

		}
		(*array_idx) = idx;
	}
	return ci;
}

static struct cache_instance *get_instance_from_server(struct server *server) {
	struct cache_instance *ci = NULL;
	ci = get_instance_from_array(server->high_ptry_ins,&server->high_prty_idx, &server->high_prty_cnt);
	if (ci == NULL)
	{
		ci = get_instance_from_array(server->low_prty_ins, &server->low_prty_idx, &server->low_prty_cnt);
	}
	return ci;
}

struct conn *server_pool_conn(struct context *ctx, struct server_pool *pool, 
		struct msg *msg) {
	int status;
	struct server *server;
	struct conn *conn = NULL;
	struct cache_instance *ci;

	/* from a given {key, keylen} pick a server from pool */
	server = server_pool_server(pool, msg);
	if (server == NULL) {
		return NULL;
	}

	

	// if write cmd always send to master or replica is disable in sys 
	// always forward to master instance
	if (msg->cmd != MSG_REQ_GET || pool->replica_enable == 0)
	{
		conn = instance_conn(server->master);
		if (conn == NULL) {
			return NULL;
		}
		status = instance_connect(ctx, server->master, conn);
		if (status != 0) {
			log_error("instance connect failed, close server.");
			server_close(ctx, conn);
			return NULL;
		}
		ci = conn->owner;

		return conn;
	}
	else
	{
		ci = get_instance_from_server(server);
		if (ci == NULL){
			log_error("No machine is normal");
			return NULL;
		}
		ci->num++;
		conn = instance_conn(ci);
		if (conn == NULL) {
			return NULL;
		}
		status = instance_connect(ctx, ci, conn);
		if (status != 0) {
			log_error("instance connect failed, close server.");
			server_close(ctx, conn);
			return NULL;
		}

		return conn;
	}
	
	return conn;
}

void server_connected(struct context *ctx, struct conn *conn) {
	struct cache_instance *ins = conn->owner;

	ASSERT(conn->type & BACKWORK);
	ASSERT(conn->connecting && !conn->connected);

	stats_server_incr(ctx, ins, server_connections);
	conn->connecting = 0;
	conn->connected = 1;

	log_debug("connected on s %d to server '%.*s'", conn->fd, ins->pname.len, ins->pname.data);
}

static void
server_close_stats(struct context *ctx, struct cache_instance *ci, int err,
                   unsigned eof, unsigned connected)
{
	if (connected) {
		stats_server_decr(ctx, ci, server_connections);
	}
	if (eof) {
		stats_server_incr(ctx, ci, server_eof);
	    return;
	}
	if(err)
	{
		stats_server_incr(ctx, ci, server_err);
	}
	return;
}

void server_close(struct context *ctx, struct conn *conn) {
	int status;
	struct msg *msg, *nmsg;
	struct conn *c_conn; /* peer client connection */
	struct rbnode *node, *nnode;
	struct cache_instance *ci;
	//TODO
	ci = conn->owner;
	server_close_stats(ctx, ci, conn->err, conn->eof,
	                       conn->connected);

	
	if (conn->fd < 0) { /*has been closed*/
		conn->unref(conn);
		conn_put(conn);
		return;
	}
	
	struct server *server;
	struct server_pool *pool;
	server = ci->owner;
	pool = server->owner;
	if (pool->auto_remove_replica){
		incr_instance_failure_time(ci);
	}
	

	for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {

		nmsg = TAILQ_NEXT(msg, s_i_tqe);

		conn->dequeue_inq(ctx, conn, msg);
		/*
		 * client close connection,msg can be swallow
		 */
		if (msg->swallow) {
			log_debug("close s %d swallow req %"PRIu64" len %"PRIu32 " type %d",
					conn->fd, msg->id, msg->mlen, msg->cmd);
			msg->done = 1;
			req_put(msg);
		} else {
			c_conn = msg->owner;
			msg->done = 1;
			msg->error = 1;
			msg->err = MSG_BACKWORKER_ERR;
			ci = conn->owner;
			stats_server_incr(ctx, ci, server_request_error);

			if (msg->frag_owner != NULL) {
				msg->frag_owner->nfrag_done++;
			}
			//if req is done , call req_forward
			if (req_done(c_conn, msg)) {
				rsp_forward(ctx, c_conn, msg);
			}
		}
	}

	ASSERT(TAILQ_EMPTY(&conn->imsg_q));

	for (node = rbtree_min(&conn->msg_tree); node != NULL; node = nnode) {

		msg = (struct msg *) node->data;
		conn->de_msgtree(ctx, conn, msg);

		nnode = rbtree_min(&conn->msg_tree);

		if (msg->swallow) {
			log_debug("close s %d swallow req %"PRIu64" len %"PRIu32 " type %d",
					conn->fd, msg->id, msg->mlen, msg->cmd);
			req_put(msg);
		} else {
			c_conn = msg->owner;
			msg->done = 1;
			msg->error = 1;
			msg->err = MSG_BACKWORKER_ERR;
			//TODO
			ci = conn->owner;
			stats_server_incr(ctx, ci, server_request_error);

			if (msg->frag_owner != NULL) {
				msg->frag_owner->nfrag_done++;
			}

			//if req is done , call req_forward
			if (req_done(c_conn, msg)) {
				rsp_forward(ctx, c_conn, msg);
			}
		}
	}
	msg = conn->rmsg;
	if (msg != NULL) {
		conn->rmsg = NULL;

		ASSERT(!msg->request);ASSERT(msg->peer == NULL);

		rsp_put(msg);

		log_debug(
				"close s %d discarding rsp %"PRIu64" len %"PRIu32" " "in error",
				conn->fd, msg->id, msg->mlen);
	}
	conn->unref(conn);
	status = close(conn->fd);
	if (status < 0) {
		log_error("close s %d failed, ignored: %s", conn->fd, strerror(errno));
	}
	conn->fd = -1;
	conn_put(conn);
	return;
}

static int server_each_set_owner(void *elem, void *data) {
	struct server *s = elem;
	struct server_pool *sp = data;

	s->owner = sp;

	return 0;
}

/*
 * init server
 */
int server_init(struct array *server, struct array *conf_server,
		struct server_pool *sp) {

	int status;
	uint32_t nserver;
	nserver = array_n(conf_server);

	ASSERT(nserver != 0);
	ASSERT(array_n(server) == 0);

	status = array_init(server, nserver, sizeof(struct server));
	if (status != 0) {
		return status;
	}

	status = array_each(conf_server, conf_server_each_transform, server);
	if (status != 0) {
		//server_deinit(server);
		return status;
	}

	ASSERT(array_n(server) == nserver);

	status = array_each(server, server_each_set_owner, sp);
	if (status != 0) {
		//server_deinit(server);
		return status;
	}

	log_debug("init %"PRIu32" servers in pool %"PRIu32" '%.*s'", nserver,
			sp->idx, sp->name.len, sp->name.data);
	return 0;
}


void instance_deinit(struct array *instance) {
	uint32_t i, nserver;
	for (i = 0, nserver = array_n(instance); i < nserver; i++) {
		struct cache_instance *ci;
		ci = array_pop(instance);
		printf("ip : %.*s, num : %d, fail_time : %"PRIu64"\n", ci->pname.len, ci->pname.data, ci->num, ci->last_failure_ms);
		string_deinit(&ci->pname);
		ASSERT(TAILQ_EMPTY(&ci->s_conn_q) && ci->ns_conn_q == 0);
	}
	array_deinit(instance);
}


void server_deinit(struct array *server) {
	uint32_t i, nserver;

	for (i = 0, nserver = array_n(server); i < nserver; i++) {
		struct server *s;	
		s = array_pop(server);
		string_deinit(&s->name);
		printf("\n first r\n");
		instance_deinit(&s->high_ptry_ins);
		printf("\n second r\n");
		instance_deinit(&s->low_prty_ins);
		
	}
	array_deinit(server);
}

static int server_each_preconnect(void *elem, void *data) {
	int status;
	struct server *server;
	struct server_pool *pool;
	struct conn *conn;
	int ninstance,i;
	struct cache_instance *ins;

	server = elem;
	pool = server->owner;

	conn = instance_conn(server->master);
	if (conn == NULL) {
		return -1;
	}
	status = instance_connect(pool->ctx, server->master, conn);
	if (status != 0) {
		log_warning("connect to server '%.*s' failed, ignored: %s",
			server->master->pname.len, server->master->pname.data, strerror(errno));
		server_close(pool->ctx, conn);
	}

	
	if (pool->replica_enable) // if enable replica, connect all instance
	{
		ninstance = array_n(&server->high_ptry_ins);
		for (i = 0; i < ninstance; i++)
		{	
			ins = array_get(&server->high_ptry_ins, i);	
			if (ins == server->master) continue;
			conn = instance_conn(ins);
			if (conn == NULL) {		
				return -1;
			}			
			status = instance_connect(pool->ctx, ins, conn);
			if (status != 0) {
				log_warning("connect to server '%.*s' failed, ignored: %s",
					ins->pname.len,ins->pname.data, strerror(errno));
				server_close(pool->ctx, conn);
			}
		}

		ninstance = array_n(&server->low_prty_ins);
		for (i = 0; i < ninstance; i++)
		{
			ins = array_get(&server->low_prty_ins, i);
			conn = instance_conn(ins);
			if (conn == NULL) {
				return -1;
			}
			status = instance_connect(pool->ctx, ins, conn);
			if (status != 0) {
				log_warning("connect to server '%.*s' failed, ignored: %s",
					ins->pname.len, ins->pname.data, strerror(errno));
				server_close(pool->ctx, conn);
			}
		}
	}
	//else// only connect master
	//{
	//	conn = instance_conn(server->master);
	//	if (conn == NULL) {
	//		return -1;
	//	}
	//	status = instance_connect(pool->ctx, server->master, conn);
	//	if (status != 0) {
	//		log_warning("connect to server '%.*s' failed, ignored: %s",
	//			server->master->pname.len, server->master->pname.data, strerror(errno));
	//		server_close(pool->ctx, conn);
	//	}

	//}
	

	return 0;
}

static int instance_disconnect(struct array *elem, void *data)
{
	struct cache_instance *ins;
	struct server_pool *pool = data;
	int i, nrepilca;
	nrepilca = array_n(elem);
	for ( i = 0; i < nrepilca; i++) {
		ins = array_get(elem, i);
		while (!TAILQ_EMPTY(&ins->s_conn_q)) {
			struct conn *conn;
			conn = TAILQ_FIRST(&ins->s_conn_q);
			ins = conn->owner;
			conn->close(pool->ctx, conn);
			log_debug("close conn : %d in server '%.*s'", conn->fd,
				ins->pname.len, ins->pname.data);
		}
	}
	return 0;
}

static int server_each_disconnect(void *elem, void *data) {
	struct server *server;
	struct server_pool *pool;
	//struct cache_instance *ins;
	server = elem;
	pool = server->owner;

	/*while (!TAILQ_EMPTY(&server->master.s_conn_q)) {
		struct conn *conn;
		conn = TAILQ_FIRST(&server->master.s_conn_q);
		ins = conn->owner;
		conn->close(pool->ctx, conn);
		log_debug("close conn : %d in server '%.*s'", conn->fd,
			server->master.pname.len, server->master.pname.data);
	}*/

	
	instance_disconnect(&server->high_ptry_ins,pool);
	instance_disconnect(&server->low_prty_ins, pool);
	


	return 0;
}

static int server_pool_each_preconnect(void *elem, void *data) {
	int status;
	struct server_pool *sp;

	sp = (struct server_pool *) elem;
	if (!sp->preconnect) {
		return 0;
	}
	status = array_each(&sp->server, server_each_preconnect, NULL);
	if (status != 0) {
		return status;
	}
	return 0;
}

int server_pool_preconnect(struct context *ctx) {
	int status;

	status = array_each(&ctx->pool, server_pool_each_preconnect, NULL);
	if (status != 0) {
		return status;
	}

	return 0;
}

static int server_pool_each_disconnect(void *elem, void *data) {
	int status;
	struct server_pool *sp;
	sp = (struct server_pool *)elem;

	status = array_each(&sp->server, server_each_disconnect, NULL);
	if (status != 0) {
		return status;
	}
	return 0;
}

void server_pool_disconnect(struct context *ctx) {
	array_each(&ctx->pool, server_pool_each_disconnect, NULL);
}

static int server_pool_each_set_owner(void *elem, void *data) {
	struct server_pool *sp = elem;
	struct context *ctx = data;

	sp->ctx = ctx;

	return 0;
}

/*
 * calc the total server connection numbers
 */
static int server_pool_each_calc_connections(void *elem, void *data) {
	struct server_pool *sp = elem;
	struct server *server;
	int ninstance,i;
	struct context *ctx = data;
	ninstance = 0;
	for (i = 0; i < array_n(&sp->server); i++){
		server = array_get(&sp->server, i);
		if (sp->replica_enable) {
			ninstance += array_n(&server->high_ptry_ins);
			ninstance += array_n(&server->low_prty_ins);
		}
	}
	ctx->max_nsconn += sp->server_connections * ninstance;
	ctx->max_nsconn += 1; /* pool listening socket */

	ctx->sum_nconn += sp->client_connections;
	ctx->sum_nconn += sp->server_connections * ninstance;
	ctx->sum_nconn += 1; /* pool listening socket */

	return 0;
}

/*
 * init the ketama structure
 */
int server_pool_run(struct server_pool *pool) {

	ASSERT(array_n(&pool->server) != 0);
	return ketama_update(pool);

}

/*
 * For each server pool, build backend server structure and construct hash ring
 */
static int server_pool_each_run(void *elem, void *data) {
	return server_pool_run(elem);
}



/*
 * Initialize server pool
 */
int server_pool_init(struct array *server_pool, struct array *conf_pool,
		struct context *ctx) {
	int status;
	uint32_t npool;

	npool = array_n(conf_pool);
	ASSERT(npool != 0);
	ASSERT(array_n(server_pool) == 0);

	status = array_init(server_pool, npool, sizeof(struct server_pool));
	if (status != 0) {
		return status;
	}

	/* transform conf pool to server pool */
	// Call conf_pool_each_transform function for each object in conf_pool
	status = array_each(conf_pool, conf_pool_each_transform, server_pool);
	if (status != 0) {
		server_pool_deinit(server_pool);
		return status;
	}ASSERT(array_n(server_pool) == npool);

	/* set ctx as the server pool owner */
	status = array_each(server_pool, server_pool_each_set_owner, ctx);
	if (status != 0) {
		server_pool_deinit(server_pool);
		return status;
	}

	/* compute max server connections */
	ctx->max_nsconn = 0;
	status = array_each(server_pool, server_pool_each_calc_connections, ctx);
	if (status != 0) {
		server_pool_deinit(server_pool);
		return status;
	}

	/* update server pool continuum */
	status = array_each(server_pool, server_pool_each_run, NULL);
	if (status != 0) {
		server_pool_deinit(server_pool);
		return status;
	}

	
	log_debug("init %"PRIu32" pools", npool);

	return 0;
}

static void server_pool_disconnect_client(struct server_pool *pool)
{
	log_debug("disconnect %d clients on pool %"PRIu32" '%.*s'",
			pool->c_conn_count, pool->idx, pool->name.len, pool->name.data);
	while (!TAILQ_EMPTY(&pool->c_conn_q)) {
		struct conn *c = TAILQ_FIRST(&pool->c_conn_q);
		c->close(pool->ctx, c);
	}
}
/*
 * destory server pool
 */
void server_pool_deinit(struct array *server_pool) {
	uint32_t i, npool;

	for (i = 0, npool = array_n(server_pool); i < npool; i++) {
		struct server_pool *sp;

		sp = array_pop(server_pool);

		ASSERT(sp->p_conn == NULL);
		ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->nc_conn_q == 0);

		server_pool_disconnect_client(sp);

		if (sp->continuum != NULL) {
			free(sp->continuum);
			sp->ncontinuum = 0;
			sp->nserver_continuum = 0;
		}

		string_deinit(&sp->accesskey);
		string_deinit(&sp->addrstr);
		string_deinit(&sp->accesskey);
		string_deinit(&sp->module_idc);
		
		server_deinit(&sp->server);

		if(sp->top_percentile_param)
			free(sp->top_percentile_param);

		log_debug("deinit pool %"PRIu32" '%.*s'", sp->idx, sp->name.len,
				sp->name.data);
	}
	array_deinit(server_pool);
}

/*
 * server timeout
 */
int server_timeout(struct conn *conn) {
	struct cache_instance *ins;
	struct server *server;
	struct server_pool *pool;

	ASSERT(!conn->client && !conn->proxy);

	ins = conn->owner;
	server = ins->owner;
	pool = server->owner;
	return pool->timeout;
}
/*decr instance failure time*/
int decr_instance_failure_time(struct msg *msg)
{
	struct cache_instance *ci;
	if (msg->peer_conn == NULL){
		return -1;
	}
	if (msg->peer_conn->type & BACKWORK){
		ci = msg->peer_conn->owner;
		if (ci != NULL){
			
			ci->failure_num = 0;
			ci->nerr = 0;
			return 0;
		}
	}
	return -1;
}


int incr_instance_failure_time(struct cache_instance *ci)
{
		ci->nerr++;
		if (ci->nerr >= ci->ns_conn_q) {
			if (ci->failure_num < FAIL_TIME_LIMIT) {
				ci->failure_num++;
				ci->last_failure_ms = now_ms;
			}
		}
		return 0;
	
}

