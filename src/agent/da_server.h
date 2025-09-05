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

#ifndef DA_SERVER_H_
#define DA_SERVER_H_

#include "da_array.h"
#include "da_conn.h"
#include "da_string.h"
#include <stddef.h>
#include <stdint.h>

typedef uint32_t (*hash_t)(const char *, size_t);
#define ERROR_UPPER_LIMIT 1
#define FAIL_TIME_LIMIT 6

struct continuum {
  uint32_t index; /* server index */
  uint32_t value; /* hash value */
};

struct cache_instance {
  struct string pname;
  void *owner;
  int nerr;
  int idx;
  uint16_t port;            /* port */
  uint32_t weight;          /* weight */
  int family;               /* socket family */
  socklen_t addrlen;        /* socket length */
  struct sockaddr *addr;    /* socket address (ref in conf_server) */
  uint32_t ns_conn_q;       /* # server connection */
  struct conn_tqh s_conn_q; /*server connection*/
  uint64_t last_failure_ms; /*cahche the failure time*/
  uint16_t failure_num;     /*cache failure time*/
  int num;
};

struct server {
  uint32_t idx;              /* server index */
  struct server_pool *owner; /* owner pool */
  uint16_t high_prty_idx;
  uint16_t high_prty_cnt;
  uint16_t low_prty_idx;
  uint16_t low_prty_cnt;
  struct string name; /* name (ref in conf_server) */
  int weight;
  int replica_enable;
  struct cache_instance *master;
  struct array high_ptry_ins;
  struct array low_prty_ins;
};

struct server_pool {
  uint32_t idx;        /* pool index */
  uint32_t mid;        /* pool manager id*/
  struct context *ctx; /* owner context */
  struct conn *listener;
  struct conn_tqh c_conn_q; /*client connection*/
  uint32_t c_conn_count;    /* number of client connection */
  struct array server;      /* server[] */
  uint32_t ncontinuum;      /* # continuum points */
  uint32_t
      nserver_continuum; /* # servers - live and dead on continuum (const) */
  struct continuum *continuum; /* continuum */

  struct string name;          /* pool name (ref in conf_pool) */
  struct string addrstr;       /* pool address (ref in conf_pool) */
  struct string accesskey;     /* access token for this pool */
  uint16_t port;               /* port */
  int family;                  /* socket family */
  socklen_t addrlen;           /* socket length */
  struct sockaddr *addr;       /* socket address (ref in conf_pool) */
  int key_hash_type;           /* key hash type (hash_type_t) */
  hash_t key_hash;             /* key hasher */
  int backlog;                 /* listen backlog */
  int timeout;                 /* timeout in msec */
  uint32_t client_connections; /* maximum # client connection */
  uint32_t server_connections; /* maximum # server connection */
  int replica_enable;          /*Replica enable for whole system*/
  struct string module_idc;    /*sp IDC*/
  unsigned preconnect : 1;     /* preconnect? */
  int main_report;
  int instance_report;
  int auto_remove_replica;

  int top_percentile_enable;
  int top_percentile_fd;
  struct sockaddr_in top_percentile_addr;
  int top_percentile_addr_len;
  struct remote_param *top_percentile_param;
};

uint32_t server_pool_idx(struct server_pool *pool, uint8_t *key,
                         uint32_t keylen);
void server_close(struct context *ctx, struct conn *conn);
void instance_ref(struct conn *conn, void *owner);
void instance_unref(struct conn *conn);
int server_active(struct conn *conn);
int server_init(struct array *server, struct array *conf_server,
                struct server_pool *sp);
void server_connected(struct context *ctx, struct conn *conn);
int server_init(struct array *server, struct array *conf_server,
                struct server_pool *sp);
void instance_deinit(struct array *server);
void server_deinit(struct array *server);
int server_pool_preconnect(struct context *ctx);
void server_pool_disconnect(struct context *ctx);
int server_pool_run(struct server_pool *pool);
void server_pool_deinit(struct array *server_pool);
int server_pool_init(struct array *server_pool, struct array *conf_pool,
                     struct context *ctx);
int server_timeout(struct conn *conn);
struct conn *server_pool_conn(struct context *ctx, struct server_pool *pool,
                              struct msg *msg);
int incr_instance_failure_time(struct cache_instance *ci);
int decr_instance_failure_time(struct msg *msg);
#endif /* DA_SERVER_H_ */
