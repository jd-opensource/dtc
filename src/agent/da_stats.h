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

#ifndef DA_STATS_H_
#define DA_STATS_H_

#include "da_array.h"
#include "da_string.h"

struct event_base;
struct server_pool;
struct context;
struct server;
struct cache_instance;

typedef void (*event_stats_cb_t)(void *, int);

#define STATS_FILE "da.stats"
#define STATS_DIR "../stats/"
#define STATS_INTERVAL (10 * 1000) /* in msec */
#define DEFAULT_HTTP_TIMEOUT                                                   \
  5 /*default heep connect and request timeout in second*/

#define STATS_POOL_CODEC(ACTION)                                               \
  /* client behavior */                                                        \
  ACTION(client_eof, STATS_COUNTER, "# eof on client connections")             \
  ACTION(client_err, STATS_COUNTER, "# errors on client connections")          \
  ACTION(client_connections, STATS_GAUGE, "# active client connections")       \
  /* forwarder behavior */                                                     \
  ACTION(fragment_error, STATS_COUNTER,                                        \
         "# times we encountered a fragment error")                            \
  ACTION(forward_error, STATS_COUNTER,                                         \
         "# times we encountered a forwarding error")                          \
  ACTION(coalesce_error, STATS_COUNTER,                                        \
         "# times we encountered a coalesce error")                            \
  /* data behavior */                                                          \
  ACTION(pool_requests, STATS_COUNTER, "# pool requests")                      \
  ACTION(pool_requests_get, STATS_COUNTER, "# pool requests get")              \
  ACTION(pool_request_bytes, STATS_COUNTER, "  pool total request bytes")      \
  ACTION(pool_responses, STATS_COUNTER, "# pool responses")                    \
  ACTION(pool_response_bytes, STATS_COUNTER, "  pool total response bytes")    \
  ACTION(pool_withoutkey_req, STATS_COUNTER, "# pool without key requests")    \
  ACTION(pool_tech_hit, STATS_COUNTER, "# pool tech hit times")                \
  ACTION(pool_logic_hit, STATS_COUNTER, "# pool logic hit times")              \
  ACTION(pool_elaspe_time, STATS_COUNTER, "# pool elapse time(us)")            \
  ACTION(pool_package_split, STATS_COUNTER, "# pool package split times")      \
  ACTION(pool_request_get_keys, STATS_COUNTER, "# pool get request key count")

#define STATS_SERVER_CODEC(ACTION)                                             \
  /* server behavior */                                                        \
  ACTION(server_eof, STATS_COUNTER, "# eof on server connections")             \
  ACTION(server_err, STATS_COUNTER, "# errors on server connections")          \
  ACTION(server_connections, STATS_GAUGE, "# active server connections")       \
  /* data behavior */                                                          \
  ACTION(server_requests, STATS_COUNTER, "# server requests")                  \
  ACTION(server_request_bytes, STATS_COUNTER, "server total request bytes")    \
  ACTION(server_responses, STATS_COUNTER, "# server responses")                \
  ACTION(server_response_bytes, STATS_COUNTER, "server total response bytes")  \
  ACTION(server_request_error, STATS_COUNTER, "# server requests error")       \
  ACTION(server_in_queue, STATS_GAUGE, "# requests in incoming queue")         \
  ACTION(server_in_tree, STATS_GAUGE, "# requests in backwork search tree")

typedef enum stats_type {
  STATS_INVALID,
  STATS_COUNTER,   /* monotonic accumulator */
  STATS_GAUGE,     /* non-monotonic accumulator */
  STATS_TIMESTAMP, /* monotonic timestamp (in nsec) */
  STATS_SENTINEL
} stats_type_t;

/* struct for flush data to stats file*/
struct _map_item {
  char filename[256];
  void *_map_start;
  int _map_size;
};

struct stats_file_pool_head {
  char poolname[256];
  uint32_t mid;
  uint32_t poolfields;
  uint32_t servernum;
};

struct stats_file_server_head {
  char servername[256];
  uint32_t sid;
  uint32_t serverfields;
};

struct stats_file_item {
  stats_type_t type;
  int64_t stat_once;
  uint64_t stat_all;
};

struct stats_file_server {
  struct stats_file_server_head *shead;
  struct stats_file_item *server_item_list;
};

struct stats_file_pool {
  struct stats_file_pool_head *phead;
  struct stats_file_item *pool_item_list;
  struct array stats_file_servers;
};

struct stats_metric {
  stats_type_t type;  /* type */
  struct string name; /* name (ref) */
  union {
    int64_t counter;   /* accumulating counter */
    int64_t timestamp; /* monotonic timestamp */
  } value;
};

struct stats_server {
  struct string name;  /* server name (ref) */
  struct array metric; /* stats_metric[] for server codec */
};

struct stats_pool {
  uint32_t mid;
  uint16_t port;
  struct string name;  /* pool name (ref) */
  struct array metric; /* stats_metric[] for pool codec */
  struct array server; /* stats_server[] */
  int main_report;
  int instance_report;
};

struct stats {
  int interval;            /* stats aggregation interval */
  uint64_t start_ts;       /* start timestamp of da */
  struct array _map_items; /* _map_item[]		 */
  struct array aggregator; /* stats_file_pool[](b,a+b) */
  struct array current;    /* stats_pool[] (a)*/
  struct array shadow;     /* stats_pool[] (b)*/
  pthread_t tid;           /* stats aggregator thread */
  char localip[16];        /* ip address of this machine */
  volatile int aggregate;  /* shadow (b) aggregate? */
  volatile int updated;    /* current (a) updated? */
};

#define DEFINE_ACTION(_name, _type, _desc) STATS_POOL_##_name,
typedef enum stats_pool_field {
  STATS_POOL_CODEC(DEFINE_ACTION) STATS_POOL_NFIELD
} stats_pool_field_t;
#undef DEFINE_ACTION

#define DEFINE_ACTION(_name, _type, _desc) STATS_SERVER_##_name,
typedef enum stats_server_field {
  STATS_SERVER_CODEC(DEFINE_ACTION) STATS_SERVER_NFIELD
} stats_server_field_t;
#undef DEFINE_ACTION

/* struct for report data to monitor centor*/
struct _ReportParam {
  uint32_t uCType;
  uint32_t uEType;
  int64_t iData1;
  int64_t iData2;
  int64_t iExtra;
  int64_t iDateTime;
  uint32_t uCmd;
};

enum {
  REQ_ELAPSE = 1,
  GET_HIT_RATIO,
  REQ_COUNT,
  NET_IN_FLOW,
  NET_OUT_FLOW = 6,
  RESP_COUNT,
  GET_TECH_HIT_RATIO = 9,
  AGENT_TIMES = 15,       // agent count. data1 req, data2 res
  AGENT_TIME = 16,        // agent time. data1 req count, data2 time
  AGENT_ERROR = 17,       // agent error count. data1 count, data2 default 0
  AGENT_HIT = 18,         // agent logic ratio. data1 get count, data2 hit count
  AGENT_TECH_HIT = 19,    // agent tech ratio. data1 get count, data2 hit count
  AGENT_INOUT_BYTES = 20, // agent request and response bytes. data1 request
                          // bytes, data2 response bytes
  CONN_COUNT = 100
};

enum {
  FIVE_SEC = 1,
  TEN_SEC,
  THIRTY_SEC,
  ONE_MIN,
};

enum {
  CMD_INSERT = 0,
  CMD_UPDATE,
};

#if defined DA_STATS && DA_STATS == 1

#define stats_pool_incr(_ctx, _pool, _name)                                    \
  do {                                                                         \
    _stats_pool_incr(_ctx, _pool, STATS_POOL_##_name);                         \
  } while (0)

#define stats_pool_decr(_ctx, _pool, _name)                                    \
  do {                                                                         \
    _stats_pool_decr(_ctx, _pool, STATS_POOL_##_name);                         \
  } while (0)

#define stats_pool_incr_by(_ctx, _pool, _name, _val)                           \
  do {                                                                         \
    _stats_pool_incr_by(_ctx, _pool, STATS_POOL_##_name, _val);                \
  } while (0)

#define stats_pool_decr_by(_ctx, _pool, _name, _val)                           \
  do {                                                                         \
    _stats_pool_decr_by(_ctx, _pool, STATS_POOL_##_name, _val);                \
  } while (0)

#define stats_pool_set_ts(_ctx, _pool, _name, _val)                            \
  do {                                                                         \
    _stats_pool_set_ts(_ctx, _pool, STATS_POOL_##_name, _val);                 \
  } while (0)

#define stats_server_incr(_ctx, _server, _name)                                \
  do {                                                                         \
    _stats_server_incr(_ctx, _server, STATS_SERVER_##_name);                   \
  } while (0)

#define stats_server_decr(_ctx, _server, _name)                                \
  do {                                                                         \
    _stats_server_decr(_ctx, _server, STATS_SERVER_##_name);                   \
  } while (0)

#define stats_server_incr_by(_ctx, _server, _name, _val)                       \
  do {                                                                         \
    _stats_server_incr_by(_ctx, _server, STATS_SERVER_##_name, _val);          \
  } while (0)

#define stats_server_decr_by(_ctx, _server, _name, _val)                       \
  do {                                                                         \
    _stats_server_decr_by(_ctx, _server, STATS_SERVER_##_name, _val);          \
  } while (0)

#define stats_server_set_ts(_ctx, _server, _name, _val)                        \
  do {                                                                         \
    _stats_server_set_ts(_ctx, _server, STATS_SERVER_##_name, _val);           \
  } while (0)

#else

#define stats_pool_incr(_ctx, _pool, _name)

#define stats_pool_decr(_ctx, _pool, _name)

#define stats_pool_incr_by(_ctx, _pool, _name, _val)

#define stats_pool_decr_by(_ctx, _pool, _name, _val)

#define stats_server_incr(_ctx, _server, _name)

#define stats_server_decr(_ctx, _server, _name)

#define stats_server_incr_by(_ctx, _server, _name, _val)

#define stats_server_decr_by(_ctx, _server, _name, _val)

#endif

#define stats_enabled DA_STATS

void stats_describe(void);

void _stats_pool_incr(struct context *ctx, struct server_pool *pool,
                      stats_pool_field_t fidx);
void _stats_pool_decr(struct context *ctx, struct server_pool *pool,
                      stats_pool_field_t fidx);
void _stats_pool_incr_by(struct context *ctx, struct server_pool *pool,
                         stats_pool_field_t fidx, int64_t val);
void _stats_pool_decr_by(struct context *ctx, struct server_pool *pool,
                         stats_pool_field_t fidx, int64_t val);
void _stats_pool_set_ts(struct context *ctx, struct server_pool *pool,
                        stats_pool_field_t fidx, int64_t val);

void _stats_server_incr(struct context *ctx, struct cache_instance *ins,
                        stats_server_field_t fidx);
void _stats_server_decr(struct context *ctx, struct cache_instance *ins,
                        stats_server_field_t fidx);
void _stats_server_incr_by(struct context *ctx, struct cache_instance *ins,
                           stats_server_field_t fidx, int64_t val);
void _stats_server_decr_by(struct context *ctx, struct cache_instance *ins,
                           stats_server_field_t fidx, int64_t val);
void _stats_server_set_ts(struct context *ctx, struct cache_instance *ins,
                          stats_server_field_t fidx, int64_t val);

struct stats *stats_create(int stats_interval, char *localip,
                           struct array *server_pool);
void stats_destroy(struct stats *stats);
void stats_swap(struct stats *stats);

#endif /* DA_STATS_H_ */
