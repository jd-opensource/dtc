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

#ifndef DA_CORE_H_
#define DA_CORE_H_

#include "da_array.h"
#include "da_util.h"

#ifdef HAVE_ASSERT_PANIC
#define DA_ASSERT_PANIC 1
#endif
#ifdef HAVE_ASSERT_LOG
#define DA_ASSERT_LOG 1
#endif

#define DA_STATS 1
#define USE_COMPATIBLE_MODE

#ifdef USE_COMPATIBLE_MODE
#define DA_COMPATIBLE_MODE 1
#else
#define DA_COMPATIBLE_MODE 0
#endif

#define RESERVED_FDS 32
#define NC_ENV_FDS "NC_ENV_FDS"

#define DA_ADDR_LEN 32

struct event_base;
struct context;
struct conn;
struct instance;

struct context {
  uint32_t id;         /* unique context id */
  struct conf *cf;     /* configuration */
  struct stats *stats; /* stats */

  struct array pool;      /* server_pool[] */
  struct event_base *evb; /* event base */
  int max_timeout;        /* max timeout in msec */
  int timeout;            /* timeout in msec */

  uint32_t max_nfd;    /* max # files */
  uint32_t max_ncconn; /* max # client connections */
  uint32_t max_nsconn; /* max # server connections */

  uint32_t sum_nconn; /* client connections and server connections sum*/
};

struct instance {
  struct context *ctx;              /* active context */
  int log_level;                    /* log level */
  char *log_dir;                    /* log dir*/
  char *conf_filename;              /* configuration filename */
  char hostname[DA_MAXHOSTNAMELEN]; /* hostname */
  size_t mbuf_chunk_size;           /* mbuf chunk size */
  int event_max_timeout;            /* epoll max time out*/
  int stats_interval;               /* stats aggregation interval */
  pid_t pid;                        /* process id */
  char *pid_filename;               /* pid filename */
  unsigned pidfile : 1;             /* pid file created? */
  int cpumask;                      /*cpu mask for run*/
  char **argv;                      /* argv of main() */
};

// status for server
enum core_status {
  NORMAL,
  RELOADING,
  EXITING,
  EXITED,
};

// extern struct conn *wait_send_queue[]; /*cached conn*/

void cache_send_event(struct conn *conn);
int core_core(void *arg, uint32_t events);
struct context *core_start(struct instance *dai);
void core_stop(struct context *ctx);
int core_loop(struct context *ctx);

int core_exec_new_binary(struct instance *dai);
int core_inherited_socket(char *listen_address);
void core_cleanup_inherited_socket(void);

void core_setinst_status(enum core_status status);
enum core_status core_getinst_status();
#endif /* DA_CORE_H_ */
