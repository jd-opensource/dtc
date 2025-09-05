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

#include <stddef.h>
#include <sys/time.h>
#include <pthread.h>
#include <inttypes.h>
#include <sys/mman.h>
#include "da_core.h"
#include "da_stats.h"
#include "da_server.h"
#include "da_log.h"
#include "da_time.h"

struct string http_rsp;
static char *report_url = "";
static char *report_url_instance = "";
static int httptimeout = 2;
static int report_offset = 0;

struct stats_desc {
	char *name; /* stats name */
	stats_type_t type;
	char *desc; /* stats description */
};
#define DEFINE_ACTION(_name, _type, _desc) { .type = _type, .name = string(#_name) },
static struct stats_metric stats_pool_codec[] = {
STATS_POOL_CODEC( DEFINE_ACTION ) };

static struct stats_metric stats_server_codec[] = {
STATS_SERVER_CODEC( DEFINE_ACTION ) };
#undef DEFINE_ACTION

#define DEFINE_ACTION(_name, _type, _desc) { .type = _type, .name = #_name, .desc = _desc },
static struct stats_desc stats_pool_desc[] = {
STATS_POOL_CODEC( DEFINE_ACTION ) };

static struct stats_desc stats_server_desc[] = {
STATS_SERVER_CODEC( DEFINE_ACTION ) };
#undef DEFINE_ACTION

/* wake lock for lock timeout*/
static pthread_mutex_t wakeLock;

void stats_describe(void) {
	uint32_t i;

	write_stderr("pool stats:");
	for (i = 0; i < NELEMS(stats_pool_desc); i++) {
		write_stderr("  %-20s\"%s\"", stats_pool_desc[i].name,
				stats_pool_desc[i].desc);
	}

	write_stderr("");

	write_stderr("server stats:");
	for (i = 0; i < NELEMS(stats_server_desc); i++) {
		write_stderr("  %-20s\"%s\"", stats_server_desc[i].name,
				stats_server_desc[i].desc);
	}
}

static void stats_metric_init(struct stats_metric *stm) {
	switch (stm->type) {
	case STATS_COUNTER:
		stm->value.counter = 0LL;
		break;

	case STATS_GAUGE:
		stm->value.counter = 0LL;
		break;

	case STATS_TIMESTAMP:
		stm->value.timestamp = 0LL;
		break;

	default:
		log_error("error stats_type");
	}
}

static void stats_metric_reset(struct array *stats_metric) {
	uint32_t i, nmetric;

	nmetric = array_n(stats_metric);
	ASSERT(nmetric == STATS_POOL_NFIELD || nmetric == STATS_SERVER_NFIELD);

	for (i = 0; i < nmetric; i++) {
		struct stats_metric *stm = array_get(stats_metric, i);

		stats_metric_init(stm);
	}
}

static void stats_pool_reset(struct array *stats_pool) {
	uint32_t i, npool;

	npool = array_n(stats_pool);

	for (i = 0; i < npool; i++) {
		struct stats_pool *stp = array_get(stats_pool, i);
		uint32_t j, nserver;

		stats_metric_reset(&stp->metric);

		nserver = array_n(&stp->server);
		
		for (j = 0; j < nserver; j++) {
			struct stats_server *sts = array_get(&stp->server, j);
			stats_metric_reset(&sts->metric);
		}
	}
}

static void stats_aggregate_item(struct stats_file_item *item_list,
		struct array *shadow_metric, int list_size) {
	int i;
	struct stats_metric *stm;
	for (i = 0; i < list_size; i++) {
		stm = array_get(shadow_metric, i);
		switch (item_list[i].type) {
		case STATS_COUNTER:
			item_list[i].stat_once = stm->value.counter;
			item_list[i].stat_all += stm->value.counter;
			break;
		case STATS_GAUGE:
			item_list[i].stat_once = stm->value.counter;
			item_list[i].stat_all += stm->value.counter;
			break;
		case STATS_TIMESTAMP:
			if (stm->value.timestamp) {
				item_list[i].stat_once = stm->value.counter;
				item_list[i].stat_all = stm->value.counter;
			}
			break;
		default:
			log_error("error stats_type");
		}
	}
}

static void stats_aggregate_reset(struct stats_file_item *item_list,
		int list_size) {
	int i;
	for (i = 0; i < list_size; i++) {
		switch (item_list[i].type) {
		case STATS_COUNTER:
			item_list[i].stat_once = 0;
			break;
		case STATS_GAUGE:
			item_list[i].stat_once = 0;
			break;
		case STATS_TIMESTAMP:
			item_list[i].stat_once = 0;
			item_list[i].stat_all = 0;
			break;
		default:
			log_error("error stats_type");
		}
	}
}
static void stats_aggregate(struct stats *st) {
	uint32_t i;
	if (st->aggregate == 0) {
		//log_debug("skip aggregate of shadow %p  as generator is slow",
		//		st->shadow.elem);
		for (i = 0; i < array_n(&st->aggregator); i++) {
			uint32_t j;
			struct stats_file_pool *stfp;
			stfp = array_get(&st->aggregator, i);
			stats_aggregate_reset(stfp->pool_item_list,
					stfp->phead->poolfields);
			for (j = 0; j < array_n(&stfp->stats_file_servers); j++) {
				struct stats_file_server *stfs;
				stfs = array_get(&stfp->stats_file_servers, j);
				stats_aggregate_reset(stfs->server_item_list,
						stfs->shead->serverfields);
			}
		}
		return;
	}

	for (i = 0; i < array_n(&st->aggregator); i++) {
		uint32_t j;
		struct stats_file_pool *stfp;
		struct stats_pool *stp;

		stp = array_get(&st->shadow, i);
		stfp = array_get(&st->aggregator, i);
		stats_aggregate_item(stfp->pool_item_list, &stp->metric,
				stfp->phead->poolfields);

		for (j = 0; j < array_n(&stfp->stats_file_servers); j++) {
			struct stats_file_server *stfs;
			struct stats_server *sts;

			sts = array_get(&stp->server, j);
			stfs = array_get(&stfp->stats_file_servers, j);
			stats_aggregate_item(stfs->server_item_list, &sts->metric,
					stfs->shead->serverfields);
		}
	}
	/*
	 * Reset shadow (b) stats before giving it back to generator to keep
	 * stats addition idempotent
	 */
	stats_pool_reset(&st->shadow);
	st->aggregate = 0;
	return;
}

/* report data to monitor center*/
static int stats_report(struct stats *st) {
	return 0;
}


static void stats_loop_callback(void *arg1, int arg2) {
	struct stats *st = arg1;
	int n = arg2;
	/* aggregate stats from shadow (b) -> sum (c) */
	stats_aggregate(st);

	/* report data to monitor center*/
	if (n == 1) {
		stats_report(st);
	}

	return;
}

static void event_loop_stats(event_stats_cb_t cb, void *arg) {
	struct stats *st = arg;
	time_t next = 0;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	next = (tv.tv_sec / 10) * 10 + 10+report_offset;
	struct timespec ts;
	ts.tv_sec = next;
	ts.tv_nsec = 0;

	_set_remote_log_fd_();

	while (pthread_mutex_timedlock(&wakeLock, &ts) != 0) {
		gettimeofday(&tv, NULL);
		if (tv.tv_sec >= next) {
			//call stats_loop_callback
			cb(st, 1);
			gettimeofday(&tv, NULL);
			//report add an offset
			next = (tv.tv_sec / 10) * 10 + 10 + report_offset;
		}
		ts.tv_sec = next;
		ts.tv_nsec = 0;
	}

	pthread_mutex_unlock(&wakeLock);
	pthread_mutex_destroy(&wakeLock);
}

static void *stats_loop(void *arg) {
	event_loop_stats(stats_loop_callback, arg);
	return NULL;
}

static int stats_start_aggregator(struct stats *st) {
	int status;

	if (!stats_enabled) {
		return 0;
	}
	report_offset =rand() % 10;
	//init the globe mutex
	pthread_mutex_init(&wakeLock, NULL);
	if (pthread_mutex_trylock(&wakeLock) == 0) {
		status = pthread_create(&st->tid, NULL, stats_loop, st);
		if (status < 0) {
			log_error("stats aggregator create failed: %s", strerror(status));
			return -1;
		}
	}
	return 0;
}

static void stats_stop_aggregator(struct stats *st) {
	if (!stats_enabled) {
		return;
	}
	if (pthread_mutex_trylock(&wakeLock) == 0) {
		pthread_mutex_unlock(&wakeLock);
	}
	return;
}

static void *mount(const char *filename, int rw, int *size) {
	char path[256];
	void *_map;
	int fd;

	mkdir(STATS_DIR, 0755);
	if (access(STATS_DIR, W_OK | X_OK) < 0) {
		log_error("stats dir(%s): Not writable", STATS_DIR);
		return NULL;
	}
	int flag = rw;
	int prot = 0;

	switch (flag) {
	case O_WRONLY:
	case O_RDWR:
	default:
		flag = O_CREAT | O_RDWR;
		prot = PROT_WRITE | PROT_READ;
		break;

	case O_RDONLY:
		prot = PROT_READ;
		break;
	}

	da_strcpy(path, STATS_DIR);
	da_strcat(path, filename);

	if ((fd = open(path, flag, 0644)) < 0) {
		log_error("open failed[path:%s], errno:%d %m", path, errno);
		return NULL;
	}
	struct stat st;
	fstat(fd, &st);

	if (O_RDONLY == rw) {
		*size = st.st_size;
	} else if (st.st_size != *size) {
		int unused;
		unused = ftruncate(fd, *size);
	}

	_map = (char *) mmap(0, *size, prot, MAP_SHARED, fd, 0);
	if (MAP_FAILED == _map) {
		log_error("map failed[path:%s, size:%d, _fd:%d], errno:%d %m", path,
				*size, fd, errno);
		close(fd);
		return NULL;
	}
	close(fd);
	log_debug("map success[path:%s, size:%d, _fd:%d]", path, *size, fd);
	return _map;
}

static int stats_file_mount(struct array *_map_items, struct array *server_pool) {
	int i, status, npool;
	pid_t pid;
	array_null(_map_items);

	pid = getpid();
	npool = array_n(server_pool);
	status = array_init(_map_items, npool, sizeof(struct _map_item));
	if (status < 0) {
		log_error("_map_items array init fail,lack of memory");
		return -1;
	}

	for (i = 0; i < npool; i++) {
		int nserver, size, j, k, l,ninstance;
		uint8_t *pos;
		struct _map_item *mi = array_push(_map_items);
		struct server_pool *sp = array_get(server_pool, i);
		nserver = array_n(&sp->server);
		ninstance = 0;
		for (j = 0; j < nserver; j++)
		{
			struct server *s;
			s = array_get(&sp->server,j);
			ninstance += array_n(&s->high_ptry_ins);
			ninstance += array_n(&s->low_prty_ins);
		}
		size = sizeof(struct stats_file_pool_head)
				+ STATS_POOL_NFIELD * sizeof(struct stats_file_item)
				+ ninstance
						* (sizeof(struct stats_file_server_head)
								+ STATS_SERVER_NFIELD
										* sizeof(struct stats_file_item));
		da_snprintf(mi->filename, 256, "%s_%s_%d", STATS_FILE, sp->name.data,
				pid);
		mi->_map_size = size;
		mi->_map_start = mount(mi->filename, O_RDWR, &size);
		if (mi->_map_start == NULL) {
			log_error("mmap stats file[name:%s] for %s failed", mi->filename,
					sp->name.data);
			return -1;
		}
		//reinit the mmap mem
		pos = mi->_map_start;
		struct stats_file_pool_head * sfph = (struct stats_file_pool_head *) pos;
		da_strcpy(sfph->poolname, sp->name.data);
		sfph->mid = sp->mid;
		sfph->poolfields = STATS_POOL_NFIELD;
		sfph->servernum = ninstance;
		pos += sizeof(struct stats_file_pool_head);
		for (j = 0; j < STATS_POOL_NFIELD; j++) {
			struct stats_file_item *sfi = (struct stats_file_item *) pos;
			sfi->type = stats_pool_desc[j].type;
			sfi->stat_once = 0;
			sfi->stat_all = 0;
			pos += sizeof(struct stats_file_item);
		}
		for (j = 0; j < nserver; j++) {
			struct server *s = array_get(&sp->server, j);
			ninstance = array_n(&s->high_ptry_ins);
			for(l = 0; l < ninstance ; l++)
			{
				struct cache_instance *ins = array_get(&s->high_ptry_ins,l);
				struct stats_file_server_head * sfsh =
					(struct stats_file_server_head *) pos;
				da_strcpy(sfsh->servername, s->name.data);
				sfsh->sid = ins->idx;
				sfsh->serverfields = STATS_SERVER_NFIELD;
				pos += sizeof(struct stats_file_server_head);
				for (k = 0; k < STATS_SERVER_NFIELD; k++) {
					struct stats_file_item *sfi = (struct stats_file_item *) pos;
					sfi->type = stats_pool_desc[k].type;
					sfi->stat_once = 0;
					sfi->stat_all = 0;
					pos += sizeof(struct stats_file_item);
				}
			}
			ninstance = array_n(&s->low_prty_ins);
			for (l = 0; l < ninstance; l++)
			{
				struct cache_instance *ins = array_get(&s->low_prty_ins, l);
				struct stats_file_server_head * sfsh =
					(struct stats_file_server_head *) pos;
				da_strcpy(sfsh->servername, s->name.data);
				sfsh->sid = ins->idx;
				sfsh->serverfields = STATS_SERVER_NFIELD;
				pos += sizeof(struct stats_file_server_head);
				for (k = 0; k < STATS_SERVER_NFIELD; k++) {
					struct stats_file_item *sfi = (struct stats_file_item *) pos;
					sfi->type = stats_pool_desc[k].type;
					sfi->stat_once = 0;
					sfi->stat_all = 0;
					pos += sizeof(struct stats_file_item);
				}
			}
		}
	}
	return 0;
}

static int stats_file_unmount(struct array *_map_items) {
	int i, status;
	char path[256];
	for (i = 0; i < array_n(_map_items); i++) {
		struct _map_item *mi = array_pop(_map_items);
		munmap((void *) mi->_map_start, mi->_map_size);

		da_strcpy(path, STATS_DIR);
		da_strcat(path, mi->filename);
		status = unlink(path);
		if (status < 0) {
			log_error("unlink of pid file '%s' failed, ignored: %s", path,
					strerror(errno));
		}
	}
	array_deinit(_map_items);
	return 0;
}

static int stats_aggregator_map(struct array *_map_items,
		struct array *aggregator) {
	int i, j, npool, status;
	array_null(aggregator);

	npool = array_n(_map_items);
	status = array_init(aggregator, npool, sizeof(struct stats_file_pool));
	if (status != 0) {
		log_error("init stats_file_pool error, lack of memory");
		return status;
	}
	for (i = 0; i < npool; i++) {
		struct _map_item *mi = array_get(_map_items, i);
		struct stats_file_pool *stfp = array_push(aggregator);
		uint8_t *temp = (uint8_t *) mi->_map_start;
		stfp->phead = (struct stats_file_pool_head *) temp;
		temp += sizeof(struct stats_file_pool_head);
		stfp->pool_item_list = (struct stats_file_item *) temp;
		temp += sizeof(struct stats_file_item) * stfp->phead->poolfields;

		array_null(&stfp->stats_file_servers);
		status = array_init(&stfp->stats_file_servers, stfp->phead->servernum,
				sizeof(struct stats_file_server));
		for (j = 0; j < stfp->phead->servernum; j++) {
			struct stats_file_server *stfs = array_push(
					&stfp->stats_file_servers);
			stfs->shead = (struct stats_file_server_head *) temp;
			temp += sizeof(struct stats_file_server_head);
			stfs->server_item_list = (struct stats_file_item *) temp;
			temp += sizeof(struct stats_file_item) * stfs->shead->serverfields;
		}

	}
	return 0;
}

static int stats_aggregator_unmap(struct array *aggregator) {
	int i;
	for (i = 0; i < array_n(aggregator); i++) {
		array_pop(aggregator);
	}
	array_deinit(aggregator);
	return 0;
}

static void stats_metric_deinit(struct array *metric) {
	uint32_t i, nmetric;

	nmetric = array_n(metric);
	for (i = 0; i < nmetric; i++) {
		array_pop(metric);
	}
	array_deinit(metric);
}

static int stats_pool_metric_init(struct array *stats_metric) {
	int status;
	uint32_t i, nfield = STATS_POOL_NFIELD;

	status = array_init(stats_metric, nfield, sizeof(struct stats_metric));
	if (status != 0) {
		return status;
	}

	for (i = 0; i < nfield; i++) {
		struct stats_metric *stm = array_push(stats_metric);

		/* initialize from pool codec first */
		*stm = stats_pool_codec[i];

		/* initialize individual metric */
		stats_metric_init(stm);
	}

	return 0;
}

static int stats_server_metric_init(struct stats_server *sts) {
	int status;
	uint32_t i, nfield = STATS_SERVER_NFIELD;

	status = array_init(&sts->metric, nfield, sizeof(struct stats_metric));
	if (status != 0) {
		return status;
	}

	for (i = 0; i < nfield; i++) {
		struct stats_metric *stm = array_push(&sts->metric);

		/* initialize from server codec first */
		*stm = stats_server_codec[i];

		/* initialize individual metric */
		stats_metric_init(stm);
	}

	return 0;
}

static int stats_server_init(struct stats_server *sts, struct server *s) {
	int status;

	sts->name = s->name;
	array_null(&sts->metric);

	status = stats_server_metric_init(sts);
	if (status != 0) {
		return status;
	}

	log_debug("init stats server '%.*s' with %"PRIu32" metric", sts->name.len,
			sts->name.data, array_n(&sts->metric));

	return 0;

}

static int stats_server_map(struct array *stats_server, struct array *server) {
	int status;
	uint32_t i, nserver, j, ninstance;;
	struct stats_server *sts;
	int cnt = 0;
	nserver = array_n(server);
	ASSERT(nserver != 0);

	status = array_init(stats_server, nserver*2, sizeof(struct stats_server));
	if (status != 0) {
		return status;
	}

	for (i = 0; i < nserver; i++) {
		struct server *s = array_get(server, i);
		ninstance = array_n(&s->high_ptry_ins);
		for (j = 0; j < ninstance; j++)
		{
			sts = array_push(stats_server);
			status = stats_server_init(sts, s);
			cnt++;
		}
		ninstance = array_n(&s->low_prty_ins);
		for (j = 0; j < ninstance; j++)
		{
			sts = array_push(stats_server);
			status = stats_server_init(sts, s);
			cnt++;
		}
			
		if (status != 0) {
			return status;
		}
	}

	log_debug("map %"PRIu32" stats servers %"PRIu32" stats instance", nserver, ninstance);

	return 0;
}

static void stats_server_unmap(struct array *stats_server) {
	uint32_t i, nserver;

	nserver = array_n(stats_server);

	for (i = 0; i < nserver; i++) {
		struct stats_server *sts = array_pop(stats_server);
		stats_metric_deinit(&sts->metric);
	}
	array_deinit(stats_server);

	log_debug("unmap %"PRIu32" stats servers", nserver);
}

static int stats_pool_init(struct stats_pool *stp, struct server_pool *sp) {
	int status;

	stp->mid = sp->mid;
	stp->port = sp->port;
	stp->name = sp->name;
	stp->main_report = sp->main_report;
	stp->instance_report = sp->instance_report;
	array_null(&stp->metric);
	array_null(&stp->server);

	status = stats_pool_metric_init(&stp->metric);
	if (status != 0) {
		return status;
	}

	status = stats_server_map(&stp->server, &sp->server);
	if (status != 0) {
		stats_metric_deinit(&stp->metric);
		return status;
	}

	log_debug(
			"init stats pool '%.*s' with %"PRIu32" metric and " "%"PRIu32" server",
			stp->name.len, stp->name.data, array_n(&stp->metric),
			array_n(&stp->metric));

	return 0;
}

static int stats_pool_map(struct array *stats_pool, struct array *server_pool) {
	int status;
	uint32_t i, npool;

	npool = array_n(server_pool);
	ASSERT(npool != 0);

	status = array_init(stats_pool, npool, sizeof(struct stats_pool));
	if (status != 0) {
		return status;
	}

	for (i = 0; i < npool; i++) {
		struct server_pool *sp = array_get(server_pool, i);
		struct stats_pool *stp = array_push(stats_pool);

		status = stats_pool_init(stp, sp);
		if (status != 0) {
			return status;
		}
	}

	log_debug("map %"PRIu32" stats pools", npool);

	return 0;
}

static void stats_pool_unmap(struct array *stats_pool) {
	uint32_t i, npool;

	npool = array_n(stats_pool);

	for (i = 0; i < npool; i++) {
		struct stats_pool *stp = array_pop(stats_pool);
		stats_metric_deinit(&stp->metric);
		stats_server_unmap(&stp->server);
	}
	array_deinit(stats_pool);

	log_debug("unmap %"PRIu32" stats pool", npool);
}

struct stats *stats_create(int stats_interval, char * localip, struct array *server_pool) {

	int status;
	struct stats *st;

	st = malloc(sizeof(*st));
	if (st == NULL) {
		return NULL;
	}
	array_null(&st->current);
	array_null(&st->shadow);

	st->interval = stats_interval;
	st->start_ts = now_ms;
	st->tid = (pthread_t) -1;
	st->updated = 0;
	st->aggregate = 0;
	strncpy(st->localip, localip, sizeof(st->localip));

	status = stats_pool_map(&st->current, server_pool);
	if (status != 0) {
		goto error;
	}

	status = stats_pool_map(&st->shadow, server_pool);
	if (status != 0) {
		goto error;
	}

	status = stats_file_mount(&st->_map_items, server_pool);
	if (status != 0) {
		goto error;
	}

	status = stats_aggregator_map(&st->_map_items, &st->aggregator);
	if (status != 0) {
		goto error;
	}

	status = stats_start_aggregator(st);
	if (status != 0) {
		goto error;
	}

	return st;

	error: stats_destroy(st);
	return NULL;
}

void stats_destroy(struct stats *st) {
	stats_stop_aggregator(st);
	stats_aggregator_unmap(&st->aggregator);
	stats_file_unmount(&st->_map_items);
	stats_pool_unmap(&st->shadow);
	stats_pool_unmap(&st->current);
	free(st);
}

void stats_swap(struct stats *st) {
	if (!stats_enabled) {
		return;
	}

	if (st->aggregate == 1) {
		//log_debug("skip swap of current %p shadow %p as aggregator "
		//		"is busy", st->current.elem, st->shadow.elem);
		return;
	}

	if (st->updated == 0) {
		//log_debug("skip swap of current %p shadow %p as there is "
		//		"nothing new", st->current.elem, st->shadow.elem);
		return;
	}

	//log_debug("swap stats current %p shadow %p", st->current.elem,
	//		st->shadow.elem);

	//shadow has been reset
	array_swap(&st->current, &st->shadow);

	st->updated = 0;
	st->aggregate = 1;
}

static struct stats_metric *stats_pool_to_metric(struct context *ctx,
		struct server_pool *pool, stats_pool_field_t fidx) {
	struct stats *st;
	struct stats_pool *stp;
	struct stats_metric *stm;
	uint32_t pidx;

	pidx = pool->idx;
	st = ctx->stats;
	stp = array_get(&st->current, pidx);
	stm = array_get(&stp->metric, fidx);
	st->updated = 1;

	log_debug("metric '%.*s' in pool %"PRIu32"", stm->name.len, stm->name.data,
			pidx);
	return stm;
}

void _stats_pool_incr(struct context *ctx, struct server_pool *pool,
		stats_pool_field_t fidx) {
	struct stats_metric *stm;

	stm = stats_pool_to_metric(ctx, pool, fidx);

	ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
	stm->value.counter++;

	log_debug("incr field '%.*s' to %"PRId64"", stm->name.len, stm->name.data,
			stm->value.counter);
}

void _stats_pool_decr(struct context *ctx, struct server_pool *pool,
		stats_pool_field_t fidx) {
	struct stats_metric *stm;

	stm = stats_pool_to_metric(ctx, pool, fidx);

	ASSERT(stm->type == STATS_GAUGE);
	stm->value.counter--;

	log_debug("decr field '%.*s' to %"PRId64"", stm->name.len, stm->name.data,
			stm->value.counter);
}

void _stats_pool_incr_by(struct context *ctx, struct server_pool *pool,
		stats_pool_field_t fidx, int64_t val) {
	struct stats_metric *stm;

	stm = stats_pool_to_metric(ctx, pool, fidx);

	ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
	stm->value.counter += val;

	log_debug("incr by field '%.*s' to %"PRId64"", stm->name.len,
			stm->name.data, stm->value.counter);
}

void _stats_pool_decr_by(struct context *ctx, struct server_pool *pool,
		stats_pool_field_t fidx, int64_t val) {
	struct stats_metric *stm;

	stm = stats_pool_to_metric(ctx, pool, fidx);

	ASSERT(stm->type == STATS_GAUGE);
	stm->value.counter -= val;

	log_debug("decr by field '%.*s' to %"PRId64"", stm->name.len,
			stm->name.data, stm->value.counter);
}

void _stats_pool_set_ts(struct context *ctx, struct server_pool *pool,
		stats_pool_field_t fidx, int64_t val) {
	struct stats_metric *stm;

	stm = stats_pool_to_metric(ctx, pool, fidx);

	ASSERT(stm->type == STATS_TIMESTAMP);
	stm->value.timestamp = val;

	log_debug("set ts field '%.*s' to %"PRId64"", stm->name.len, stm->name.data,
			stm->value.timestamp);
}

static struct stats_metric *
stats_server_to_metric(struct context *ctx, struct cache_instance *ins,
		stats_server_field_t fidx) {
	struct stats *st;
	struct stats_pool *stp;
	struct stats_server *sts;
	struct stats_metric *stm;
	struct server_pool *sp;
	struct server *server,*tmp_server;
	uint32_t pidx, sidx, i, n ;
	n = 0;

	sidx = ins->idx;
	server = ins->owner;
	pidx = server->owner->idx;
	sp = server->owner;
	
	for (i = 0; i < server->idx; i++)
	{
		tmp_server = array_get(&sp->server, i);
		n += array_n(&tmp_server->high_ptry_ins);
		n += array_n(&tmp_server->low_prty_ins);
		
	}
	
	sidx += n;
	
	st = ctx->stats;
	stp = array_get(&st->current, pidx);
	sts = array_get(&stp->server, sidx);
	stm = array_get(&sts->metric, fidx);

	st->updated = 1;

	log_debug("metric '%.*s' in pool %"PRIu32" server %"PRIu32"", stm->name.len,
			stm->name.data, pidx, sidx);

	return stm;
}

void _stats_server_incr(struct context *ctx, struct cache_instance *ins,
		stats_server_field_t fidx) {
	struct stats_metric *stm;

	stm = stats_server_to_metric(ctx, ins, fidx);

	ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
	stm->value.counter++;

	log_debug("incr field '%.*s' to %"PRId64"", stm->name.len, stm->name.data,
			stm->value.counter);
}

void _stats_server_decr(struct context *ctx, struct cache_instance *ins,
		stats_server_field_t fidx) {
	struct stats_metric *stm;

	stm = stats_server_to_metric(ctx, ins, fidx);

	ASSERT(stm->type == STATS_GAUGE);
	stm->value.counter--;

	log_debug("decr field '%.*s' to %"PRId64"", stm->name.len, stm->name.data,
			stm->value.counter);
}

void _stats_server_incr_by(struct context *ctx, struct cache_instance *ins,
		stats_server_field_t fidx, int64_t val) {
	struct stats_metric *stm;

	stm = stats_server_to_metric(ctx, ins, fidx);

	ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
	stm->value.counter += val;

	log_debug("incr by field '%.*s' to %"PRId64"", stm->name.len,
			stm->name.data, stm->value.counter);
}

void _stats_server_decr_by(struct context *ctx, struct cache_instance *ins,
		stats_server_field_t fidx, int64_t val) {
	struct stats_metric *stm;

	stm = stats_server_to_metric(ctx, ins, fidx);

	ASSERT(stm->type == STATS_GAUGE);
	stm->value.counter -= val;

	log_debug("decr by field '%.*s' to %"PRId64"", stm->name.len,
			stm->name.data, stm->value.counter);
}

void _stats_server_set_ts(struct context *ctx, struct cache_instance *ins,
		stats_server_field_t fidx, int64_t val) {
	struct stats_metric *stm;

	stm = stats_server_to_metric(ctx, ins, fidx);

	ASSERT(stm->type == STATS_TIMESTAMP);
	stm->value.timestamp = val;

	log_debug("set ts field '%.*s' to %"PRId64"", stm->name.len, stm->name.data,
			stm->value.timestamp);
}
