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
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <inttypes.h>
#include "da_hashkit.h"
#include "../da_time.h"
#include "../da_core.h"
#include "../da_log.h"

/*
 * 一致性hash节点数
 */
#define KETAMA_CONTINUUM_ADDITION   10  /* # extra slots to build into continuum */
#define KETAMA_POINTS_PER_SERVER    160 /* 40 points per hash */
#define KETAMA_MAX_HOSTLEN          86

static uint32_t ketama_hash(const char *key, size_t key_length,
		uint32_t alignment) {
	unsigned char results[16];

	md5_signature((unsigned char*) key, key_length, results);

	return ((uint32_t) (results[3 + alignment * 4] & 0xFF) << 24)
			| ((uint32_t) (results[2 + alignment * 4] & 0xFF) << 16)
			| ((uint32_t) (results[1 + alignment * 4] & 0xFF) << 8)
			| (results[0 + alignment * 4] & 0xFF);
}

static int ketama_item_cmp(const void *t1, const void *t2) {
	const struct continuum *ct1 = t1, *ct2 = t2;

	if (ct1->value == ct2->value) {
		return 0;
	} else if (ct1->value > ct2->value) {
		return 1;
	} else {
		return -1;
	}
}

#if defined DA_COMPATIBLE_MODE && DA_COMPATIBLE_MODE == 1
int ketama_update(struct server_pool *pool) {
	uint32_t nserver; /* # server - live and dead */
	uint32_t pointer_counter; /* # pointers on continuum */
	int pointer_index; /* pointer index */
	uint32_t points_per_server; /* points per server */
	uint32_t continuum_index; /* continuum index */
	uint32_t continuum_addition; /* extra space in the continuum */
	uint32_t server_index; /* server index */
	uint32_t value; /* continuum value */

	ASSERT(array_n(&pool->server) > 0);
	nserver = array_n(&pool->server);

	continuum_addition = KETAMA_CONTINUUM_ADDITION;
	points_per_server = 100;
	/*
	 * Allocate the continuum for the pool, the first time, and every time we
	 * add a new server to the pool
	 */
	if (nserver > pool->nserver_continuum) {
		struct continuum *continuum;
		uint32_t nserver_continuum = nserver + continuum_addition;
		uint32_t ncontinuum = nserver_continuum * points_per_server;

		continuum = realloc(pool->continuum, sizeof(*continuum) * ncontinuum);
		if (continuum == NULL) {
			return -1;
		}
		//设置pool的一致性hash环
		pool->continuum = continuum;
		pool->nserver_continuum = nserver_continuum;
		/* pool->ncontinuum is initialized later as it could be <= ncontinuum */
	}

	/*
	 * Build a continuum with the servers that are live and points from
	 * these servers that are proportial to their weight
	 */
	continuum_index = 0;
	pointer_counter = 0;
	for (server_index = 0; server_index < nserver; server_index++) {
		struct server *server;

		server = array_get(&pool->server, server_index);

		for (pointer_index = 0;
				pointer_index < points_per_server;
				pointer_index++) {

			char host[KETAMA_MAX_HOSTLEN] = "";
			size_t hostlen;

			hostlen = snprintf(host, KETAMA_MAX_HOSTLEN, "%.*s#%u",
					server->name.len, server->name.data, pointer_index);

			value = hash_chash(host,hostlen);
			pool->continuum[continuum_index].index = server_index;
			pool->continuum[continuum_index++].value = value;
		}
		pointer_counter += points_per_server;
	}
	pool->ncontinuum = pointer_counter;
	//对continum进行排序
	qsort(pool->continuum, pool->ncontinuum, sizeof(*pool->continuum),
			ketama_item_cmp);
	for (pointer_index = 0;
			pointer_index < ((nserver * points_per_server) - 1);
			pointer_index++) {
		if (pointer_index + 1 >= pointer_counter) {
			break;
		}ASSERT(
				pool->continuum[pointer_index].value
				<= pool->continuum[pointer_index + 1].value);
	}
	return 0;
}

#else
int ketama_update(struct server_pool *pool) {
	uint32_t nserver; /* # server - live and dead */
	uint32_t pointer_per_server; /* pointers per server proportional to weight */
	uint32_t pointer_per_hash; /* pointers per hash */
	uint32_t pointer_counter; /* # pointers on continuum */
	uint32_t pointer_index; /* pointer index */
	uint32_t points_per_server; /* points per server */
	uint32_t continuum_index; /* continuum index */
	uint32_t continuum_addition; /* extra space in the continuum */
	uint32_t server_index; /* server index */
	uint32_t value; /* continuum value */
	uint32_t total_weight; /* total live server weight */

	ASSERT(array_n(&pool->server) > 0);

	/*
	 * Count live servers and total weight, and also update the next time to
	 * rebuild the distribution
	 */
	nserver = array_n(&pool->server);
	total_weight = 0;
	for (server_index = 0; server_index < nserver; server_index++) {

		struct server *server = array_get(&pool->server, server_index);
		//‘host:port:weight’ or ‘host:port:weight name’
		ASSERT(server->weight > 0);
		total_weight += server->weight;

	}

	continuum_addition = KETAMA_CONTINUUM_ADDITION;
	points_per_server = KETAMA_POINTS_PER_SERVER;
	/*
	 * Allocate the continuum for the pool, the first time, and every time we
	 * add a new server to the pool
	 */
	if (nserver > pool->nserver_continuum) {
		struct continuum *continuum;
		uint32_t nserver_continuum = nserver + continuum_addition;
		uint32_t ncontinuum = nserver_continuum * points_per_server;

		continuum = realloc(pool->continuum, sizeof(*continuum) * ncontinuum);
		if (continuum == NULL) {
			return -1;
		}
		//设置pool的一致性hash环
		pool->continuum = continuum;
		pool->nserver_continuum = nserver_continuum;
		/* pool->ncontinuum is initialized later as it could be <= ncontinuum */
	}

	/*
	 * Build a continuum with the servers that are live and points from
	 * these servers that are proportial to their weight
	 */
	continuum_index = 0;
	pointer_counter = 0;
	for (server_index = 0; server_index < nserver; server_index++) {
		struct server *server;
		float pct;

		server = array_get(&pool->server, server_index);

		pct = (float) server->weight / (float) total_weight; //权重计算
		pointer_per_server = (uint32_t) ((floorf(
				(float) (pct * KETAMA_POINTS_PER_SERVER / 4 * (float) nserver
						+ 0.0000000001))) * 4);
		pointer_per_hash = 4;

		for (pointer_index = 1;
				pointer_index <= pointer_per_server / pointer_per_hash;
				pointer_index++) {

			char host[KETAMA_MAX_HOSTLEN] = "";
			size_t hostlen;
			uint32_t x;

			hostlen = snprintf(host, KETAMA_MAX_HOSTLEN, "%.*s-%u",
					server->name.len, server->name.data, pointer_index - 1);

			for (x = 0; x < pointer_per_hash; x++) {
				value = ketama_hash(host, hostlen, x);
				pool->continuum[continuum_index].index = server_index;
				pool->continuum[continuum_index++].value = value;
			}
		}
		pointer_counter += pointer_per_server;
	}

	pool->ncontinuum = pointer_counter;
	//对continum进行排序
	qsort(pool->continuum, pool->ncontinuum, sizeof(*pool->continuum),
			ketama_item_cmp);

	for (pointer_index = 0;
			pointer_index < ((nserver * KETAMA_POINTS_PER_SERVER) - 1);
			pointer_index++) {
		if (pointer_index + 1 >= pointer_counter) {
			break;
		}ASSERT(
				pool->continuum[pointer_index].value
				<= pool->continuum[pointer_index + 1].value);
	}
	return 0;
}
#endif

uint32_t ketama_dispatch(struct continuum *continuum, uint32_t ncontinuum,
		uint32_t hash) {
	struct continuum *begin, *end, *left, *right, *middle;

	ASSERT(continuum != NULL);
	ASSERT(ncontinuum != 0);

	begin = left = continuum;
	end = right = continuum + ncontinuum;

	while (left < right) {
		middle = left + (right - left) / 2;
		if (middle->value < hash) {
			left = middle + 1;
		} else {
			right = middle;
		}
	}

	if (right == end) {
		right = begin;
	}

	log_debug("ncontinuum: %d, idx: %d\n", ncontinuum, right->index);
	return right->index;
}
