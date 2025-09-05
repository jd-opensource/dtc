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

#include "expire_time.h"
#include <time.h>
#include <stdlib.h>

DTC_USING_NAMESPACE

ExpireTime::ExpireTime(TimerList *t, BufferPond *c, DataProcess *p,
		       DTCTableDefinition *td, int e)
	: timer(t), cache(c), process(p), table_definition_(td), max_expire_(e)
{
	stat_expire_count =
		g_stat_mgr.get_stat_int_counter(DTC_KEY_EXPIRE_DTC_COUNT);
	stat_get_request_count = g_stat_mgr.get_stat_int_counter(DTC_GET_COUNT);
	stat_insert_request_count =
		g_stat_mgr.get_stat_int_counter(DTC_INSERT_COUNT);
	stat_update_request_count =
		g_stat_mgr.get_stat_int_counter(DTC_UPDATE_COUNT);
	stat_delete_request_count =
		g_stat_mgr.get_stat_int_counter(DTC_DELETE_COUNT);
	stat_purge_request_count =
		g_stat_mgr.get_stat_int_counter(DTC_PURGE_COUNT);
}

ExpireTime::~ExpireTime()
{
}

void ExpireTime::start_key_expired_task(void)
{
	log4cplus_info("start key expired job");
	attach_timer(timer);
	return;
}

int ExpireTime::try_expire_count()
{
	int num1 = max_expire_ - (stat_get_request_count.get() +
				  stat_insert_request_count.get() +
				  stat_update_request_count.get() +
				  stat_delete_request_count.get() +
				  stat_purge_request_count.get()) /
					 10;
	int num2 = cache->get_total_used_node();
	return num1 < num2 ? num1 : num2;
}

void ExpireTime::job_timer_procedure(void)
{
	log4cplus_debug("enter timer procedure");
	log4cplus_debug("sched key expire job");
	int start = cache->get_min_valid_node_id(), end = cache->max_node_id();
	int count, interval = end - start, node_id;
	int i, j, k = 0;
	struct timeval tv;

	gettimeofday(&tv, NULL);
	log4cplus_debug("tv.tv_usec: %ld", tv.tv_usec);
	srandom(tv.tv_usec);
	count = try_expire_count();
	log4cplus_debug("try_expire_count: %d", count);
	for (i = 0, j = 0; i < count && j < count * 3; ++j) {
		Node node;
		node_id = random() % interval + start;
		node = I_SEARCH(node_id);
		uint32_t expire = 0;
		if (!!node && !node.not_in_lru_list() &&
		    !cache->is_time_marker(node)) {
			// read expire time
			// if expired
			// 	purge
			++i;
			if (process->get_expire_time(table_definition_, &node,
						     expire) != 0) {
				log4cplus_error(
					"get expire time error for node: %d",
					node.node_id());
				continue;
			}
			log4cplus_debug("node id: %d, expire: %d, current: %ld",
					node.node_id(), expire, tv.tv_sec);
			if (expire != 0 && expire < tv.tv_sec) {
				log4cplus_debug(
					"expire time timer purge node: %d, %d",
					node.node_id(), ++k);
				cache->inc_total_row(
					0LL - cache->node_rows_count(node));
				if (cache->purge_node_and_data(node) != 0) {
					log4cplus_error(
						"purge node error, node: %d",
						node.node_id());
				}
				++stat_expire_count;
			}
		}
	}
	log4cplus_debug("expire time found %d real node, %d", i, k);

	attach_timer(timer);
	log4cplus_debug("leave timer procedure");
	return;
}
