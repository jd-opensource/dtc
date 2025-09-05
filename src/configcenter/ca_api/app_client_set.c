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
* 
*/
#include "app_client_set.h"
#include "config_center_client.h"
#include "ca_quick_find.h"
#include "app_shm_manager.h"

typedef struct handle_ptr {
	uint64_t version;
	IP_NODE *master_app_set;
	IP_NODE *slave_app_set;
	FORWARD_ITEM *forward_ptr;
} HANDLE_PTR;

int print_forward(FORWARD_ITEM *header_ptr)
{
	if (!header_ptr)
		return -NULL_PTR;
	printf("lock=[%d]\n", header_ptr->lock);
	printf("shm_key=[%d]\n", header_ptr->shm_key);
	printf("shm_size=[%d]\n", header_ptr->shm_size);
	printf("time_stamp=[%lu]\n", header_ptr->time_stamp);
	printf("bid_size=[%d]\n", header_ptr->bid_size);
	printf("node_size=[%d]\n", header_ptr->node_size);
	printf("forward_shm_id=[%d]\n", header_ptr->forward_shm_id);
	printf("master_shm_id=[%d]\n", header_ptr->master_shm_id);
	printf("slave_shm_id=[%d]\n", header_ptr->slave_shm_id);

	int i = 0;
	for (; i < header_ptr->bid_size; ++i) {
		printf("count=[%d] bid=[%d] size=[%d] offset=[%d]\n", i,
		       header_ptr->headers[i].bid, header_ptr->headers[i].size,
		       header_ptr->headers[i].offset);
	}

	return 0;
}

int print_app_set(IP_NODE *app_set_ptr, int size)
{
	if (!app_set_ptr)
		return -NULL_PTR;
	int i = 0;
	for (; i < size; ++i) {
		printf("bid=%dip=%sport=%dstatus=%dweight=%d\n",
		       app_set_ptr->bid, app_set_ptr->ip, app_set_ptr->port,
		       app_set_ptr->status, app_set_ptr->weight);
		app_set_ptr += 1;
	}

	return 0;
}

int load_header(HANDLE_PTR *ca_handler)
{
	if (!ca_handler)
		return -NULL_PTR;
	int exist = 1;
	int shm_id = 0;
	ca_handler->forward_ptr =
		(FORWARD_ITEM *)get_shm(FORWARD_SHM_KEY,
					sizeof(FORWARD_ITEM) * 2, 0644, &shm_id,
					false, &exist);
	if (!ca_handler->forward_ptr)
		return -GET_SHM_ERR;
	ca_handler->version = ca_handler->forward_ptr->time_stamp;
	return 0;
}

int load_app_set(HANDLE_PTR *ca_handler)
{
	if (!ca_handler)
		return -NULL_PTR;
	int exist = 1;
	int shm_id = 0;
	FORWARD_ITEM *master_forward = ca_handler->forward_ptr;
	FORWARD_ITEM *slave_forward = ca_handler->forward_ptr + 1;
	ca_handler->master_app_set =
		(IP_NODE *)get_shm(MASTER_SHM_KEY, master_forward->shm_size,
				   0644, &shm_id, false, &exist);
	ca_handler->slave_app_set =
		(IP_NODE *)get_shm(SLAVE_SHM_KEY, slave_forward->shm_size, 0644,
				   &shm_id, false, &exist);
	if (!ca_handler->master_app_set || !ca_handler->slave_app_set)
		return -GET_SHM_ERR;
	return 0;
}

int init_shm(HANDLE_PTR *ca_handler)
{
	if (!ca_handler)
		return -NULL_PTR;
	memset(ca_handler, 0, sizeof(HANDLE_PTR));
	int ret = load_header(ca_handler);
	if (ret < 0)
		return ret;
	ret = load_app_set(ca_handler);
	return ret;
}

int get_version(uint64_t *version)
{
	HANDLE_PTR ca_handler;
	int ret = load_header(&ca_handler);
	if (ret < 0)
		return ret;

	*version = ca_handler.version;
	return detach_shm(ca_handler.forward_ptr);
}

int dump_ca_shm()
{
	HANDLE_PTR client;
	int ret = init_shm(&client);
	if (ret < 0)
		return ret;

	{
		if (!client.forward_ptr->lock) {
			printf("\n======================= master ====================\n");
			print_forward(client.forward_ptr);
			printf("\n======================= master app set ====================\n");
			int size = client.forward_ptr->node_size;
			print_app_set(client.master_app_set, size);
		} else {
			printf("\n======================= slave ====================\n");
			print_forward(client.forward_ptr + 1);
			printf("\n======================= slave app set ====================\n");
			int size = client.forward_ptr->node_size;
			print_app_set(client.slave_app_set, size);
		}
	}
	return 0;
}

int detach_handler(HANDLE_PTR *ca_handler)
{
	int ret1 = detach_shm(ca_handler->forward_ptr);
	int ret2 = detach_shm(ca_handler->master_app_set);
	int ret3 = detach_shm(ca_handler->slave_app_set);
	if (ret1 < 0 || ret2 < 0 || ret3 < 0)
		return -DETACH_SHM_ERR;

	return 0;
}

int get_ip_route(int bid, IP_ROUTE *ip_route)
{
	if (bid < 0 || ip_route == NULL)
		return PARAM_ERR;
	HANDLE_PTR ca_handler;
	int ret = init_shm(&ca_handler);
	if (ret < 0) {
		detach_handler(&ca_handler);
		return ret;
	}

	FORWARD_ITEM *master_forward = ca_handler.forward_ptr;
	FORWARD_ITEM *slave_forward = ca_handler.forward_ptr + 1;
	IP_NODE *master_app_set = ca_handler.master_app_set;
	IP_NODE *slave_app_set = ca_handler.slave_app_set;
	if (!master_forward->lock) {
		//		printf("master!\n");
		int pos = binary_search_header(master_forward->headers, 0,
					       master_forward->bid_size, bid);
		if (pos < 0) {
			detach_handler(&ca_handler);
			return -NOT_FIND_BID;
		}
		if (master_forward->headers[pos].offset < 0) {
			detach_handler(&ca_handler);
			return -OFFSET_ERR;
		}
		ip_route->ip_num = master_forward->headers[pos].size;
		if (ip_route->ip_num <= 0)
			return -IP_NUM_ERR;
		ip_route->ip_list =
			(IP_NODE *)malloc(sizeof(IP_NODE) * (ip_route->ip_num));
		memcpy(ip_route->ip_list,
		       master_app_set + master_forward->headers[pos].offset,
		       sizeof(IP_NODE) * (ip_route->ip_num));
	} else {
		//		printf("slave!\n");
		int pos = binary_search_header(slave_forward->headers, 0,
					       slave_forward->bid_size, bid);
		if (pos < 0) {
			detach_handler(&ca_handler);
			return -NOT_FIND_BID;
		}
		if (slave_forward->headers[pos].offset < 0) {
			detach_handler(&ca_handler);
			return -OFFSET_ERR;
		}
		ip_route->ip_num = slave_forward->headers[pos].size;
		if (ip_route->ip_num <= 0)
			return -IP_NUM_ERR;
		ip_route->ip_list =
			(IP_NODE *)malloc(sizeof(IP_NODE) * (ip_route->ip_num));
		memcpy(ip_route->ip_list,
		       slave_app_set + slave_forward->headers[pos].offset,
		       sizeof(IP_NODE) * (ip_route->ip_num));
	}

	return detach_handler(&ca_handler);
}

int free_ip_route(IP_ROUTE *ip_route)
{
	if (ip_route) {
		ip_route->ip_num = 0;
		if (ip_route->ip_list) {
			free(ip_route->ip_list);
			ip_route->ip_list = NULL;
		}
	}
	return 0;
}
