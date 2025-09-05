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
#include <stdio.h>
#include "app_client_set.h"

int main(int argc, char **argv)
{
	if (argc < 2) {
		dump_ca_shm();
	} else {
		int bid = atoi(argv[1]);
		uint64_t version = 0;
		get_version(&version);
		IP_ROUTE route;
		int ret = get_ip_route(bid, &route);
		if (ret < 0) {
			printf("get ip route error %d\n", ret);
			return -1;
		}
		IP_NODE *ips =
			(IP_NODE *)malloc(sizeof(IP_NODE) * route.ip_num);
		IP_NODE *head_ips = ips;
		int ip_num = route.ip_num;
		memcpy(ips, route.ip_list, sizeof(IP_NODE) * route.ip_num);
		free_ip_route(&route);
		if (ret >= 0) {
			printf("bid=[%d]  count=[%d] timestamp=[%lu]\n", bid,
			       ip_num, version);
			int i = 0;
			for (; i < ip_num; ++i) {
				printf("bid=%dip=%sport=%dstatus=%dweight=%d\n",
				       ips->bid, ips->ip, ips->port,
				       ips->status, ips->weight);
				ips += 1;
			}
		} else {
			if (head_ips) {
				free(head_ips);
				head_ips = NULL;
				ips = NULL;
			}
			printf("can not find bid=[%d] ip list\n", bid);
		}
		if (head_ips) {
			free(head_ips);
			head_ips = NULL;
			ips = NULL;
		}
	}
	return 0;
}
