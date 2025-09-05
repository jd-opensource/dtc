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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "da_top_percentile.h"
#include "da_core.h"
#include "da_server.h"


#define ADDR_LEN 16
#define MSG_SIZE 1024


int8_t get_host_name_info(const char *addr, char *result)
{
	struct addrinfo *answer;
	struct addrinfo *curr;
	struct addrinfo hint;
	struct sockaddr_in *sk_addr;
	char szIP[ADDR_LEN];
	bzero(&hint, sizeof(hint));
	hint.ai_family = AF_INET;
	hint.ai_socktype = SOCK_DGRAM;

	int8_t ret = getaddrinfo(addr, NULL, &hint, &answer);
	if(ret != 0)
		return -1;

	//Normally there should be only one IP, so only take the first one
	for(curr = answer; curr != NULL; curr = curr->ai_next)
	{
		sk_addr = (struct sockaddr_in *)(curr->ai_addr);
		inet_ntop(AF_INET, &(sk_addr->sin_addr), szIP, ADDR_LEN);
		break;
	}
	freeaddrinfo(answer);
	strncpy(result, szIP, ADDR_LEN);
	log_debug("remote ip is : [%s]", szIP);
	return 0;
}

int8_t set_remote_config(const char *addr, uint16_t port, struct sockaddr_in *remote_addr)
{
	if(NULL == addr)
		return -1;
	if(0 == port)
		return -1;
	if(NULL == remote_addr)
		return -1;
	int8_t ret = 0;
	//Since domain names are not used and addr is directly an IP, no get_host_name_info conversion is needed
	/*
	char szIP[ADDR_LEN];
	memset(szIP, 0, ADDR_LEN);
	ret = get_host_name_info(addr, szIP);
	if(0 != ret)
		return -1;
	*/
	bzero((void*)remote_addr, sizeof(*remote_addr));
	ret = inet_pton(AF_INET, addr, &(remote_addr->sin_addr));
	if(1 != ret)
		return -1;
	remote_addr->sin_family = AF_INET;
	remote_addr->sin_port = htons(port);
	return 0;
}

int8_t set_remote_param(uint64_t app_id, uint64_t interface_id, enum E_REPORT_TYPE type, struct remote_param *pParam)
{
	if(NULL == pParam)
		return -1;
	if((int8_t)type <= (int8_t)RT_MIN || (int8_t)type >= (int8_t)RT_MAX)
		return -1;
	pParam[(int8_t)type - 1].app_id = app_id;
	pParam[(int8_t)type - 1].interface_id = interface_id;
	return 0;
}

static int build_remote_socket()
{
	int fd = socket(AF_INET, SOCK_DGRAM, 0);
	if(fd < 0)
		return -1;
	return fd;
}

int set_remote_fd()
{
	int fd = build_remote_socket();
	if(fd < 0)
		return -1;
	return fd;
}

void top_percentile_report(struct context *ctx, struct server_pool *pool, int64_t elaspe, int32_t status, enum E_REPORT_TYPE type)
{
	if(NULL == ctx || NULL == pool)
	{
		log_debug("ctx or pool is null!");
		return;
	}
	
	if((int8_t)type <= (int8_t)RT_MIN || (int8_t)type >= (int8_t)RT_MAX)
	{
		log_debug("the type <= RT_MIN or the type >= RT_MAX");
		return;
	}

	if(0 == pool->top_percentile_enable)
	{
		log_debug("the top_percentile_enable is 0");
		return;
	}

	if(pool->top_percentile_fd < 0)
	{
		log_debug("the top_percentile_fd < 0");
		return;
	}

	if(0 == pool->top_percentile_addr_len)
	{
		log_debug("the top_percentile_addr_len is 0");
		return;
	}

	if(NULL == pool->top_percentile_param)
	{
		log_debug("the top_percentile_param is null");
		return;
	}
	
	char szMsg[MSG_SIZE];
	memset(szMsg, 0, MSG_SIZE);
	int off = snprintf(szMsg, sizeof(szMsg), "{\"app_id\":%"PRIu64",\"interface_id\":%"PRIu64",\"call_time\":%"PRIu64",\"status\":%d}", pool->top_percentile_param[(int8_t)type - 1].app_id, pool->top_percentile_param[(int8_t)type - 1].interface_id, elaspe, status);
	sendto(pool->top_percentile_fd, szMsg, off, 0, (const struct sockaddr *)&(pool->top_percentile_addr), pool->top_percentile_addr_len);

	log_debug("send msg is : [%s]", szMsg);
}
