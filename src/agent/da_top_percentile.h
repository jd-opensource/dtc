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
#ifndef DA_TOP_PERCENTILE_H_
#define DA_TOP_PERCENTILE_H_

#include <stdint.h>

struct context;
struct server_pool;

enum E_REPORT_TYPE { RT_MIN, RT_SHARDING, RT_ALL, RT_MAX };

struct remote_param {
  uint64_t app_id;
  uint64_t interface_id;
};

int8_t get_host_name_info(const char *addr, char *result);
int8_t set_remote_config(const char *addr, uint16_t port,
                         struct sockaddr_in *remote_addr);
int8_t set_remote_param(uint64_t app_id, uint64_t interface_id,
                        enum E_REPORT_TYPE type, struct remote_param *pParam);
int set_remote_fd();
void top_percentile_report(struct context *ctx, struct server_pool *pool,
                           int64_t elaspe, int32_t status,
                           enum E_REPORT_TYPE type);

#endif
