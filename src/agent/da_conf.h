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

#ifndef DA_CONF_H_
#define DA_CONF_H_

#include "da_hashkit.h"
#include "da_string.h"
#include "mxml.h"

#define CONF_ERROR (void *)"has an invalid value"
#define CONF_OK (void *)NULL

#define CONF_UNSET_HASH (hash_type_t) - 1
#define CONF_UNSET_PTR NULL
#define CONF_UNSET_NUM -1
#define CONF_DEFAULT_SERVERS 8

#define CONF_DEFAULT_POOL 8
#define CONF_DEFAULT_INSTANCE 8
#define CONF_DEFAULT_HASH HASH_CHASH
#define CONF_DEFAULT_TIMEOUT -1
#define CONF_DEFAULT_LISTEN_BACKLOG 512
#define CONF_DEFAULT_CLIENT_CONNECTIONS 0
#define CONF_DEFAULT_REDIS false
#define CONF_DEFAULT_PRECONNECT false
#define CONF_DEFAULT_AUTO_EJECT_HOSTS false
#define CONF_DEFAULT_SERVER_RETRY_TIMEOUT 30 * 1000 /* in msec */
#define CONF_DEFAULT_SERVER_FAILURE_LIMIT 2
#define CONF_DEFAULT_SERVER_CONNECTIONS 1
#define CONF_DEFAULT_TOP_PERCENTILE_ENABLE 1
#define CONF_DEFAULT_TOP_PERCENTILE_DOMAIN "127.0.0.1"
#define CONF_DEFAULT_TOP_PERCENTILE_PORT 20020
#define CONF_DEFAULT_LOG_SWITCH 0 /*1:on, 0:off*/
#define CONF_DEFAULT_REMOTE_LOG_SWITCH 1 /*1:on, 0:off*/
#define CONF_DEFAULT_REMOTE_LOG_IP "127.0.0.1"
#define CONF_DEFAULT_REMOTE_LOG_PORT 9997

struct conf_listen {
	struct string pname; /* listen: as "name:port" */
	struct string name; /* name */
	int port; /* port */
	struct sockinfo info; /* listen socket info */
	unsigned valid : 1; /* valid? */
};

struct conf_server {
	struct string name; /* name */
	struct array instance; /* instance */
	int replica_enable;
	struct string idc;
	unsigned valid : 1; /* valid? */
};

struct conf_instance {
	struct string addr; /* server: as "name:port:weight" */
	int port; /* port */
	int weight; /* weight */
	struct sockinfo info; /* connect socket info */
	struct string idc; /* idc */
	struct string role; /* role */
	unsigned valid : 1; /* valid? */
	int enable; /* Enable */
};

struct conf_pool {
	int mid;
	struct string name; /* pool name (root node) */
	struct string accesskey;
	struct conf_listen listen; /* listen: */
	hash_type_t hash; /* hash: */
	int timeout; /* timeout: */
	int backlog; /* backlog: */
	int client_connections; /* client_connections: */
	int preconnect; /* preconnect: */
	int server_connections; /* server_connections: */
	struct array server; /* servers: conf_server[] */
	struct string idc; /* idc*/
	int replica_enable; /* ReplicaEnable*/
	int main_report;
	int instance_report;
	int auto_remove_replica;

	int top_percentile_enable; /* tp99 performance metrics enable status */
	struct string top_percentile_domain; /* tp99 performance metrics reporting server address */
	int top_percentile_port; /* tp99 performance metrics reporting server port */

	char localip[16]; /* Local IP, placed at this position */
	unsigned valid : 1; /* valid? */
};

struct conf_log {
	int log_switch;
	int remote_log_switch;
	struct string remote_log_ip;
	int remote_log_port;
};

struct conf {
	char *fname; /* file name (ref in argv[]) */
	FILE *fh; /* file handle */
	struct string arg; /* string[] (parsed {key, value} pairs) */
	struct array pool; /* conf_pool[] (parsed pools) */
	mxml_node_t *tree;
	char localip[16];
	struct conf_log stCL;
	unsigned parsed : 1; /* parsed? */
	unsigned valid : 1; /* valid? */
};

struct command {
	struct string name;
	char *(*set)(struct conf *cf, struct command *cmd, void *data);
	int offset;
};

#define null_command                                                           \
	{                                                                      \
		null_string, NULL, 0                                           \
	}

int conf_server_each_transform(void *elem, void *data);
int conf_pool_each_transform(void *elem, void *data);
struct conf *conf_create(char *filename);
char *conf_set_string(struct conf *cf, struct command *cmd, void *conf);
char *conf_add_server(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_bool(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_num(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_hash(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_listen(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_addr(struct conf *cf, struct command *cmd, void *conf);
void conf_destroy(struct conf *cf);
#endif /* DA_CONF_H_ */
