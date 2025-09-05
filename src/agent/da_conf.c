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
#include <fcntl.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "da_log.h"
#include "da_conf.h"
#include "da_util.h"
#include "da_top_percentile.h"

#define DEFINE_ACTION(_hash, _name) string(#_name),
static struct string hash_strings[] = {
HASH_CODEC( DEFINE_ACTION )
null_string };
#undef DEFINE_ACTION

#define DEFINE_ACTION(_hash, _name) hash_##_name,
static hash_t hash_algos[] = { HASH_CODEC(DEFINE_ACTION)
NULL };
#undef DEFINE_ACTION

static struct command conf_commands[] = {
		{ string("Mid"), conf_set_num, offsetof(struct conf_pool, mid) },
		{ string("AccessToken"), conf_set_string, offsetof(struct conf_pool,accesskey) },
		{ string("ListenOn"), conf_set_listen, offsetof(struct conf_pool, listen) },
		{ string("Backlog"), conf_set_num, offsetof(struct conf_pool, backlog) },
		{ string("Client_Connections"), conf_set_num, offsetof(struct conf_pool,client_connections) },
		{ string("Preconnect"), conf_set_bool, offsetof(struct conf_pool, preconnect) },
		{ string("Server_Connections"), conf_set_num, offsetof(struct conf_pool,server_connections) },
		{ string("Hash"), conf_set_hash, offsetof(struct conf_pool, hash) },
		{ string("Timeout"), conf_set_num, offsetof(struct conf_pool, timeout) },	
		{ string("ReplicaEnable"), conf_set_bool, offsetof(struct conf_pool, replica_enable) },
		{ string("ModuleIDC"), conf_set_string, offsetof(struct conf_pool, idc) },		
		{ string("MainReport"), conf_set_bool, offsetof(struct conf_pool, main_report) },
		{ string("InstanceReport"), conf_set_bool, offsetof(struct conf_pool, instance_report) },
		{ string("AutoRemoveReplica"), conf_set_bool, offsetof(struct conf_pool, auto_remove_replica) },
		{ string("TopPercentileEnable"), conf_set_bool, offsetof(struct conf_pool, top_percentile_enable) },
		{ string("TopPercentileDomain"), conf_set_string, offsetof(struct conf_pool, top_percentile_domain) },
		{ string("TopPercentilePort"), conf_set_num, offsetof(struct conf_pool, top_percentile_port) },
		//For Sharding 
		{ string("ShardingReplicaEnable"), conf_set_bool, offsetof(struct conf_server, replica_enable) },
		{ string("ShardingName"), conf_set_string, offsetof(struct conf_server, name) },
		//instance parameter
		{ string("Role"), conf_set_string, offsetof(struct conf_instance, role) },
		{ string("Enable"), conf_set_bool, offsetof(struct conf_instance, enable) },
		{ string("Addr"), conf_set_addr, offsetof(struct conf_instance, addr) },
		{ string("idc"), conf_set_string, offsetof(struct conf_instance, idc) },
		//for log
		{ string("LogSwitch"), conf_set_num, offsetof(struct conf_log, log_switch) },
		{ string("RemoteLogSwitch"), conf_set_num, offsetof(struct conf_log, remote_log_switch) },
		{ string("RemoteLogIP"), conf_set_string, offsetof(struct conf_log, remote_log_ip) },
		{ string("RemoteLogPort"), conf_set_num, offsetof(struct conf_log, remote_log_port) },
		null_command };

static char *svrpool_elem[] = {
		"Name",//name	
		"Mid",				
		"AccessToken",				//accesstoken
		"ListenOn",					//listen address
		"Backlog",
		"Client_Connections",
		"Preconnect",
		"Server_Connections",
		"Hash",
		"Timeout",
		"ReplicaEnable",
		"ModuleIDC",
		"MainReport",
		"InstanceReport",
		"AutoRemoveReplica",
		"TopPercentileEnable",
		"TopPercentileDomain",
		"TopPercentilePort",
		};

static char *server_elem[] = {
		"ShardingReplicaEnable",
		"ShardingName",             //Server name
		};

static char *instance_elem[] = {
		"Addr",
		"idc",
		"Role",
		"Enable",
};

static char *log_elem[] = {
	"LogSwitch",
	"RemoteLogSwitch",
	"RemoteLogIP",
	"RemoteLogPort",
};

static void conf_instance_init(struct conf_instance *ci) {
	string_init(&ci->addr);
	string_init(&ci->idc);
	ci->port = CONF_UNSET_NUM;
	string_init(&ci->role);
	ci->weight = CONF_UNSET_NUM;
	ci->enable = CONF_UNSET_NUM;
	ci->valid = 1;

	log_debug("init conf instance %p", ci);
}

static void conf_instance_deinit(struct conf_instance *ci) {

	
	string_deinit(&ci->addr);	
	string_deinit(&ci->idc);
	string_deinit(&ci->role);
	ci->valid = 0;
	log_debug("deinit conf instance %p", ci);
}


static int conf_server_init(struct conf_server *cs) {
	string_init(&cs->name);
	string_init(&cs->idc);
	int status;
	cs->replica_enable = CONF_UNSET_NUM;
	status = array_init(&cs->instance, CONF_DEFAULT_INSTANCE, sizeof(struct conf_instance));
	if (status != 0) {
		string_deinit(&cs->name);
		return -1;
	}
	cs->valid = 0;
	log_debug("init conf server %p", cs);
	return 0;
}

static void conf_server_deinit(struct conf_server *cs) {
	string_deinit(&cs->name);
	string_deinit(&cs->idc);
	while (array_n(&cs->instance) != 0) {
		conf_instance_deinit(array_pop(&cs->instance));
	}
	array_deinit(&cs->instance);
	cs->valid = 0;
	log_debug("deinit conf server %p", cs);
}

int conf_instance_transform(void *elem,void *data) {
	struct conf_instance *ci = elem;
	struct cache_instance *i = data;
	i->addrlen = ci->info.addrlen;
	i->family = ci->info.family;
	i->port = ci->port;
	i->weight = ci->weight;
	i->addr = (struct sockaddr *) &ci->info.addr.in;
	string_copy(&i->pname, ci->addr.data, ci->addr.len);
	return 0;
}

int conf_server_each_transform(void *elem, void *data) {
	struct conf_server *cs = elem;
	struct conf_instance *ci;
	struct array *server = data;
	struct server *s;
	struct cache_instance *cachei = NULL;
	int ninstance, status, i, cnt;

	const char *master_char = "master";
	struct string master_str; 
	string_copy(&master_str,(uint8_t *) master_char, strlen(master_char));

	ASSERT(cs->valid);

	s = array_push(server);
	ASSERT(s != NULL);

	ninstance = array_n(&cs->instance);

	s->idx = array_idx(server, s);
	s->owner = NULL;
	string_copy(&s->name, cs->name.data, cs->name.len);
	s->weight = 1;
	s->replica_enable = cs->replica_enable;

	status = array_init(&s->high_ptry_ins, ninstance, sizeof(struct cache_instance));
	if (status != 0)
	{
		return -1;
		log_error("transform from conf to server error : can not calloc mem for instance");
	}

	status = array_init(&s->low_prty_ins, ninstance, sizeof(struct cache_instance));
	if (status != 0)
	{
		array_deinit(&s->high_ptry_ins);
		log_error("transform from conf to server error : can not calloc mem for instance");
		return -1;
	}

	//for every conf instance, transform to cache_instance 
	cnt = 1;
	for (i = 0; i < ninstance; i++)
	{
		
		ci = array_get(&cs->instance, i);
		ASSERT(ci != NULL);			
		if (string_compare(&ci->role, &master_str) == 0 && ci->enable != 0) {
			if (string_compare(&ci->idc, &cs->idc) == 0) {
				cachei = array_push(&s->high_ptry_ins);
			}
			else {
				cachei = array_push(&s->low_prty_ins);
			}
			
			cachei->idx = 0;
			s->master = cachei;
			
		}
		else if (string_compare(&ci->role, &master_str) == 0 && ci->enable == 0){
			log_error("master instance should be enable");
			return -1;
		}

		else if (cs->replica_enable != 0 && ci->enable != 0 && string_compare(&ci->idc, &cs->idc) == 0) {
			cachei = array_push(&s->high_ptry_ins);
			cachei->idx = cnt++;
		}
		else if (cs->replica_enable != 0 && ci->enable != 0 && string_compare(&ci->idc, &cs->idc) != 0 ) {
			cachei = array_push(&s->low_prty_ins);
			cachei->idx = cnt++;
			
		}
		else{
			continue;
		}
		conf_instance_transform(ci, cachei);
		cachei->owner = s;
		cachei->ns_conn_q = 0;
		cachei->nerr = 0;
		cachei->failure_num = 0;
		cachei->num = 0;
		TAILQ_INIT(&cachei->s_conn_q);
	}
	
	
	s->high_prty_idx = 0;
	s->high_prty_cnt = 0;
	s->low_prty_idx = 0;
	s->low_prty_cnt = 0;

	log_debug("transform to server %"PRIu32" '%.*s'", s->idx, s->name.len,
			s->name.data);

	return 0;
}

static int conf_pool_init(struct conf_pool *cp, struct string *name) {
	int status;

	string_init(&cp->name);
	string_init(&cp->accesskey);
	string_init(&cp->top_percentile_domain);

	string_init(&cp->listen.pname);
	string_init(&cp->listen.name);
	cp->listen.port = 0;
	memset(&cp->listen.info, 0, sizeof(cp->listen.info));
	cp->listen.valid = 0;

	cp->hash = CONF_UNSET_HASH;
	cp->mid = CONF_UNSET_NUM;
	cp->timeout = CONF_UNSET_NUM;
	cp->backlog = CONF_UNSET_NUM;

	cp->client_connections = CONF_UNSET_NUM;
	

	cp->preconnect = CONF_UNSET_NUM;
	cp->server_connections = CONF_UNSET_NUM;

	array_null(&cp->server);
	cp->replica_enable = CONF_UNSET_NUM;
	cp->main_report = CONF_UNSET_NUM;
	cp->instance_report = CONF_UNSET_NUM;
	cp->auto_remove_replica = CONF_UNSET_NUM;
	cp->top_percentile_enable = CONF_UNSET_NUM;
	cp->top_percentile_port = CONF_UNSET_NUM;

	string_init(&cp->idc);
	cp->valid = 0;

	status = string_duplicate(&cp->name, name);
	if (status != 0) {
		return status;
	}

	status = array_init(&cp->server, CONF_DEFAULT_SERVERS,
			sizeof(struct conf_server));
	if (status != 0) {
		string_deinit(&cp->name);
		return status;
	}

	log_debug("init conf pool %p, '%.*s'", cp, name->len, name->data);

	return 0;
}

static void conf_pool_deinit(struct conf_pool *cp) {

	string_deinit(&cp->name);
	string_deinit(&cp->listen.pname);
	string_deinit(&cp->listen.name);
	string_deinit(&cp->idc);
	string_deinit(&cp->top_percentile_domain);

	while (array_n(&cp->server) != 0) {
		conf_server_deinit(array_pop(&cp->server));
	}
	array_deinit(&cp->server);

	log_debug("deinit conf pool %p", cp);
}

static void conf_log_init(struct conf_log *cl){
	cl->log_switch = CONF_UNSET_NUM;
	cl->remote_log_switch = CONF_UNSET_NUM;
	cl->remote_log_port = CONF_UNSET_NUM;
	string_init(&cl->remote_log_ip);
}

static void conf_log_deinit(struct conf_log *cl){
	string_deinit(&cl->remote_log_ip);
	log_debug("deinit conf log %p", cl);
}

int conf_pool_each_transform(void *elem, void *data) {
	int status;
	struct conf_pool *cp = elem;
	struct array *server_pool = data;
	struct server_pool *sp;

	ASSERT(cp->valid);

	sp = array_push(server_pool);
	ASSERT(sp != NULL);

	sp->idx = array_idx(server_pool, sp);
	sp->mid = cp->mid;
	sp->ctx = NULL;

	sp->listener = NULL;
	sp->c_conn_count = 0;
	TAILQ_INIT(&sp->c_conn_q);

	array_null(&sp->server);
	sp->ncontinuum = 0;
	sp->nserver_continuum = 0;
	sp->continuum = NULL;

	string_copy(&sp->name, cp->name.data, cp->name.len);
	string_copy(&sp->accesskey, cp->accesskey.data, cp->accesskey.len);
	string_copy(&sp->addrstr, cp->listen.pname.data, cp->listen.pname.len);

	sp->port = (uint16_t) cp->listen.port;

	sp->family = cp->listen.info.family;
	sp->addrlen = cp->listen.info.addrlen;
	sp->addr = (struct sockaddr *) &cp->listen.info.addr;

	sp->key_hash_type = cp->hash;
	sp->key_hash = hash_algos[cp->hash];

	sp->backlog = cp->backlog;
	sp->timeout = cp->timeout;
	sp->client_connections = (uint32_t) cp->client_connections;
	sp->server_connections = (uint32_t) cp->server_connections;
	sp->preconnect = cp->preconnect ? 1 : 0;
	sp->replica_enable = cp->replica_enable ? 1 : 0;
	string_copy(&sp->module_idc, cp->idc.data, cp->idc.len);

	sp->main_report = cp->main_report;
	sp->instance_report = cp->instance_report;
	sp->auto_remove_replica = cp->auto_remove_replica;

	sp->top_percentile_enable = cp->top_percentile_enable;
	sp->top_percentile_fd = set_remote_fd();
	sp->top_percentile_addr_len = set_remote_config((char *)cp->top_percentile_domain.data, (uint16_t)cp->top_percentile_port, &sp->top_percentile_addr);
	if(0 == sp->top_percentile_addr_len)
		sp->top_percentile_addr_len = sizeof(sp->top_percentile_addr);
	else
		sp->top_percentile_addr_len = 0;
	sp->top_percentile_param = malloc(sizeof(struct remote_param) * ((int8_t)RT_MAX - 1));
	if(sp->top_percentile_param)
	{
		char szBid[16];
		snprintf(szBid, 9, "%s", sp->accesskey.data);
		uint64_t uBid = (uint64_t)(atoi(szBid));
		uint64_t uIP = (uint64_t)(ntohl(inet_addr(cp->localip)));
		uint64_t uConstValue = 1;
		set_remote_param((uBid << 32) | uIP, (uConstValue << 32) | sp->port, RT_SHARDING, sp->top_percentile_param);
		set_remote_param(uBid, 1, RT_ALL, sp->top_percentile_param);
	}

	status = server_init(&sp->server, &cp->server, sp);
	if (status != 0) {
		return status;
	}

	log_debug("transform to pool %"PRIu32" '%.*s'", sp->idx, sp->name.len,
			sp->name.data);

	return 0;
}

static struct conf *conf_open(char *filename) {
	int status;
	struct conf *cf;
	FILE *fh;

	fh = fopen(filename, "r");
	if (fh == NULL) {
		log_error("conf: failed to open configuration '%s': %s", filename,
				strerror(errno));
		return NULL;
	}

	cf = malloc(sizeof(*cf));
	if (cf == NULL) {
		fclose(fh);
		return NULL;
	}

	string_init(&cf->arg);

	status = array_init(&cf->pool, CONF_DEFAULT_POOL, sizeof(struct conf_pool));
	if (status != 0) {
		string_deinit(&cf->arg);
		free(cf);
		fclose(fh);
		return NULL;
	}

	cf->fname = filename;
	cf->fh = fh;
	cf->tree = NULL;
	/* parser, event, and token are initialized later */
	cf->parsed = 0;
	cf->valid = 0;

	log_debug("opened conf '%s'", filename);

	return cf;
}

static int conf_begin_parse(struct conf *cf) {
	ASSERT(!cf->parsed);

	cf->tree = mxmlLoadFile(NULL, cf->fh, MXML_TEXT_CALLBACK);
	if (cf->tree == NULL) {
		return -1;
	}

	return 0;
}

static int conf_handler(char *key, struct conf *cf, void *data) {
	int status;
	struct command *cmd;

	if (da_strncmp(key,"Name",da_strlen(key)) == 0) {
		return conf_pool_init(data, &cf->arg);
	}
	struct string key_str;
	status = string_copy(&key_str, (uint8_t *) key, da_strlen(key));
	{
		if (status < 0) {
			log_debug("no memory when parse xml");
			return -1;
		}
	}
	for (cmd = conf_commands; cmd->name.len != 0; cmd++) {
		char *rv;

		if (string_compare(&key_str, &cmd->name) != 0) {
			continue;
		}

		rv = cmd->set(cf, cmd, data);
		if (rv != CONF_OK) {
			log_error("conf: directive %s %s", key, rv);
			return -1;
		}
		return 0;
	}
	log_error("conf: directive %s is unknown", key);
	return -1;
}

static int conf_parse_core(struct conf *cf) {
	ASSERT(!cf->parsed);ASSERT(cf->tree !=NULL);

	int status, i;
	void *void_cp;
	void *void_cs;
	void *void_ci;
	void *void_cl;
	mxml_node_t *poolnode, *servernode, *instancenode, *lognode;

	for (poolnode = mxmlFindElement(cf->tree, cf->tree, "MODULE",
	NULL, NULL, MXML_DESCEND); poolnode != NULL;
			poolnode = mxmlFindElement(poolnode, cf->tree, "MODULE",
			NULL, NULL, MXML_DESCEND)) {
		// get a conf_pool
		void_cp = array_push(&cf->pool);
		if (void_cp == NULL) {
			return -1;
		}
		
		//store localip to conf_pool for later use by server_pool
		struct conf_pool *tmpCP = (struct conf_pool *)void_cp;
		strncpy(tmpCP->localip, cf->localip, sizeof(tmpCP->localip));
		
		int sz = sizeof(svrpool_elem) / sizeof(svrpool_elem[0]);
		for (i = 0; i < sz; i++) {
			char *argment = (char *) mxmlElementGetAttr(poolnode,
					svrpool_elem[i]);
			if (argment == NULL) {
				log_error("get %s from conf '%s' error", svrpool_elem[i],
						cf->fname);
				return -1;
			}
			status = string_copy(&cf->arg, (uint8_t *) argment,
					da_strlen(argment));
			if (status < 0) {
				return status;
			}
			status = conf_handler(svrpool_elem[i], cf, void_cp);
			if (status < 0) {
				return status;
			}
			string_deinit(&cf->arg);
		}

		for (servernode = mxmlFindElement(poolnode, poolnode, "CACHESHARDING",
		NULL, NULL, MXML_DESCEND); servernode != NULL; servernode =
				mxmlFindElement(servernode, poolnode, "CACHESHARDING",
				NULL, NULL, MXML_DESCEND)) {
			struct conf_pool *cp = (struct conf_pool*) void_cp;
			void_cs = array_push(&cp->server);
			if (void_cs == NULL)
			{
				return -1;
			}
			status = conf_server_init(void_cs);
			if (status != 0)
			{
				array_pop(&cp->server);
				return -1;
			}
			int sz = sizeof(server_elem) / sizeof(server_elem[0]);
			for (i = 0; i < sz; i++) {
				char *argment = (char *) mxmlElementGetAttr(servernode,
						server_elem[i]);
				if (argment == NULL) {					
					log_error("get %s from conf '%s' error", server_elem[i],
							cf->fname);
					array_pop(&cp->server);
					conf_server_deinit(void_cs);
					
					return -1;
				}
				status = string_copy(&cf->arg, (uint8_t *) argment,
						da_strlen(argment));
				if (status < 0) {
					array_pop(&cp->server);
					conf_server_deinit(void_cs);
					return status;
				}
				status = conf_handler(server_elem[i], cf, void_cs);
				if (status < 0) {					
					array_pop(&cp->server);
					conf_server_deinit(void_cs);
					return status;
				}
				
				string_deinit(&cf->arg);
			}
			

			for (instancenode = mxmlFindElement(servernode, servernode, "INSTANCE",
				NULL, NULL, MXML_DESCEND); instancenode != NULL; instancenode =
				mxmlFindElement(instancenode, servernode, "INSTANCE",
					NULL, NULL, MXML_DESCEND)) {
				
				struct conf_server *cs = (struct conf_server *)void_cs;
				string_copy(&cs->idc, cp->idc.data, cp->idc.len);
				void_ci = array_push(&cs->instance);
				if (void_ci == NULL) {
					return -1;
				}
				conf_instance_init(void_ci);
				int sz = sizeof(instance_elem) / sizeof(instance_elem[0]);
				for (i = 0; i < sz; i++) {
					char *argment = (char *)mxmlElementGetAttr(instancenode,
						instance_elem[i]);
					if (argment == NULL) {
						array_pop(&cs->instance);
						conf_instance_deinit(void_ci);
						log_error("get %s from conf '%s' error", server_elem[i],
							cf->fname);
						return -1;
					}
					status = string_copy(&cf->arg, (uint8_t *)argment,
						da_strlen(argment));
					if (status < 0) {
						array_pop(&cs->instance);
						conf_instance_deinit(void_ci);
						return status;
					}
					status = conf_handler(instance_elem[i], cf, void_ci);
					if (status < 0) {
						array_pop(&cs->instance);
						conf_instance_deinit(void_ci);
						return status;
					}
					string_deinit(&cf->arg);
				}
			}

		}
	}

	lognode = mxmlFindElement(cf->tree, cf->tree, "LOG_MODULE", NULL, NULL, MXML_DESCEND);
	void_cl = (void *)&cf->stCL;
	conf_log_init(void_cl);
	int lnSize = sizeof(log_elem) / sizeof(log_elem[0]);
	for(i = 0; i < lnSize; ++ i)
	{
		char *argment = (char *)mxmlElementGetAttr(lognode, log_elem[i]);
		if(NULL == argment)
		{
			log_error("get %s from conf '%s' error", log_elem[i], cf->fname);
			continue;
		}
		status = string_copy(&cf->arg, (uint8_t *)argment, da_strlen(argment));
		if(status < 0)
			return status;
		status = conf_handler(log_elem[i], cf, void_cl);
		if(status < 0)
			return status;
		string_deinit(&cf->arg);
	}
	return 0;
}

char* arg_module[] = {"shardingsphere", "999", "0000", "127.0.0.1:12999", "500", "900", "true", "1", "chash", "3000", "true", 
						"LF", "false", "false", "true", "false", "127.0.0.1", "20020"};
char* cachesharding_module[] = {"false", "ss"};
char* instance_module[] = {"", "LF", "master", "true"};

static int conf_add_ss(struct conf *cf) {

	int status, i;
	void *void_cp;
	void *void_cs;
	void *void_ci;
	void *void_cl;

	// get a conf_pool
	void_cp = array_push(&cf->pool);
	if (void_cp == NULL) {
		return -1;
	}
	
	//store localip to conf_pool for later use by server_pool
	struct conf_pool *tmpCP = (struct conf_pool *)void_cp;
	strncpy(tmpCP->localip, cf->localip, sizeof(tmpCP->localip));
	
	// MODULE
	int sz = sizeof(svrpool_elem) / sizeof(svrpool_elem[0]);
	for (i = 0; i < sz; i++) {
		char *argment = arg_module[i];
		if (argment == NULL) {
			log_error("get %s from conf '%s' error", svrpool_elem[i],
					cf->fname);
			return -1;
		}
		status = string_copy(&cf->arg, (uint8_t *) argment,
				da_strlen(argment));
		if (status < 0) {
			return status;
		}
		status = conf_handler(svrpool_elem[i], cf, void_cp);
		if (status < 0) {
			return status;
		}
		string_deinit(&cf->arg);
	}

	// CACHESHARDING
	struct conf_pool *cp = (struct conf_pool*) void_cp;
	void_cs = array_push(&cp->server);
	if (void_cs == NULL)
	{
		return -1;
	}
	status = conf_server_init(void_cs);
	if (status != 0)
	{
		array_pop(&cp->server);
		return -1;
	}
	sz = sizeof(server_elem) / sizeof(server_elem[0]);
	for (i = 0; i < sz; i++) {
		char *argment = cachesharding_module[i];
		if (argment == NULL) {					
			log_error("get %s from conf '%s' error", server_elem[i],
					cf->fname);
			array_pop(&cp->server);
			conf_server_deinit(void_cs);
			
			return -1;
		}
		status = string_copy(&cf->arg, (uint8_t *) argment,
				da_strlen(argment));
		if (status < 0) {
			array_pop(&cp->server);
			conf_server_deinit(void_cs);
			return status;
		}
		status = conf_handler(server_elem[i], cf, void_cs);
		if (status < 0) {					
			array_pop(&cp->server);
			conf_server_deinit(void_cs);
			return status;
		}
		
		string_deinit(&cf->arg);
	}
	
	// INSTANCE
	struct conf_server *cs = (struct conf_server *)void_cs;
	char ssaddr[250] = {0};
	string_copy(&cs->idc, cp->idc.data, cp->idc.len);
	void_ci = array_push(&cs->instance);
	if (void_ci == NULL) {
		return -1;
	}
	conf_instance_init(void_ci);
	sz = sizeof(instance_elem) / sizeof(instance_elem[0]);
	for (i = 0; i < sz; i++) {
		char *argment = NULL;
		if(i == 0)
		{
			mxml_node_t* ssnode = mxmlFindElement(cf->tree, cf->tree, "SS_MODULE", NULL, NULL, MXML_DESCEND);
			char *ssport = (char *)mxmlElementGetAttr(ssnode, "port");
			if(NULL == ssport)
			{
				log_error("get ssport from conf SS_MODULE error");
				continue;
			}

			sprintf(ssaddr, "127.0.0.1:%s:1", ssport);
			argment = ssaddr;
		}
		else
		{
			argment = (char *)instance_module[i];
		}
		if (argment == NULL) {
			array_pop(&cs->instance);
			conf_instance_deinit(void_ci);
			log_error("get %s from conf '%s' error", server_elem[i],
				cf->fname);
			return -1;
		}
		status = string_copy(&cf->arg, (uint8_t *)argment,
			da_strlen(argment));
		if (status < 0) {
			array_pop(&cs->instance);
			conf_instance_deinit(void_ci);
			return status;
		}
		status = conf_handler(instance_elem[i], cf, void_ci);
		if (status < 0) {
			array_pop(&cs->instance);
			conf_instance_deinit(void_ci);
			return status;
		}
		string_deinit(&cf->arg);
	}

	return 0;
}

static int conf_end_parse(struct conf *cf) {
	mxmlDelete(cf->tree);
	return 0;
}

static int conf_parse(struct conf *cf) {
	int status;

	ASSERT(!cf->parsed);ASSERT(array_n(&cf->arg) == 0);

	status = conf_begin_parse(cf);
	if (status != 0) {
		return status;
	}
	status = conf_parse_core(cf);
	if (status != 0) {
		return status;
	}

	status = conf_add_ss(cf);
	if (status != 0) {
		return status;
	}

	status = conf_end_parse(cf);
	{
		if (status != 0) {
			return status;
		}
	}
	cf->parsed = 1;
	return 0;
}

void conf_destroy(struct conf *cf) {

	string_deinit(&cf->arg);
	conf_log_deinit(&cf->stCL);

	while (array_n(&cf->pool) != 0) {
		conf_pool_deinit(array_pop(&cf->pool));
	}
	array_deinit(&cf->pool);

	free(cf);
}

static int conf_server_name_cmp(const void *t1, const void *t2) {
	const struct conf_server *s1 = t1, *s2 = t2;

	return string_compare(&s1->name, &s2->name);
}

static int conf_pool_listen_cmp(const void *t1, const void *t2) {
	const struct conf_pool *p1 = t1, *p2 = t2;

	return string_compare(&p1->listen.pname, &p2->listen.pname);
}

static int conf_pool_name_cmp(const void *t1, const void *t2) {
	const struct conf_pool *p1 = t1, *p2 = t2;

	return string_compare(&p1->name, &p2->name);
}

static int conf_pool_mid_cmp(const void *t1, const void *t2) {
	const struct conf_pool *p1 = t1, *p2 = t2;

	return p1->mid >= p2->mid;
}

static int conf_validate_server(struct conf *cf, struct conf_pool *cp) {
	uint32_t i, nserver;
	bool valid;

	nserver = array_n(&cp->server);
	if (nserver == 0) {
		log_alert("conf: pool '%.*s' has no servers", cp->name.len,
				cp->name.data);
		return 0;
	}
	array_sort(&cp->server, conf_server_name_cmp);

	for (valid = true, i = 0; i < nserver - 1; i++) {
		struct conf_server *cs1, *cs2;

		cs1 = array_get(&cp->server, i);
		cs2 = array_get(&cp->server, i + 1);

		if (string_compare(&cs1->name, &cs2->name) == 0) {
			log_error("conf: pool '%.*s' has servers with same name '%.*s'",
					cp->name.len, cp->name.data, cs1->name.len, cs1->name.data);
			valid = false;
			break;
		}
	}

	if (!valid) {
		return -1;
	}

	return 0;
}

static int conf_validate_pool(struct conf *cf, struct conf_pool *cp) {
	int status;

	ASSERT(!cp->valid);
	ASSERT(!string_empty(&cp->name));

	if (!cp->listen.valid) {
		log_error("conf: directive \"listen:\" is missing");
		return -1;
	}

	if (cp->hash == CONF_UNSET_HASH) {
		cp->hash = CONF_DEFAULT_HASH;
	}

	if (cp->timeout == CONF_UNSET_NUM) {
		cp->timeout = CONF_DEFAULT_TIMEOUT;
	}

	if (cp->backlog == CONF_UNSET_NUM) {
		cp->backlog = CONF_DEFAULT_LISTEN_BACKLOG;
	}

	if (cp->client_connections == CONF_UNSET_NUM) {
		cp->client_connections = CONF_DEFAULT_CLIENT_CONNECTIONS;
	}

	if (cp->preconnect == CONF_UNSET_NUM) {
		cp->preconnect = CONF_DEFAULT_PRECONNECT;
	}

	if (cp->top_percentile_enable == CONF_UNSET_NUM){
		cp->top_percentile_enable = CONF_DEFAULT_TOP_PERCENTILE_ENABLE;
	}

	if (string_empty(&cp->top_percentile_domain)){
		string_copy(&cp->top_percentile_domain, (const uint8_t *)(CONF_DEFAULT_TOP_PERCENTILE_DOMAIN), sizeof(CONF_DEFAULT_TOP_PERCENTILE_DOMAIN));
	}

	if (cp->top_percentile_port == CONF_UNSET_NUM){
		cp->top_percentile_port = CONF_DEFAULT_TOP_PERCENTILE_PORT;
	}

	if (cp->server_connections == CONF_UNSET_NUM) {
		cp->server_connections = CONF_DEFAULT_SERVER_CONNECTIONS;
	} else if (cp->server_connections == 0) {
		log_error("conf: directive \"server_connections:\" cannot be 0");
		return -1;
	}

	status = conf_validate_server(cf, cp);
	if (status != 0) {
		return status;
	}

	cp->valid = 1;

	return 0;
}

static int conf_validate_log(struct conf_log *cl)
{
	if(cl->log_switch == CONF_UNSET_NUM)
		cl->log_switch = CONF_DEFAULT_LOG_SWITCH;
	
	if(cl->remote_log_switch == CONF_UNSET_NUM)
		cl->remote_log_switch = CONF_DEFAULT_REMOTE_LOG_SWITCH;

	if(cl->remote_log_port == CONF_UNSET_NUM)
		cl->remote_log_port = CONF_DEFAULT_REMOTE_LOG_PORT;

	if(string_empty(&cl->remote_log_ip))
	{
		int status = string_copy(&cl->remote_log_ip, (const uint8_t *)(CONF_DEFAULT_REMOTE_LOG_IP), sizeof(CONF_DEFAULT_REMOTE_LOG_IP));
		if(status != 0)
			return -1;
	}

	return 0;
}

static int conf_validate(struct conf *cf) {
	int status;
	uint32_t i, npool;
	bool valid;

	ASSERT(cf->parsed);
	ASSERT(!cf->valid);

	npool = array_n(&cf->pool);
	//has no pool in conf
	if (npool == 0) {
		log_alert("conf: %s has no pools", cf->fname);
		return 0;
	}
	/* validate pool */
	for (i = 0; i < npool; i++) {
		struct conf_pool *cp = array_get(&cf->pool, i);
		status = conf_validate_pool(cf, cp);
		if (status != 0) {
			return status;
		}
	}

	/* validate log*/
	status = conf_validate_log(&cf->stCL);
	if(status != 0)
		return status;

	array_sort(&cf->pool, conf_pool_mid_cmp);
	for (valid = true, i = 0; i < npool - 1; i++) {

		struct conf_pool *p1, *p2;

		p1 = array_get(&cf->pool, i);
		p2 = array_get(&cf->pool, i + 1);
		if (p1->mid == p2->mid) {
			log_error("conf: pools '%.*s' and '%.*s' have the same mid:%d "
					, p1->name.len, p1->name.data, p2->name.len,
					p2->name.data, p1->mid);
			valid = false;
			break;
		}
	}

	array_sort(&cf->pool, conf_pool_listen_cmp);
	for (valid = true, i = 0; i < npool - 1; i++) {
		struct conf_pool *p1, *p2;

		p1 = array_get(&cf->pool, i);
		p2 = array_get(&cf->pool, i + 1);

		if (string_compare(&p1->listen.pname, &p2->listen.pname) == 0) {
			log_error("conf: pools '%.*s' and '%.*s' have the same listen "
					"address '%.*s'", p1->name.len, p1->name.data, p2->name.len,
					p2->name.data, p1->listen.pname.len, p1->listen.pname.data);
			valid = false;
			break;
		}
	}
	if (!valid) {
		return -1;
	}

	/* disallow pools with duplicate names */
	array_sort(&cf->pool, conf_pool_name_cmp);
	for (valid = true, i = 0; i < npool - 1; i++) {
		struct conf_pool *p1, *p2;

		p1 = array_get(&cf->pool, i);
		p2 = array_get(&cf->pool, i + 1);

		if (string_compare(&p1->name, &p2->name) == 0) {
			log_error("conf: '%s' has pools with same name %.*s'", cf->fname,
					p1->name.len, p1->name.data);
			valid = false;
			break;
		}
	}
	if (!valid) {
		return -1;
	}
	cf->valid = 1;
	return 0;
}

static void conf_dump(struct conf *cf) {
	uint32_t i, j, k, npool, nserver, ninstance;
	struct conf_pool *cp;
	struct conf_server *s;
	struct conf_instance *ci;

	npool = array_n(&cf->pool);
	if (npool == 0) {
		return;
	}

	printf("%"PRIu32" pools in configuration file '%s'", npool, cf->fname);

	for (i = 0; i < npool; i++) {
		cp = array_get(&cf->pool, i);
		log_debug("pool mid:%d", cp->mid);
		log_debug("%.*s", cp->name.len, cp->name.data);
		log_debug("listen: %.*s", cp->listen.pname.len, cp->listen.pname.data);
		log_debug("timeout: %d", cp->timeout);
		log_debug("backlog: %d", cp->backlog);
		log_debug("hash: %d", cp->hash);
		log_debug("client_connections: %d", cp->client_connections);
		log_debug("preconnect: %d", cp->preconnect);
		log_debug("server_connections: %d", cp->server_connections);
		log_debug("idc : %.*s", cp->idc.len , cp->idc.data);
		log_debug("ReplicaEnable : %d", cp->replica_enable);


		nserver = array_n(&cp->server);
		log_debug("  servers: %"PRIu32"", nserver);

		for (j = 0; j < nserver; j++) {
			s = array_get(&cp->server, j);
			log_debug("    %.*s", s->name.len, s->name.data);
			ninstance = array_n(&s->instance);
			log_debug("		instances: %"PRIu32"", ninstance);
			for (k = 0; k < ninstance; k++)
			{
				ci = array_get(&s->instance, k);
				log_debug("	    %.*s", ci->addr.len, ci->addr.data);
				log_debug("		Enable : %d", ci->enable);
				log_debug("		idc : %.*s", ci->idc.len, ci->idc.data);
				log_debug("		port : %d", ci->port);
				log_debug("		role : %.*s", ci->role.len, ci->role.data);
				log_debug("		weight : %d", ci->weight);
			}
		}
	}
}

static void get_local_ip(char * out, size_t len)
{
	strncpy(out, "0.0.0.0", len);
	int iClientSockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if(iClientSockfd < 0 )
		return ;

	struct sockaddr_in stINETAddr;
	stINETAddr.sin_addr.s_addr = inet_addr("192.168.0.1");
	stINETAddr.sin_family = AF_INET;
	stINETAddr.sin_port = htons(8888);

	int iCurrentFlag = fcntl(iClientSockfd, F_GETFL, 0);
	fcntl(iClientSockfd, F_SETFL, iCurrentFlag | FNDELAY);

	if(connect(iClientSockfd, (struct sockaddr *)&stINETAddr, sizeof(struct sockaddr)) != 0) {
		close(iClientSockfd);
		return ;
	}

	struct sockaddr_in stINETAddrLocal;
	socklen_t iAddrLenLocal = sizeof(stINETAddrLocal);
	getsockname(iClientSockfd, (struct sockaddr *)&stINETAddrLocal, &iAddrLenLocal);
	close(iClientSockfd);

	strncpy(out, inet_ntoa(stINETAddrLocal.sin_addr), len);
}

struct conf *conf_create(char *filename) {
	int status;
	struct conf *cf;

	cf = conf_open(filename);
	if (cf == NULL) {
		return NULL;
	}

	//need to use local IP later, so get it first
	get_local_ip(cf->localip, sizeof(cf->localip));
	
	status = conf_parse(cf);
	if (status < 0) {
		goto error;
	}
	status = conf_validate(cf);
	if (status != 0) {
		goto error;
	}

	conf_dump(cf);
	fclose(cf->fh);
	cf->fh = NULL;

	return cf;
error: 
	fclose(cf->fh);
	cf->fh = NULL;
	conf_destroy(cf);
	return NULL;
}

char *conf_set_listen(struct conf *cf, struct command *cmd, void *conf) {
	int status;
	struct string *value;
	struct conf_listen *field;
	uint8_t *p, *name;
	uint32_t namelen;

	p = conf;
	field = (struct conf_listen *) (p + cmd->offset);

	if (field->valid == 1) {
		return "is a duplicate";
	}

	value = &cf->arg;

	status = string_duplicate(&field->pname, value);
	if (status != 0) {
		return CONF_ERROR ;
	}

	if (value->data[0] == '/') {
		name = value->data;
		namelen = value->len;
	} else {
		uint8_t *q, *start, *port;
		uint32_t portlen;

		/* parse "hostname:port" from the end */
		p = value->data + value->len - 1;
		start = value->data;
		q = da_strrchr(p, start, ':');
		if (q == NULL) {
			return "has an invalid \"hostname:port\" format string";
		}

		port = q + 1;
		portlen = (uint32_t) (p - port + 1);

		p = q - 1;

		name = start;
		namelen = (uint32_t) (p - start + 1);

		field->port = da_atoi(port, portlen);
		if (field->port < 0 || !da_valid_port(field->port)) {
			return "has an invalid port in \"hostname:port\" format string";
		}
	}

	status = string_copy(&field->name, name, namelen);
	if (status != 0) {
		return CONF_ERROR ;
	}

	status = da_resolve(&field->name, field->port, &field->info);
	if (status != 0) {
		return CONF_ERROR ;
	}

	field->valid = 1;

	return CONF_OK ;
}

char *conf_set_hash(struct conf *cf, struct command *cmd, void *conf) {
	uint8_t *p;
	hash_type_t *hp;
	struct string *value, *hash;

	p = conf;
	hp = (hash_type_t *) (p + cmd->offset);

	if (*hp != CONF_UNSET_HASH) {
		return "is a duplicate";
	}

	value = &cf->arg;

	for (hash = hash_strings; hash->len != 0; hash++) {
		if (string_compare(value, hash) != 0) {
			continue;
		}

		*hp = hash - hash_strings;

		return CONF_OK ;
	}

	return "is not a valid hash";
}

char *conf_set_num(struct conf *cf, struct command *cmd, void *conf) {
	uint8_t *p;
	int num, *np;
	struct string *value;

	p = conf;
	np = (int *) (p + cmd->offset);

	if (*np != CONF_UNSET_NUM) {
		return "is a duplicate";
	}

	value = &cf->arg;

	num = da_atoi(value->data, value->len);
	if (num < 0) {
		return "is not a number";
	}

	*np = num;

	return CONF_OK ;
}

char *conf_set_bool(struct conf *cf, struct command *cmd, void *conf) {
	uint8_t *p;
	int *bp;
	struct string *value, true_str, false_str;

	p = conf;
	bp = (int *) (p + cmd->offset);

	if (*bp != CONF_UNSET_NUM) {
		return "is a duplicate";
	}

	value = &cf->arg;
	string_set_text(&true_str, "true");
	string_set_text(&false_str, "false");

	if (string_compare(value, &true_str) == 0) {
		*bp = 1;
	} else if (string_compare(value, &false_str) == 0) {
		*bp = 0;
	} else {
		return "is not \"true\" or \"false\"";
	}

	return CONF_OK ;
}





char *conf_set_string(struct conf *cf, struct command *cmd, void *conf) {
	int status;
	uint8_t *p;
	struct string *field, *value;

	p = conf;
	field = (struct string *) (p + cmd->offset);

	if (field->data != CONF_UNSET_PTR) {
		return "is a duplicate";
	}

	value = &cf->arg;

	status = string_duplicate(field, value);
	if (status != 0) {
		return CONF_ERROR ;
	}

	return CONF_OK ;
}

char *conf_set_addr(struct conf *cf, struct command *cmd, void *conf) {
	int status;
	struct string *value;
	struct conf_instance *field;
	struct string address;
	char delim[] = "::";
	uint8_t *p, *q, *start;
	uint8_t *pname, *addr, *port, *weight;
	uint32_t k, delimlen, pnamelen, addrlen, portlen, weightlen;

	string_init(&address);

	field = conf;
	value = &cf->arg;
	delimlen = value->data[0] == '/' ? 1 : 2;
	
	p = value->data + value->len - 1;
	start = value->data;

	port = NULL;
	portlen = 0;
	weight = NULL;
	weightlen = 0;
	
	/* resolve the addr info for  ip:port:weight*/
	for (k = 0; k < sizeof(delim); k++) {
		q = da_strrchr(p, start, delim[k]);
		if (q == NULL) {
			if (k == 0) {
				/*
				* name in "hostname:port:weight [name]" format string is
				* optional
				*/
				continue;
			}
			break;
		}

		switch (k) {
		case 0:
			weight = q + 1;
			weightlen = (uint32_t)(p - weight + 1);
			break;

		case 1:
			port = q + 1;
			portlen = (uint32_t)(p - port + 1);
			break;

		default:
			return CONF_ERROR;
		}

		p = q - 1;
	}

	if (k != delimlen) {
		return "has an invalid \"hostname:port:weight [name]\"or \"/path/unix_socket:weight [name]\" format string";
	}

	pname = value->data;
	pnamelen = value->len;

	status = string_copy(&field->addr, pname, pnamelen);
	if (status != 0) {
		return CONF_ERROR;
	}


	field->weight = da_atoi(weight, weightlen);
	if (field->weight < 0) {
		return "has an invalid weight in \"hostname:port:weight [name]\" format string";
	}
	else if (field->weight == 0) {
		return "has a zero weight in \"hostname:port:weight [name]\" format string";
	}

	if (value->data[0] != '/') {
		field->port = da_atoi(port, portlen);
		if (field->port < 0 || !nc_valid_port(field->port)) {
			return "has an invalid port in \"hostname:port:weight [name]\" format string";
		}
	}

	addr = start;
	addrlen = (uint32_t)(p - start + 1);

	status = string_copy(&address, addr, addrlen);
	if (status != 0) {
		return CONF_ERROR;
	}


	status = da_resolve(&address, field->port, &field->info); 
	if (status != 0) {
		string_deinit(&address);
		return CONF_ERROR;
	}

	string_deinit(&address);
	return CONF_OK;
}
