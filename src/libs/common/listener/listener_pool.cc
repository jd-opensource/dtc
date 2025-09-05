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
#include <stdlib.h>

#include "listener/listener_pool.h"
#include "listener/listener.h"
#include "client/client_unit.h"
#include "poll/poller_base.h"
#include "socket/unix_socket.h"
#include "task/task_multi_unit.h"
#include "task/task_request.h"
#include "config/config.h"
#include "log/log.h"

// this file is obsoleted, new one is agent_listen_pool.[hc]
extern DTCTableDefinition *g_table_def[];

ListenerPool::ListenerPool(void)
{
	memset(listener, 0, sizeof(listener));
	memset(thread, 0, sizeof(thread));
	memset(decoder, 0, sizeof(decoder));
}

int ListenerPool::init_decoder(int n, int idle,
			       JobAskInterface<DTCJobOperation> *out)
{
	if (thread[n] == NULL) {
		char name[16];
		snprintf(name, sizeof(name), "inc%d", n);
		thread[n] = new PollerBase(name);
		if (thread[n]->initialize_thread() == -1)
			return -1;
		try {
			decoder[n] = new DTCDecoderUnit(thread[n], g_table_def,
							idle);
		} catch (int err) {
			DELETE(decoder[n]);
			return -1;
		}
		if (decoder[n] == NULL)
			return -1;

		JobHubAskChain *taskMultiplexer = new JobHubAskChain(thread[n]);
		taskMultiplexer->get_main_chain()->register_next_chain(out);
		decoder[n]->get_main_chain()->register_next_chain(
			taskMultiplexer);
	}

	return 0;
}

int ListenerPool::do_bind(DTCConfig *gc, JobAskInterface<DTCJobOperation> *out)
{
	bool hasBindAddr = false;
	int idle = gc->get_int_val("cache", "idle_timeout", 100);
	if (idle < 0) {
		log4cplus_info("idle_timeout invalid, use default value: 100");
		idle = 100;
	}
	int single = gc->get_int_val("cache", "SingleIncomingThread", 0);
	int backlog = gc->get_int_val("cache", "MaxListenCount", 256);
	int win = gc->get_int_val("cache", "MaxRequestWindow", 0);

	for (int i = 0; i < MAXLISTENERS; i++) {
		const char *errmsg;
		char bindStr[32];
		int rbufsz;
		int wbufsz;

		if (i == 0) {
			snprintf(bindStr, sizeof(bindStr), "listener.port.dtc");
		} else {
			snprintf(bindStr, sizeof(bindStr), "listener.port.dtc.%d", i);
		}

		int port = gc->get_config_node()["props"][bindStr].as<int>();
        char sz[200] = {0};
        sprintf(sz, "*:%d/tcp", port);
        std::string addrStr = sz;
		if (addrStr.length() == 0)
			continue;
		errmsg = sockaddr[i].set_address(addrStr.c_str(), (const char*)NULL);
		if (errmsg) {
			log4cplus_error("bad BIND_ADDR%d/BindPort%d: %s\n", i,
					i, errmsg);
			continue;
		}

		int n = single ? 0 : i;
		if (sockaddr[i].socket_type() == SOCK_DGRAM) { // DGRAM listener
			rbufsz = gc->get_int_val("cache", "UdpRecvBufferSize",
						 0);
			wbufsz = gc->get_int_val("cache", "UdpSendBufferSize",
						 0);
		} else {
			// STREAM socket listener
			rbufsz = wbufsz = 0;
		}

		listener[i] = new DTCListener(&sockaddr[i]);
		listener[i]->set_request_window(win);
		if (listener[i]->do_bind(backlog, rbufsz, wbufsz) != 0) {
			if (i == 0) {
				log4cplus_error("Error bind unix-socket");
				return -1;
			} else {
				continue;
			}
		}

		if (init_decoder(n, idle, out) != 0)
			return -1;
		if (listener[i]->do_attach(decoder[n], backlog) < 0)
			return -1;
		hasBindAddr = true;
	}
	if (!hasBindAddr) {
		log4cplus_error("Must has a BIND_ADDR");
		return -1;
	}

	for (int i = 0; i < MAXLISTENERS; i++) {
		if (thread[i] == NULL)
			continue;
		thread[i]->running_thread();
	}
	return 0;
}

ListenerPool::~ListenerPool(void)
{
	for (int i = 0; i < MAXLISTENERS; i++) {
		if (thread[i]) {
			thread[i]->interrupt();
			//delete thread[i];
		}
		DELETE(listener[i]);
		DELETE(decoder[i]);
	}
}

int ListenerPool::Match(const char *name, int port)
{
	for (int i = 0; i < MAXLISTENERS; i++) {
		if (listener[i] == NULL)
			continue;
		if (sockaddr[i].Match(name, port))
			return 1;
	}
	return 0;
}

int ListenerPool::Match(const char *name, const char *port)
{
	for (int i = 0; i < MAXLISTENERS; i++) {
		if (listener[i] == NULL)
			continue;
		if (sockaddr[i].Match(name, port))
			return 1;
	}
	return 0;
}
