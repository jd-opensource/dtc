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

#include "plugin_proxy_listener_pool.h"
#include "plugin_global.h"
#include "listener/listener.h"
#include "../poll/poller_base.h"
#include "plugin_unit.h"
#include "socket/unix_socket.h"
#include "../config/config.h"
#include "../log/log.h"
#include "mem_check.h"

PluginAgentListenerPool::PluginAgentListenerPool(void)
{
	memset(_listener, 0, sizeof(_listener));
	memset(_thread, 0, sizeof(_thread));
	memset(_decoder, 0, sizeof(_decoder));
	memset(_udpfd, 0xff, sizeof(_udpfd));
}

PluginAgentListenerPool::~PluginAgentListenerPool(void)
{
	for (int i = 0; i < MAXLISTENERS; i++) {
		if (_thread[i]) {
			_thread[i]->interrupt();
		}

		DELETE(_listener[i]);
		DELETE(_decoder[i]);

		if (_udpfd[i] >= 0) {
			close(_udpfd[i]);
		}
	}
}

int PluginAgentListenerPool::init_decoder(int n, int idle)
{
	if (_thread[n] == NULL) {
		char name[16];
		snprintf(name, sizeof(name) - 1, "plugin_inc%d", n);

		_thread[n] = NULL;
		NEW(PollerBase(name), _thread[n]);
		if (NULL == _thread[n]) {
			log4cplus_error("create PollerBase object failed, %m");
			return -1;
		}
		if (_thread[n]->initialize_thread() == -1) {
			return -1;
		}

		_decoder[n] = NULL;
		NEW(PluginDecoderUnit(_thread[n], idle), _decoder[n]);
		if (NULL == _decoder[n]) {
			log4cplus_error(
				"create PluginDecoderUnit object failed, %m");
			return -1;
		}

		if (_decoder[n]->attach_incoming_notifier() != 0) {
			log4cplus_error("attach incoming notifier failed.");
			return -1;
		}
	}

	return 0;
}

int PluginAgentListenerPool::do_bind()
{
	int succ_count = 0;

	int idle = PluginGlobal::_idle_timeout;
	if (idle < 0) {
		log4cplus_info("idle_timeout invalid, use default value: 100");
		idle = 100;
	}
	idle = 100;

	int single = PluginGlobal::_single_incoming_thread;
	int backlog = PluginGlobal::_max_listen_count;
	int win = PluginGlobal::_max_request_window;

	for (int i = 0; i < 1; i++) // Only One
	{
		const char *errmsg = NULL;
		int rbufsz = 0;
		int wbufsz = 0;

		const char *addrStr = PluginGlobal::_bind_address;
		if (addrStr == NULL) {
			continue;
		}

		const char *port = NULL;
		errmsg = _sockaddr[i].set_address(addrStr, port);
		if (errmsg) {
			log4cplus_error("bad BindAddr%d/BindPort%d: %s\n", i, i,
					errmsg);
			continue;
		}

		int n = single ? 0 : i;
		if (_sockaddr[i].socket_type() ==
		    SOCK_DGRAM) { // DGRAM listener
			rbufsz = PluginGlobal::_udp_recv_buffer_size;
			wbufsz = PluginGlobal::_udp_send_buffer_size;
		} else {
			// STREAM socket listener
			rbufsz = wbufsz = 0;
		}

		_listener[i] = new DTCListener(&_sockaddr[i]);
		_listener[i]->set_request_window(win);
		if (_listener[i]->do_bind(backlog, rbufsz, wbufsz) != 0) {
			if (i == 0) {
				log4cplus_error("Error bind unix-socket");
				return -1;
			} else {
				continue;
			}
		}

		if (init_decoder(n, idle) != 0) {
			return -1;
		}

		if (_listener[i]->do_attach(_decoder[n], backlog) < 0) {
			return -1;
		}

		//inc succ count
		succ_count++;
	}

	for (int i = 0; i < MAXLISTENERS; i++) {
		if (_thread[i] == NULL) {
			continue;
		}

		_thread[i]->running_thread();
	}

	if (0 == succ_count) {
		log4cplus_error("all plugin bind address & port invalid.");
		return -1;
	}

	return 0;
}

int PluginAgentListenerPool::Match(const char *name, int port)
{
	for (int i = 0; i < MAXLISTENERS; i++) {
		if (_listener[i] == NULL) {
			continue;
		}

		if (_sockaddr[i].Match(name, port)) {
			return 1;
		}
	}

	return 0;
}

int PluginAgentListenerPool::Match(const char *name, const char *port)
{
	for (int i = 0; i < MAXLISTENERS; i++) {
		if (_listener[i] == NULL) {
			continue;
		}

		if (_sockaddr[i].Match(name, port)) {
			return 1;
		}
	}

	return 0;
}
