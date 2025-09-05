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
#ifndef __PLUGIN_LISTENER_POOL_H__
#define __PLUGIN_LISTENER_POOL_H__

#include "daemon/daemon.h"
#include "plugin_unit.h"
#include "socket/socket_addr.h"

class DTCListener;
class PollerBase;

class PluginAgentListenerPool {
    public:
	PluginAgentListenerPool();
	~PluginAgentListenerPool();
	int do_bind();

	int Match(const char *, int = 0);
	int Match(const char *, const char *);

    private:
	SocketAddress _sockaddr[MAXLISTENERS];
	DTCListener *_listener[MAXLISTENERS];
	PollerBase *_thread[MAXLISTENERS];
	PluginDecoderUnit *_decoder[MAXLISTENERS];
	int _udpfd[MAXLISTENERS];

	int init_decoder(int n, int idle);
};

#endif
