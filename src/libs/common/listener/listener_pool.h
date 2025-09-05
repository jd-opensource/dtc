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
#include "daemon/daemon.h"
#include "socket/socket_addr.h"

// this file is obsoleted, new one is agent_listen_pool.[hc]
class DTCListener;
class PollerBase;
class DecoderBase;
class DTCJobOperation;
class DTCDecoderUnit;
template <typename T> class JobAskInterface;

class ListenerPool {
    private:
	SocketAddress sockaddr[MAXLISTENERS];
	DTCListener *listener[MAXLISTENERS];
	PollerBase *thread[MAXLISTENERS];
	DTCDecoderUnit *decoder[MAXLISTENERS];

	int init_decoder(int n, int idle,
			 JobAskInterface<DTCJobOperation> *out);

    public:
	ListenerPool();
	~ListenerPool();
	int do_bind(DTCConfig *gc, JobAskInterface<DTCJobOperation> *out);

	int Match(const char *, int = 0);
	int Match(const char *, const char *);
};
