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
#ifndef __LISTENER_H__
#define __LISTENER_H__
#include "poll/poller.h"
#include "socket/socket_addr.h"

int unix_sock_bind(const char *path, int backlog = 0);
int udp_sock_bind(const char *addr, uint16_t port, int rbufsz, int wbufsz);
int socket_bind(const char *addr, uint16_t port, int backlog = 0);

class DecoderBase;
class DTCListener : public EpollBase {
    public:
	DTCListener(const SocketAddress *);
	~DTCListener();

	int do_bind(int blog, int rbufsz, int wbufsz);
	virtual void input_notify(void);
	virtual int do_attach(DecoderBase *, int blog = 0, int rbufsz = 0,
			      int wbufsz = 0);
	void set_request_window(int w)
	{
		window = w;
	};
	int FD(void) const
	{
		return netfd;
	}

    private:
	DecoderBase *outPeer;
	const SocketAddress *sockaddr;
	int bind;
	int window;
};
#endif
