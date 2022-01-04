/*
* Copyright [2021] JD.com, Inc.
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
#include <errno.h>

#include "agent_listener.h"
#include "agent/agent_client.h"
#include "poll/poller_base.h"
#include "log/log.h"

AgentListener::AgentListener(PollerBase *t, JobEntranceAskChain *o,
			     SocketAddress &a)
	: EpollBase(t, 0), ownerThread(t), out(o), addr(a)
{
}

AgentListener::~AgentListener()
{
}

/* part of framework construction */
int AgentListener::do_bind(int blog)
{
	if ((netfd = socket_bind(&addr, blog, 0, 0, 1 /*reuse*/, 1 /*nodelay*/,
				 1 /*defer_accept*/)) == -1)
		return -1;
	return 0;
}

int AgentListener::attach_thread()
{
	enable_input();
	if (EpollBase::attach_poller() < 0) {
		log4cplus_error("agent listener attach agentInc thread error");
		return -1;
	}
	return 0;
}

void AgentListener::input_notify()
{
	log4cplus_debug("enter input_notify.");
	while (1) {
		int newfd;
		struct sockaddr peer;
		socklen_t peersize = sizeof(peer);

		newfd = accept(netfd, &peer, &peersize);
		if (newfd < 0) {
			if (EINTR != errno && EAGAIN != errno)
				log4cplus_error(
					"agent listener accept error, %m");
			break;
		}

		log4cplus_debug("new client connection accepting.");

		ClientAgent *client;
		try {
			client = new ClientAgent(ownerThread, out, newfd);
		} catch (int err) {
			return;
		}

		if (NULL == client) {
			log4cplus_error("no mem for new client agent");
			break;
		}

		client->attach_thread();
		client->input_notify();
	}
	log4cplus_debug("leave input_notify.");
}
