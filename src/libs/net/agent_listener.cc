/*
 * =====================================================================================
 *
 *       Filename:  agent_listener.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  09/01/2010 12:38:00 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  newmanwang (nmwang), newmanwang@tencent.com
 *        Company:  Tencent, China
 *
 * =====================================================================================
 */

#include <errno.h>

#include "agent_listener.h"
#include "agent_client.h"
#include "poll_thread.h"
#include "log.h"

CAgentListener::CAgentListener(PollerBase * t, CAgentClientUnit * o, CSocketAddress & a):
    CPollerObject(t, 0),
    ownerThread(t),
    out(o),
    addr(a)
{
}

CAgentListener::~CAgentListener()
{
}

/* part of framework construction */
int CAgentListener::Bind(int blog)
{
    if ((netfd = SockBind (&addr, blog, 0, 0, 1/*reuse*/, 1/*nodelay*/, 1/*defer_accept*/)) == -1)
	return -1;
    return 0;
}

int CAgentListener::AttachThread()
{
    EnableInput();
    if(CPollerObject::AttachPoller() < 0)
    {
	log4cplus_error("agent listener attach agentInc thread error");
	return -1;
    }
    return 0;
}

void CAgentListener::InputNotify()
{
    while(1)
    {
		int newfd;
		struct sockaddr peer;
		socklen_t peersize = sizeof(peer);

		newfd = accept(netfd, &peer, &peersize);
		if(newfd < 0)
		{
			if(EINTR != errno && EAGAIN != errno)
				log4cplus_error("agent listener accept error, %m");
			break;
		}

		log4cplus_debug("new CAgentClient accepted!!");

		CClientAgent * client;
		try { client = new CClientAgent(ownerThread, out, newfd); }
		catch(int err) { return; }

		if(NULL == client)
		{
			log4cplus_error("no mem for new client agent");
			break;
		}

		client->AttachThread();
		client->InputNotify();
    }
}
