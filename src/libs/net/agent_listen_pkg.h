
#ifndef __AGENT_LISTEN_PKG_H__
#define __AGENT_LISTEN_PKG_H__

#include "request_base.h"
#include "sockaddr.h"

#define MAX_AGENT_LISTENER 1

class CAgentClientUnit;
class CAgentListener;
class PollerBase;
class CConfig;
class CTaskRequest;
class CAgentListenPkg
{
    private:
	CSocketAddress addr;
	PollerBase * thread;
	CAgentClientUnit * out;
	CAgentListener * listener;

    public:
	CAgentListenPkg();
	~CAgentListenPkg();

	int Bind(const char *bindaddr, CTaskDispatcher<CTaskRequest> *out);
//	int Bind(char *bindaddr, CTaskDispatcher<CTaskRequest> *out, PollerBase *workThread);
	int Run();
	int Match(const char *host, const char *port);
	PollerBase *GetThread()const{	return thread;	}
};

#endif
