#include "agent_listen_pkg.h"
#include "agent_listener.h"
#include "agent_client_unit.h"
#include "poll_thread.h"
#include "task_request.h"
#include "log.h"

CAgentListenPkg::CAgentListenPkg():thread(NULL),out(NULL),listener(NULL)
{
}

CAgentListenPkg::~CAgentListenPkg()
{
	DELETE(listener);
	DELETE(out);
	if(thread){
		thread->interrupt();
		DELETE(thread);
	}
}

/* part of framework construction */
int CAgentListenPkg::Bind(const char *bindaddr, CTaskDispatcher<CTaskRequest> * agentprocess)
{
//    char bindstr[64];
    const char * errmsg = NULL;
    char threadname[64];
    int checktime = 5;
    int blog = 256;

//    checktime = gc->GetIntVal("cache", "AgentRcvBufCheck", 5);
//    blog = gc->GetIntVal("cache", "AgentListenBlog", 256);

	if(NULL == bindaddr)
		return -1;

	if((errmsg = addr.SetAddress(bindaddr, (const char *)NULL)))
		return -1;

	snprintf(threadname, sizeof(threadname), "inc0");
	thread = new PollerBase(threadname);
	if(NULL == thread)
	{
		log_error("no mem to new agent inc thread 0");
		return -1;
	}
	if(thread->InitializeThread() < 0)
	{
		log_error("agent inc thread 0 init error");
		return -1;
	}

	out = new CAgentClientUnit(thread, checktime);
	if(NULL == out)
	{
		log_error("no mem to new agent client unit 0");
		return -1;
	}
	out->BindDispatcher(agentprocess);

	listener = new CAgentListener(thread, out, addr);
	if(NULL == listener)
	{
		log_error("no mem to new agent listener 0");
		return -1;
	}
	if(listener->Bind(blog) < 0)
	{
		log_error("agent listener 0 bind error");
		return -1;
	}

	if(listener->AttachThread() < 0)
		return -1;

    return 0;
}


int CAgentListenPkg::Run()
{

	if(thread)
	    thread->RunningThread();

    return 0;
}

int CAgentListenPkg::Match(const char *host, const char *port)
{

	if(listener==NULL)
		return -1;
	if(addr.Match(host, port))
		return 1;

    return 0;
}
