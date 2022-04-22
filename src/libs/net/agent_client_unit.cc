#include "agent_client_unit.h"
#include "timerlist.h"
#include "task_request.h"
#include "poll_thread.h"

CAgentClientUnit::CAgentClientUnit(PollerBase * t, int c):
    ownerThread(t),
    output(t),
    check(c)
{
	tlist = t->GetTimerList(c);
}

CAgentClientUnit::~CAgentClientUnit()
{
}

void CAgentClientUnit::BindDispatcher(CTaskDispatcher<CTaskRequest> * proc) 
{ 
	output.BindDispatcher(proc); 
}

void CAgentClientUnit::RecordRequestTime(int hit, int type, unsigned int usec)
{
}

void CAgentClientUnit::RecordRequestTime(CTaskRequest *req) 
{
        RecordRequestTime(0, 9, req->responseTimer.live());
}
