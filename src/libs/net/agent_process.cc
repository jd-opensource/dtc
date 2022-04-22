#include "agent_process.h"
#include "poll_thread.h"
#include "task_request.h"
#include "log.h"

CAgentProcess::CAgentProcess(PollerBase * o) :
    CTaskDispatcher<CTaskRequest>(o),
    ownerThread(o),
    output(o)
{
}

CAgentProcess::~CAgentProcess()
{
}

void CAgentProcess::TaskNotify(CTaskRequest * curr)
{
    curr->CopyReplyForAgentSubTask();
    log_debug("CAgentProcess::TaskNotify start");
    //there is a race condition here:
    //curr may be deleted during process (in task->ReplyNotify())
    int taskCount = curr->AgentSubTaskCount();
     log_debug("CAgentProcess::TaskNotify task count is %d",taskCount );
    for(int i = 0; i < taskCount; i++)
    {
		CTaskRequest * task = NULL;

			if(NULL == (task = curr->CurrAgentSubTask(i)))
			continue;

		if(curr->IsCurrSubTaskProcessed(i))
		{
			 log_debug("CAgentProcess::TaskNotify task reply notify");
			 std::string ret("{\"error\":\"cmd not found\"}");
			 task->setResult(ret);
			 task->ReplyNotify();
		}
		else
		{
			 log_debug("CAgentProcess::TaskNotify TaskNotify next process");
			  output.TaskNotify(task);
		}
	   
    }

    return;
}
