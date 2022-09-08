#include <iostream>
#include <string>
#include <sstream>
#include "net_server.h"

#include "log.h"
#include "poll_thread.h"
#include "task_request.h"
#include "global.h"
#include "transaction_task.h"
#include "task_request.h"
#include "transaction_group.h"

extern CTransactionGroup* FullDBGroup;
extern CTransactionGroup* HotDBGroup;

CNetServerProcess::CNetServerProcess(PollerBase * o) :
    CTaskDispatcher<CTaskRequest>(o),
    ownerThread(o),
    output(o)
{
}

CNetServerProcess::~CNetServerProcess()
{
}

void CNetServerProcess::TaskNotify(CTaskRequest * cur)
{
    log4cplus_debug("async-conn: packet receiving.");
    //there is a race condition here:
    //curr may be deleted during process (in task->ReplyNotify())
	CTransactionGroup* group = NULL;
	CTaskRequest * request = cur;
	int level = 0;

	if(request == NULL)
		return;

	//TODO: Parsing input, adapting thread groups.
	level = request->get_db_layer_level();
	log4cplus_debug("async-conn: packet db layer:%d.", level);
	if(level == 3)
		group = FullDBGroup;
	else if(level == 2)
		group = HotDBGroup;
	else 
	{
		char err[260] = {0};
		sprintf(err, "layer level error:%d.", level);
		request->setResult(err);
		request->ReplyNotify();
	}
	
	if(group->Push(request) != 0)
	{
		request->setResult("transaction insert queue failed.");
		request->ReplyNotify();
	}
	
    return ;
}

void CNetServerProcess::ReplyNotify(CTaskRequest* task) 
{
	m_mutex.lock();
	task->ReplyNotify();
	m_mutex.unlock();
}