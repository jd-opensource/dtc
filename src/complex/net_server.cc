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

extern CTransactionGroup* transactionGroup;

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
    log4cplus_debug("CNetServerProcess::TaskNotify start");
    //there is a race condition here:
    //curr may be deleted during process (in task->ReplyNotify())

	CTaskRequest * request = cur;
	if(request == NULL){
		return;
	}
	
	if(transactionGroup->Push(request) != 0)
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