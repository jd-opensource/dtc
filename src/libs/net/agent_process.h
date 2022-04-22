/*
 * =====================================================================================
 *
 *       Filename:  agent_process.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/26/2010 08:59:29 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  newmanwang (nmwang), newmanwang@tencent.com
 *        Company:  Tencent, China
 *
 * =====================================================================================
 */

#ifndef __AGENT_PROCESS_H__
#define __AGENT_PROCESS_H__

#include "request_base.h"

class PollerBase;
class CTaskRequest;

class CAgentProcess : public CTaskDispatcher<CTaskRequest>
{
    private:
	PollerBase * ownerThread;
	CRequestOutput<CTaskRequest> output;

    public:
	CAgentProcess(PollerBase * o);
	virtual ~CAgentProcess();

	inline void BindDispatcher(CTaskDispatcher<CTaskRequest> *p)
	{
	    output.BindDispatcher(p);
	}
	virtual void TaskNotify(CTaskRequest * curr);
};

#endif
