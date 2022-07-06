#ifndef SRC_DEAMON_DAEMONTEST_H_
#define SRC_DEAMON_DAEMONTEST_H_

#include "request_base.h"
#include "process_task.h"
#include "lock.h"

class PollerBase;
class CTaskRequest;

class CNetServerProcess : public CTaskDispatcher<CTaskRequest>
{
private:
	PollerBase * ownerThread;
	CRequestOutput<CTaskRequest> output;
	CMutex m_mutex;

public:
	CNetServerProcess(PollerBase * o);
	virtual ~CNetServerProcess();

	inline void BindDispatcher(CTaskDispatcher<CTaskRequest> *p)
	{
	    output.BindDispatcher(p);
	}
	virtual void TaskNotify(CTaskRequest * curr);

	void ReplyNotify(CTaskRequest* task);
};



#endif /* SRC_DEAMON_DAEMONTEST_H_ */
