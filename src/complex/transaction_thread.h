#ifndef _TRANSTHREAD_H_
#define _TRANSTHREAD_H_

#include "comm.h"
#include "process_task.h"
#include "task_request.h"
#include "global.h"
#include "DBConn.h"

class CTransactionThread : public CThread
{
private:
	int m_thread_id;
	TransThreadQueue* m_trans_queue;
	DBHost	m_db_host_conf;
	CDBConn m_db_conn;

public:
	CTransactionThread(const char *name, TransThreadQueue* trans_queue, int thread_id);
	virtual ~CTransactionThread();

	int ShardingsphereInit();

protected:
	virtual void* Process(void);
};

#endif