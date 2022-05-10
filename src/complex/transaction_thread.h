#ifndef _TRANSTHREAD_H_
#define _TRANSTHREAD_H_

#include "comm.h"
#include "process_task.h"
#include "task_request.h"
#include "global.h"
#include "cm_conn.h"

class CTransactionThread : public CThread
{
private:
	int m_thread_id;
	TransThreadQueue* m_trans_queue;
	MysqlConn m_db_conn;

public:
	CTransactionThread(const char *name, TransThreadQueue* trans_queue, int thread_id, DBHost* dbconfig);
	virtual ~CTransactionThread();

	int init_mysql_connection(DBHost* dbconfig);

protected:
	virtual void* Process(void);
};

#endif