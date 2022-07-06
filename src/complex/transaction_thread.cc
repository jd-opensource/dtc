#include "transaction_thread.h"
#include "transaction_task.h"
#include "net_server.h"

extern CNetServerProcess *netserverProcess;

#define MIN(x,y) ((x)<=(y)?(x):(y))

CTransactionThread::CTransactionThread(const char *name, TransThreadQueue* trans_queue, int thread_id, DBHost* dbconfig): 
	CThread(name, CThread::ThreadTypeAsync), 
	m_thread_id(thread_id),
	m_trans_queue(trans_queue)
{
	init_mysql_connection(dbconfig);
}

CTransactionThread::~CTransactionThread()
{
}

void* CTransactionThread::Process(void)
{
	while(Stopping() == false)
	{
		log4cplus_debug("transaction thread process.");
		CTaskRequest* request = m_trans_queue->Pop();
		if(request == NULL)
		{
			log4cplus_error("transaction pop task error.");
			continue;
		}

		TransactionTask* task = new TransactionTask(&m_db_conn);	
		if (task == NULL) {
			log4cplus_error("no new memory for task");
			request->setResult("create task failed");

			netserverProcess->ReplyNotify(request);
			continue;
		}
		task->Process(request);

		netserverProcess->ReplyNotify(request);
		if (task != NULL) {
			delete task;
		}
	}
	return 0;
}

int CTransactionThread::init_mysql_connection(DBHost* dbconfig)
{
	if(!dbconfig)
		return -1;

	log4cplus_debug("init_mysql_connection db: %s", dbconfig->DbName);	
	m_db_conn.Config(dbconfig);

	if(m_db_conn.Open() != 0){
		log4cplus_error("open db failed: %s %s %s, %s", dbconfig->Host, dbconfig->User, dbconfig->DbName, m_db_conn.GetErrMsg());
		return -6;
	}

	return(0);
}