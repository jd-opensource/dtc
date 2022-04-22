#include "transaction_thread.h"
#include "transaction_task.h"
#include "net_server.h"

extern CNetServerProcess *netserverProcess;

#define MIN(x,y) ((x)<=(y)?(x):(y))

CTransactionThread::CTransactionThread(const char *name, TransThreadQueue* trans_queue, int thread_id): 
	CThread(name, CThread::ThreadTypeAsync), 
	m_thread_id(thread_id),
	m_trans_queue(trans_queue)
{
	ShardingsphereInit();
}

CTransactionThread::~CTransactionThread()
{
}

void* CTransactionThread::Process(void)
{
	while(Stopping() == false)
	{
		log_debug("transaction thread process.");
		CTaskRequest* request = m_trans_queue->Pop();
		if(request == NULL)
		{
			log_error("transaction pop task error.");
			continue;
		}

		TransactionTask* task = new TransactionTask(&m_db_conn);	
		if (task == NULL) {
			log_crit("no new memory for task");
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

int CTransactionThread::ShardingsphereInit()
{
	const char* p;
	
	memset(&m_db_host_conf, 0, sizeof(DBHost));
	const char* addr = g_dbconfig.GetStringValue("dbHost").c_str();
	p = strrchr(addr, ':');
	if(p == NULL){
		strncpy(m_db_host_conf.Host, addr, sizeof(m_db_host_conf.Host)-1 );
		m_db_host_conf.Port = 0;
	}
	else{
		strncpy(m_db_host_conf.Host, addr, MIN(p - addr, (int)sizeof(m_db_host_conf.Host)-1) );
		m_db_host_conf.Port = atoi(p+1);
	}
	
	strncpy(m_db_host_conf.User, g_dbconfig.GetStringValue("dbUser").c_str(), sizeof(m_db_host_conf.User)-1 );
	strncpy(m_db_host_conf.Password, g_dbconfig.GetStringValue("dbPwd").c_str(), sizeof(m_db_host_conf.Password)-1 );
	strncpy(m_db_host_conf.DbName, g_dbconfig.GetStringValue("dbName").c_str(), sizeof(m_db_host_conf.DbName)-1 );
	m_db_host_conf.ConnTimeout = g_dbconfig.GetIntValue("ConnTimeout");
	m_db_host_conf.ReadTimeout = g_dbconfig.GetIntValue("ReadTimeout");
	
	log_debug("dbname: %s", m_db_host_conf.DbName);	
	m_db_conn.Config(&m_db_host_conf);

	if(m_db_conn.Open() != 0){
		log_warning("shardingsphere: connect db[%s] error: %s", m_db_host_conf.Host, m_db_conn.GetErrMsg());
		return(-6);
	}

	return(0);
}