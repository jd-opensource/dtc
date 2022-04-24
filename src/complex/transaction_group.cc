#include "transaction_group.h"

CTransactionGroup::CTransactionGroup(int thread_num): m_thread_num(thread_num), m_thread_index(0)
{

}

int CTransactionGroup::Initialize(DBHost* dbconfig)
{
	if(!dbconfig)
		return -1;

	m_trans_queue = new TransThreadQueue*[m_thread_num];
	m_trans_thread = new CTransactionThread*[m_thread_num];

	for(int i = 0; i < m_thread_num; i++)
	{
		char thread_name[256] = {0};
		m_trans_queue[i] = new TransThreadQueue;
		
		snprintf(thread_name, sizeof(thread_name), "%s@%d", "transaction", i);
		m_trans_thread[i] = new CTransactionThread(thread_name, m_trans_queue[i], i, dbconfig);
		m_trans_thread[i]->InitializeThread();
	}

	return 0;
}

void CTransactionGroup::RunningThread()
{
	for(int i = 0; i < m_thread_num; i++)
		m_trans_thread[i]->RunningThread();
}

int CTransactionGroup::Push(CTaskRequest* task)
{
	int count = 0;
	while(count < m_thread_num)
	{
		m_thread_index = (m_thread_index + 1) % m_thread_num;
		if(m_trans_queue[m_thread_index]->Count() == 0)
		{
			m_trans_queue[m_thread_index]->Push(task);
			return 0;
		}
		else
		{
			count++;
		}
	}

	return -1;
}