#ifndef _TRANS_GROUP_H_
#define _TRANS_GROUP_H_

#include "transaction_thread.h"
#include "poll_thread_group.h"

class CTransactionGroup
{
private:
	int m_thread_num;
	int m_thread_index;
	TransThreadQueue** m_trans_queue;
	CTransactionThread** m_trans_thread;

public:
	CTransactionGroup(int thread_num);
	~CTransactionGroup() {}

	int Initialize(DBHost* dbconfig);
	void RunningThread();
	int Push(CTaskRequest* task);
};


#endif