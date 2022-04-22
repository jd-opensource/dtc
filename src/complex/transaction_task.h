#ifndef _TRANS_TASK_H_
#define _TRANS_TASK_H_

#include "comm.h"
#include "process_task.h"
#include "task_request.h"
#include "global.h"
#include "DBConn.h"

class TransactionTask
{
public:
	TransactionTask();
	TransactionTask(CDBConn* DbConn): m_DBConn(DbConn) {}
	virtual ~TransactionTask();

	int Process(CTaskRequest *request);

	int BuildTransactionInfo();
	void BuildAdaptSql(TransactionInfo* trans_info, int idx);
	int TransactionProcess();

	std::vector<TransactionInfo> GetTransactionInfo() { return m_trans_info; }

	int HandleReadOper();
	int HandleWriteOper();

	std::string GetErrorMessage() {return m_errmsg;}
	void SetErrorMessage(std::string msg) {m_errmsg = msg;}

private:
	std::string m_oper;
	std::vector<TransactionInfo> m_trans_info;
	std::string m_errmsg;
	CDBConn* m_DBConn;
	
	int SaveRow();
	int ParseJson(const char *sz_json, int json_len);
};

#endif