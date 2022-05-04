#ifndef _TRANS_TASK_H_
#define _TRANS_TASK_H_

#include "comm.h"
#include "process_task.h"
#include "task_request.h"
#include "global.h"
#include "cm_conn.h"
#include "../libs/net/value.h"

class TransactionTask
{
public:
	TransactionTask();
	TransactionTask(MysqlConn* DbConn): m_DBConn(DbConn) {}
	virtual ~TransactionTask();

	int Process(CTaskRequest *request);

	int BuildTransactionInfo();
	void BuildAdaptSql(TransactionInfo* trans_info, int idx);

	std::vector<TransactionInfo> GetTransactionInfo() { return m_trans_info; }

	int request_db_query(std::string request_sql, CTaskRequest *request);

	std::string GetErrorMessage() {return m_errmsg;}
	void SetErrorMessage(std::string msg) {m_errmsg = msg;}
	CBufferChain* encode_mysql_protocol(CTaskRequest *request);

private:
	std::string m_oper;
	std::vector<TransactionInfo> m_trans_info;
	std::string m_errmsg;
	MysqlConn* m_DBConn;
	
	int save_row(CTaskRequest *request);
};

#endif