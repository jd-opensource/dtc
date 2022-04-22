#include "transaction_task.h"

TransactionTask::TransactionTask()
{
	
}

TransactionTask::~TransactionTask() {}

int TransactionTask::SaveRow()
{
#if 0
	int req_field_size = m_DBConn->FieldNum;
	Json::Value row;
	ostringstream oss;
	int cnt = 0;
	for (int i = 0; i < req_field_size; i++)
	{
		if (m_DBConn->Row[i])
		{
			stringstream ss;
			switch(m_DBConn->Fields[i].type)
			{
			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_TINY:
			case MYSQL_TYPE_SHORT:
			case MYSQL_TYPE_LONG:
			case MYSQL_TYPE_FLOAT:
			case MYSQL_TYPE_DOUBLE:
			case MYSQL_TYPE_LONGLONG:
			case MYSQL_TYPE_INT24:
				ss << m_DBConn->Row[i];
				ss >> row[m_DBConn->Fields[i].name];
				cnt++;
				break;	
		
			case MYSQL_TYPE_NULL:
			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_YEAR:
			case MYSQL_TYPE_NEWDATE:
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_TIMESTAMP2:
			case MYSQL_TYPE_DATETIME2:
			case MYSQL_TYPE_TIME2:
            case MYSQL_TYPE_NEWDECIMAL:
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_STRING:
				row[m_DBConn->Fields[i].name] = m_DBConn->Row[i];
				cnt++;
				break;

			default:
				oss << "enum_field_types is " << m_DBConn->Fields[i].type;
				SetErrorMessage(oss.str());
				return -4;
			}
		}
	}

	if(cnt>0)
	{
		//m_queryResult.append(row);
	}
#endif
	return 0;
}

int TransactionTask::HandleReadOper()
{
	std::string m_Sql = "";//m_data.asString();
	
	if(m_DBConn->Ping() != 0)
	{
		const char *errmsg = m_DBConn->GetErrMsg();
		int m_ErrorNo = m_DBConn->GetErrNo();
		log_error("db reconnect error, errno[%d]  errmsg[%s]", m_ErrorNo, errmsg);
		SetErrorMessage(errmsg);
		return m_ErrorNo;
	}
	int ret = m_DBConn->Query(m_Sql.c_str());

	if(0 != ret)
	{
		const char *errmsg = m_DBConn->GetErrMsg();
		int m_ErrorNo = m_DBConn->GetErrNo();
		log_error("db execute error. errno[%d]  errmsg[%s]", m_ErrorNo, errmsg);
		log_error("error sql [%s]", m_Sql.c_str());
		SetErrorMessage(errmsg);
		return m_ErrorNo;
	}
	ret = m_DBConn->UseResult();
	if (0 != ret) {
		log_error("can not use result,sql[%s]", m_Sql.c_str());
		SetErrorMessage(m_DBConn->GetErrMsg());
		return -4;
	}

	ret = m_DBConn->FetchFields();
	if (0 != ret) {
		log_error("can not use fileds,[%d]%s", m_DBConn->GetErrNo(), m_DBConn->GetErrMsg());
		return -4;
	}

	int nRows = m_DBConn->ResNum; 
	for (int i = 0; i < nRows; i++) {
		ret = m_DBConn->FetchRow();
		if (0 != ret) {
			m_DBConn->FreeResult();
			return -4;
		}
		unsigned long *lengths = 0;
		lengths = m_DBConn->getLengths();
		if (0 == lengths) {
			log_error("row length is 0,sql[%s]", m_Sql.c_str());
			m_DBConn->FreeResult();
			return -4;
		}

		if(SaveRow() != 0)
		{
			log_error("filed [%d] type unsupported, %s", i, GetErrorMessage().c_str());
			m_DBConn->FreeResult();
			return -4;
		}

	}
	m_DBConn->FreeResult();
	return 0;
}

int TransactionTask::HandleWriteOper()
{
	int ret = 0;
	return ret;
}

int TransactionTask::ParseJson(const char *sz_json, int json_len)
{
	return 0;
}

int TransactionTask::TransactionProcess()
{

	return 0;
}

void TransactionTask::BuildAdaptSql(TransactionInfo* trans_info, int idx)
{
	char szidx[8] = {0};
	sprintf(szidx, "%d", idx);
	std::string new_tablename = trans_info->tablename_prefix;
	if(idx >= 0)
		new_tablename += szidx;

	if(trans_info->sql.find(trans_info->tablename_prefix) != std::string::npos)
		trans_info->adapt_sql = trans_info->sql.replace(trans_info->sql.find(trans_info->tablename_prefix), trans_info->tablename_prefix.length(), new_tablename);

	return ;
}

int TransactionTask::BuildTransactionInfo()
{
#if 0
	for(unsigned int i = 0; i < m_data.size(); i++)
	{
		TransactionInfo ti;
		ti.tablename_prefix = m_data[i]["tablename"].asString();
		ti.sql = m_data[i]["sql"].asString();
		int idx = 0;
		
		if(m_data[i]["key"].isString())
		{
			ti.key_type = DField::String;
			ti.szkey = m_data[i]["key"].asString();

			idx = GetTableIdx((void*)m_data[i]["key"].asString().c_str(), ti.key_type, &g_table_set[ti.tablename_prefix]);
		}
		else if(m_data[i]["key"].isInt() || m_data[i]["key"].isInt64())
		{
			ti.ikey = m_data[i]["key"].asInt64();
			ti.key_type = DField::Signed;
			idx = GetTableIdx(&ti.ikey, ti.key_type, &g_table_set[ti.tablename_prefix]);
		}
		else if(m_data[i]["key"].isUInt() || m_data[i]["key"].isUInt64())
		{
			ti.ukey = m_data[i]["key"].asUInt64();
			ti.key_type = DField::Unsigned;
			idx = GetTableIdx(&ti.ukey, ti.key_type, &g_table_set[ti.tablename_prefix]);
		}

		BuildAdaptSql(&ti, idx);

		m_trans_info.push_back(ti);
	}
#endif
	return 0;
}

int TransactionTask::Process(CTaskRequest *request) {
	log_debug("TransactionTask::Process begin.");
#if 0
	Json::FastWriter writer;
	Json::Value response;
	Json::Value recv_packet;
	string request_string = request->buildRequsetString();
	log_debug("json: %s", request_string.c_str());
	if (ParseJson(request_string.c_str(), request_string.size(), recv_packet)) {
		log_debug("json parse finished.");
		response["ret"] = -1;
		response["msg"] = GetErrorMessage();
		std::string outputConfig = writer.write(response);
		request->setResult(outputConfig);
		return RT_PARSE_JSON_ERR;
	}
	log_debug("json parse finished.");
	int ret = 0;
	if(strcasecmp(m_oper.c_str(), "write") == 0)
		ret = HandleWriteOper();
	else if(strcasecmp(m_oper.c_str(), "read") == 0)
		ret = HandleReadOper();
	else{
		ret = -4;
		stringstream ss;
		ss<<"oper method error, oper method: "<<m_oper;
		SetErrorMessage(ss.str());
		log_error(ss.str().c_str());
	}
	if(ret == 0)
	{
		response["ret"] = 0;
		if(strcasecmp(m_oper.c_str(), "read") == 0)
			;//response["msg"] = m_queryResult;
		else
			response["msg"].resize(0);
	}
	else
	{
		response["ret"] = ret;
		response["msg"] = GetErrorMessage();
	}
	if(recv_packet.isMember("debug"))
	{
		response["debug"] = 1;
	}

	std::string outputConfig = writer.write(response);
	request->setResult(outputConfig);
#endif
	log_debug("TransactionTask::Process end.");
	return 0;
}
