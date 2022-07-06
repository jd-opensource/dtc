#include <stdio.h>
#include <stdlib.h>

#include "memcheck.h"
#include "task_request.h"
#include "poll_thread.h"
#include "log.h"
#include "agent_multi_request.h"

#include "../common/my/my_command.h"
#include "../common/my/my_comm.h"
#include "../common/protocol.h"

CTaskRequest::~CTaskRequest() 
{ 

    if(agentMultiReq)
	{
        delete agentMultiReq;
		agentMultiReq = NULL;
	}

	if(recvBuff)
	{
		free(recvBuff);
		recvBuff = NULL;
	}
}

void CTaskRequest::Clean()
{
    timestamp = 0;
    expire_time = 0;
    CTaskOwnerInfo::Clean();
}

#define ERR_RET(ret_msg, fmt, args...) do{/* SetError(err, "decoder", ret_msg); */log4cplus_debug(fmt, ##args); return -1; }while(0)
int CTaskRequest::PrepareProcess(void) {
//	int err;

	if(1) {
		/* timeout present */
		int client_timeout = 6000;

		log4cplus_debug("client api set timeout %d ms", client_timeout);
		struct timeval now;
		gettimeofday(&now, NULL);

		responseTimer = (int)(now.tv_sec * 1000000ULL + now.tv_usec);
		expire_time = now.tv_sec * 1000ULL + now.tv_usec/1000 + client_timeout;
		timestamp = now.tv_sec;
	}
	return 0;
}

/* for agent request */
int CTaskRequest::DecodeAgentRequest()
{
    if(agentMultiReq)
    {
        delete agentMultiReq;
        agentMultiReq = NULL;
    }

    agentMultiReq = new CAgentMultiRequest(this);
    if(agentMultiReq == NULL)
    {
        log4cplus_error("no mem new CAgentMultiRequest");
        return -1;
    }

    PassRecvedResultToAgentMutiReq();

    if(agentMultiReq->DecodeAgentRequest() < 0)
    {
       log4cplus_error("agent multi request decode error");
       return -1;
    }

    return 0;
}

void CTaskRequest::PassRecvedResultToAgentMutiReq()
{
	agentMultiReq->SaveRecvedResult(recvBuff, recvLen, recvPacketCnt);
	recvBuff = NULL; recvLen = recvPacketCnt = 0;
}

bool CTaskRequest::copyOnePacket(const char *packet, int pktLen)
{
	if(NULL == recvBuff && pktLen > 0){
		recvBuff = (char *)MALLOC(pktLen);
		if(NULL == recvBuff)
			return false;
		memcpy(recvBuff,packet,pktLen);
		recvLen = pktLen;
		return true;
	}else{
		return false;
	}
}

int CTaskRequest::do_mysql_protocol_parse()
{
	log4cplus_debug("do_mysql_protocol_parse entry.");

	char *p = recvBuff;
	int raw_len = recvLen;

	if (p == NULL || raw_len < MYSQL_HEADER_SIZE) {
		log4cplus_error("receive size small than package header.");
		return -1;
	}

	int input_packet_length = uint_trans_3(p);
	log4cplus_debug("uint_trans_3:0x%x 0x%x 0x%x, len:%d", p[0], p[1], p[2],
			input_packet_length);
	p += 3;
	this->mysql_seq_id = (uint8_t)(*p); // mysql sequence id
	p++;
	log4cplus_debug("mysql_seq_id:%d, packet len:%d", this->mysql_seq_id,
			input_packet_length);

	if (sizeof(MYSQL_HEADER_SIZE) + input_packet_length > raw_len) {
		log4cplus_error(
			"mysql header len %d is different with actual len %d.",
			input_packet_length, raw_len);
		return -2;
	}

	enum enum_server_command cmd = (enum enum_server_command)(uchar)p[0];
	if (cmd != COM_QUERY) {
		log4cplus_error("cmd type error:%d", cmd);
		return -3;
	}

	input_packet_length --;
	p++;

	if (*p == 0x0) {
		p++;
		input_packet_length--;
	}

	if (*p == 0x01) {
		p++;
		input_packet_length--;
	}
	m_sql.assign(p, input_packet_length);
	log4cplus_debug("sql: \"%s\"", m_sql.c_str());

	log4cplus_debug("do_mysql_protocol_parse leave.");
	return 0;
}

const char* CTaskRequest::get_result(void)
{
	if(send_buff && send_len > 0)
		return send_buff;
	else
		return result.c_str();
}

//TODO: parse request sql.
std::string CTaskRequest::parse_request_sql()
{
	log4cplus_debug("parse_request_sql entry, %p, %d", recvBuff, recvLen);

	if(!recvBuff)
		return "";

	if(!do_mysql_protocol_parse())
	{
		log4cplus_debug("parse_request_sql leave");
		return m_sql;
	}

	log4cplus_debug("parse_request_sql leave");
	return "";
}

int CTaskRequest::get_db_layer_level()
{
	log4cplus_debug("get_db_layer_level entry.");

	int level = get_layer();

	if(level >3 || level < 1)
		return 0;

	log4cplus_debug("get_db_layer_level leave.");
	return level;
}

bool CTaskRequest::IsAgentRequestCompleted()
{
    return agentMultiReq->IsCompleted();
}

void CTaskRequest::DoneOneAgentSubRequest()
{
    CAgentMultiRequest * ownerReq = OwnerInfo<CAgentMultiRequest>();
    if(ownerReq)
        ownerReq->CompleteTask(OwnerIndex());
    else
        delete this;
}

void CTaskRequest::LinkToOwnerClient(CListObject<CAgentMultiRequest> & head)
{
	if(ownerClient && agentMultiReq)
		agentMultiReq->ListAdd(head);
}

void CTaskRequest::SetOwnerClient(CClientAgent * client)
{ 
	ownerClient = client; 
}

CClientAgent * CTaskRequest::OwnerClient()
{ 
	return ownerClient; 
}

void CTaskRequest::ClearOwnerClient()
{ 
	ownerClient = NULL; 
}

int CTaskRequest::AgentSubTaskCount()
{
    return agentMultiReq->PacketCount();
}

bool CTaskRequest::IsCurrSubTaskProcessed(int index)
{
    return agentMultiReq->IsCurrTaskProcessed(index);
}

CTaskRequest * CTaskRequest::CurrAgentSubTask(int index)
{
    return agentMultiReq->CurrTask(index);
}

void CTaskRequest::CopyReplyForAgentSubTask()
{
	agentMultiReq->CopyReplyForSubTask();
}
