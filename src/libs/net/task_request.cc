#include <stdio.h>
#include <stdlib.h>

#include "memcheck.h"
#include "task_request.h"
//#include "task_pkey.h"
#include "poll_thread.h"
#include "log.h"
//#include "compiler.h"
//#include "keylist.h"
#include "agent_multi_request.h"
//#include "agent_client.h"

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

#define ERR_RET(ret_msg, fmt, args...) do{/* SetError(err, "decoder", ret_msg); */log_debug(fmt, ##args); return -1; }while(0)
int CTaskRequest::PrepareProcess(void) {
//	int err;

	if(1) {
		/* timeout present */
		int client_timeout = 6000;

		log_debug("client api set timeout %d ms", client_timeout);
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
        log_crit("no mem new CAgentMultiRequest");
        return -1;
    }

    PassRecvedResultToAgentMutiReq();

    if(agentMultiReq->DecodeAgentRequest() < 0)
    {
       log_error("agent multi request decode error");
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

std::string CTaskRequest::buildRequsetString(){
	std::string request(recvBuff,recvLen);
	return request;

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
