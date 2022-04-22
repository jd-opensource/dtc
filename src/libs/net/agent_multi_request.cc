#include "agent_multi_request.h"
#include "task_request.h"
#include "protocol.h"
#include <stdlib.h>
//#include "agent_client.h"

int PacketBodyLen(CPacketHeader & header)
{
        int pktbodylen = ntohl(header.len);
        return pktbodylen;
}

CAgentMultiRequest::CAgentMultiRequest(CTaskRequest * o):
	packetCnt(0),
	owner(o),
	taskList(NULL),
	compleTask(0),
	ownerClient(NULL)
{
    if(o)
        ownerClient = o->OwnerClient();
}


CAgentMultiRequest::~CAgentMultiRequest()
{
    ListDel();

    if(taskList)
    {
	for(int i = 0; i < packetCnt; i++)
	    if(taskList[i].task)
	        delete taskList[i].task;

	delete[] taskList;
    }

    if(!!packets)
	free(packets.ptr);
}

void CAgentMultiRequest::CompleteTask(int index)
{
    if(taskList[index].task)
    {
	delete taskList[index].task;
	taskList[index].task = NULL;
    }

    compleTask++;
    /* delete owner taskrequest along with us if all sub request's result putted into ClientAgent's send queue */

    if(compleTask == packetCnt)
    {
	delete owner;
    }
}

void CAgentMultiRequest::ClearOwnerInfo()
{
    ownerClient = NULL;

    if(taskList == NULL)
	return;

    for(int i = 0; i < packetCnt; i++)
    {
        if(taskList[i].task) 
	    taskList[i].task->ClearOwnerClient();
    }
}

/*
error case: set this task processed
1. no mem: set task processed
2. decode error: set task processed, reply this task
*/
void CAgentMultiRequest::DecodeOneRequest(char * packetstart, int packetlen, int index)
{
	CTaskRequest * task = NULL;
//	RequestCmd cmd;

	task = new CTaskRequest();
	if(NULL == task)
	{
		log_crit("not enough mem for new task creation, client wont recv response");
		compleTask++;
		return;
	}

	if((uint)packetlen <= sizeof(CPacketHeader)){
		compleTask++;
		delete task;
		return;
	}

	if(!task->copyOnePacket(packetstart + sizeof(CPacketHeader), packetlen - sizeof(CPacketHeader))){
		log_crit("not enough mem for buf copy, client wont recv response");
		compleTask++;
		delete task;
		return;
	}
	task->SetOwnerInfo(this, index, NULL);
	task->SetOwnerClient(this->ownerClient);
	task->ResponseTimerStart();
	CPacketHeader *h = (CPacketHeader *)packetstart;


	task->SetReqCmd(ntohs(h->cmd));
	task->SetSeqNumber(ntohl(h->seq_num));
	if(task->GetReqCmd() <= SERVICE_NONE || task->GetReqCmd() >= SERVICE_OTHER){
		taskList[index].processed = 1;
	}
	taskList[index].task = task;

	return;
}

int CAgentMultiRequest::DecodeAgentRequest()
{
    int cursor = 0;

    taskList = new DecodedTask[packetCnt];
    if(NULL == taskList)
    {
	log_crit("no mem new taskList");
	return -1;
    }
    memset((void *)taskList, 0, sizeof(DecodedTask) * packetCnt);

    /* whether can work, reply on input buffer's correctness */
    for(int i = 0; i < packetCnt; i++)
    {
        char * packetstart;
        int packetlen;

        packetstart = packets.ptr + cursor;
        packetlen = PacketBodyLen(*(CPacketHeader *)packetstart) + sizeof(CPacketHeader);
        if(packetlen < 2 || packetlen > packets.len - cursor){
        	log_crit("decode packet len error");
        	return -1;
        }

        DecodeOneRequest(packetstart, packetlen, i);
	
        cursor += packetlen;
    }

    return 0;
}

void CAgentMultiRequest::CopyReplyForSubTask()
{
    for(int i = 0; i < packetCnt; i++)
    {
	if(taskList[i].task)
	    taskList[i].task->CopyReplyPath(owner);
    }
}
