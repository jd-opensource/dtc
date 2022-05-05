#include <errno.h>

#include "agent_client.h"

#include "agent_client_unit.h"
#include "agent_receiver.h"
#include "agent_sender.h"
#include "log.h"
#include "poll_thread.h"
#include "agent_multi_request.h"
#include "packet.h"
#include "task_request.h"
#include "agent_multi_request.h"

CAgentResultQueue::~CAgentResultQueue()
{
    CPacket * p;

    while(NULL != (p = packet.Pop()))
    {
	p->FreeResultBuff();
    	delete p;
    }
}

class CAgentReply : public CReplyDispatcher<CTaskRequest>
{
    public:
	CAgentReply() {
		statInitFlag = false;
	}
	virtual ~CAgentReply() {}
	virtual void ReplyNotify(CTaskRequest *task);
    private:
	bool statInitFlag;
};

void CAgentReply::ReplyNotify(CTaskRequest *task)
{
	log4cplus_debug("CAgentReply::ReplyNotify start");

	CClientAgent * client = task->OwnerClient(); 
	if(client == NULL)
	{
		/* client gone, finish this task */
		task->DoneOneAgentSubRequest();
		return;
	}

	client->RecordRequestProcessTime(task);

	int client_timeout = 6000;
	int req_delaytime = task->responseTimer.live();

    log4cplus_debug("task client_timeout: %d", client_timeout);
			
	if ( (req_delaytime / 1000 ) >= client_timeout) //ms
	{
		log4cplus_debug("CAgentReply::ReplyNotify client_timeout[%d]ms, req delay time[%d]us", 6000, req_delaytime);
	}
    CPacket * packet = new CPacket();
    if(packet == NULL)
    {
	/* make response error, finish this task */
	task->DoneOneAgentSubRequest();
	log4cplus_error("no mem new CPacket");
	return;
    }

    packet->EncodeResult(task);
    task->DoneOneAgentSubRequest(); 

    client->AddPacket(packet);
    if(client->SendResult() < 0)
    {
        log4cplus_error("cliengAgent SendResult error");
        delete client;
        return;
    }

    log4cplus_debug("CAgentReply::ReplyNotify stop");
}

static CAgentReply agentReply;

/* sender and receiver should inited ok */
CClientAgent::CClientAgent(PollerBase * o, CAgentClientUnit * u, int fd):
    CPollerObject(o, fd),
    ownerThread(o),
    owner(u),
    tlist(NULL)
{
    tlist = u->TimerList();
    sender = new CAgentSender(fd);
    if(NULL == sender)
    {
	log4cplus_error("no mem to new sender");
	throw (int)-ENOMEM;
    }

    if(sender && sender->Init() < 0)
    {
	delete sender;
	sender = NULL;
	log4cplus_error("no mem to init sender");
	throw (int)-ENOMEM;	
    }

    if(sender)
    {
	receiver = new CAgentReceiver(fd);
	if(NULL == receiver)
	{
	    log4cplus_error("no mem to new receiver");
	    throw (int)-ENOMEM;
	}

	if(receiver && receiver->Init() < 0)
	{
	    log4cplus_error("no mem to init receiver");
	    throw (int)-ENOMEM;
	}
    }
}

CClientAgent::~CClientAgent()
{
	log4cplus_debug("~CClientAgent start");
    CListObject<CAgentMultiRequest> * node = rememberReqHeader.ListNext();
    CAgentMultiRequest * req;

    /* notify all request of this client I'm gone */
    while(node != &rememberReqHeader)
    {
        req = node->ListOwner();
        req->ClearOwnerInfo();
        req->DetachFromOwnerClient();
        node = rememberReqHeader.ListNext();
    }

    if(receiver)
	delete receiver;
    if(sender)
	delete sender;

	DetachPoller();

	log4cplus_debug("~CClientAgent end");
}

int CClientAgent::AttachThread()
{
    DisableOutput();
    EnableInput();

    if(AttachPoller() < 0)
    {
		log4cplus_error("client agent attach agengInc thread failed");
		return -1;
    }

    /* no idle test */
    return 0;
}

void CClientAgent::RememberRequest(CTaskRequest * request)
{
    request->LinkToOwnerClient(rememberReqHeader);
}

CTaskRequest * CClientAgent::PrepareRequest(char * recvbuff, int recvlen, int pktcnt)
{
    CTaskRequest * request;

    request = new CTaskRequest();
    if(NULL == request)
    {
		free(recvbuff);
		log4cplus_error("no mem allocate for new agent request");
		return NULL;
    }

    request->SetOwnerInfo(this, 0, NULL);
    request->SetOwnerClient(this);
    request->PushReplyDispatcher(&agentReply);
    request->SaveRecvedResult(recvbuff, recvlen, pktcnt);

    /* assume only a few sub request decode error */
    if(request->DecodeAgentRequest() < 0)
    {
		delete request;
		return NULL;
    }

    /* no mem new task case */
    if(request->IsAgentRequestCompleted())
    {
		delete request;
		return NULL;
    }

    RememberRequest(request);

    return request;
}

int CClientAgent::RecvRequest()
{
    RecvedPacket packets;
    char * recvbuff = NULL;
    int recvlen = 0;
    int pktcnt = 0;
    CTaskRequest * request = NULL;

    packets = receiver->Recv();

    if(packets.err < 0)
        return -1;
    else if(packets.pktCnt == 0)
	return 0;

    recvbuff = packets.buff;
    recvlen = packets.len;
    pktcnt = packets.pktCnt;

    request = PrepareRequest(recvbuff, recvlen, pktcnt);
    if(request != NULL)
        owner->TaskNotify(request);

    return 0;
}

/* exit when recv error*/
void CClientAgent::InputNotify()
{
    log4cplus_debug("CClientAgent::InputNotify() start");
    if(RecvRequest() < 0)
    {
		log4cplus_debug("erro when recv");
		delete this;
		return;
    }
    DelayApplyEvents();
    log4cplus_debug("CClientAgent::InputNotify() stop");
    return;
}

/*
return error if sender broken
*/
int CClientAgent::SendResult()
{
    if(sender->IsBroken())
    {
        log4cplus_error("sender broken");
        return -1;
    }

    while(1)
    {
	
		CPacket* frontPkt = resQueue.Front();
		if (NULL == frontPkt)
		{

			break;
		}
		
		if (frontPkt->VecCount() + sender->VecCount() > SENDER_MAX_VEC)
		{
			log4cplus_error("the sum value of front packet veccount[%d] and sender veccount[%d]is greater than SENDER_MAX_VEC[%d]",
					frontPkt->VecCount(),  sender->VecCount(), SENDER_MAX_VEC);
			break;

		}
		else
		{
			CPacket * pkt ;
			pkt = resQueue.Pop();
			if(NULL == pkt)
			{
				break;
			}
			if(sender->AddPacket(pkt) < 0)
			{
				return -1;
			}

		}
      
    }

    if(sender->Send() < 0)
    {
	log4cplus_error("agent client send error");
	return -1;
    }

    if(sender->LeftLen() != 0)
        EnableOutput();
    else
        DisableOutput();

    DelayApplyEvents();

    return 0;
}

void CClientAgent::OutputNotify()
{
    log4cplus_debug("CClientAgent::OutputNotify() start");
    if(SendResult() < 0)
    {
	log4cplus_debug("error when response");
	delete this;
	return;
    }
    log4cplus_debug("CClientAgent::OutputNotify() stop");

    return;
}

void CClientAgent::RecordRequestProcessTime(CTaskRequest * task)
{
    owner->RecordRequestTime(task);
}
