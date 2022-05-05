#ifndef AGENT_MULTI_REQUEST_H___
#define AGENT_MULTI_REQUEST_H___

#include "list.h"
#include "value.h"

class CTaskRequest;
typedef struct{
    CTaskRequest * volatile task;
    volatile int processed;
}DecodedTask;

class CClientAgent;
class CAgentMultiRequest: public CListObject<CAgentMultiRequest>
{
    public:
	CAgentMultiRequest(CTaskRequest * o);
	virtual ~CAgentMultiRequest();

        int DecodeAgentRequest();
	inline int PacketCount() { return packetCnt; }
	inline CTaskRequest * CurrTask(int index) { return taskList[index].task; }
	void CopyReplyForSubTask();
	void ClearOwnerInfo();
	inline bool IsCompleted() { return compleTask == packetCnt; }
	void CompleteTask(int index);
	inline void DetachFromOwnerClient() { ListDel(); }
	inline bool IsCurrTaskProcessed(int index) { return taskList[index].processed == 1; }
	inline void SaveRecvedResult(char * buff, int len, int pktcnt) 
	{ packets.Set(buff, len); packetCnt = pktcnt;}

    private:
	CBinary packets;
	int packetCnt;
	CTaskRequest * owner;
	DecodedTask * volatile taskList;	
	volatile int compleTask;
	CClientAgent * volatile ownerClient;

    private:
	void DecodeOneRequest(char * packetstart, int packetlen, int index);
};

#endif
