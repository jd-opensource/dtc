#ifndef __H_TTC_REQUEST_REAL_H__
#define __H_TTC_REQUEST_REAL_H__

#include <string>
#include "request_base_all.h"
//#include "task_base.h"
#include "stopwatch.h"
//#include "hotbacktask.h"
//class CDecoderUnit;
//class CMultiRequest;
//class NCKeyValueList;
//class NCRequest;
class CAgentMultiRequest;
class CClientAgent;

enum RequestCmd {
	SERVICE_NONE = 100,
	SERVICE_SEARCH,
	SERVICE_SUGGEST,
	SERVICE_CLICK,
	SERVICE_RELATE,
	SERVICE_HOT,
	SERVICE_INDEXGEN,
	SERVICE_TOPINDEX,
	SERVICE_SNAPSHOT, // snapshot
	SERVICE_PIC,
	SERVICE_CONF_UPD,
	SERVICE_ROUTE_UPD,
	SERVICE_TRANSACTION = 1000,		//dbproxy transaction
	SERVICE_OTHER
};

class CTaskOwnerInfo {
private:
	const struct sockaddr *clientaddr;
	void *volatile ownerInfo;
	int ownerIndex;

public:
	CTaskOwnerInfo(void) : clientaddr(NULL), ownerInfo(NULL), ownerIndex(0) {}
	virtual ~CTaskOwnerInfo(void) {}

	void SetOwnerInfo(void *info, int idx, const struct sockaddr *addr) {
		ownerInfo = info;
		ownerIndex = idx;
		clientaddr = addr;
	}

	inline void Clean(){ownerInfo = NULL;}

	void ClearOwnerInfo(void) { ownerInfo = NULL; }

	template<typename T>
		T *OwnerInfo(void) const { return (T *)ownerInfo; }

	const struct sockaddr *OwnerAddress(void) const { return clientaddr; }
	int OwnerIndex(void) const { return ownerIndex; }
};

class CTaskRequest:public CTaskReplyList<CTaskRequest, 10>, public CTaskOwnerInfo
{
public:
	CTaskRequest():
		timestamp(0),
		expire_time(0),
		agentMultiReq(NULL),
                ownerClient(NULL),
                recvBuff(NULL),
                recvLen(0),
                recvPacketCnt(0),
				seq_number(0),
				request_cmd(0),
		resourceId(0),
		resourceSeq(0)
	{
	};

	virtual ~CTaskRequest();

public:
	void Clean();
	// msecond: absolute ms time
	uint64_t DefaultExpireTime(void) { return 5000 /*default:5000ms*/; }
	const uint64_t ExpireTime(void) const { return expire_time; }

	int IsExpired(uint64_t now) const {
		// client gone, always expired
		if(OwnerInfo<void>() == NULL)
			return 1;
		return expire_time <= now;
	}
	uint32_t Timestamp(void) const { return timestamp; }
	void RenewTimestamp(void) { timestamp = time(NULL); }

public:
	int PrepareProcess(void);

private:
	/* following filed should be clean:
	 * blacklist_size
	 * timestamp
	 * barHash
	 * expire_time
	 *
	 * */
	uint32_t timestamp;

	uint64_t expire_time; /* ms */ /* derived from packet */
    char remoteAddr[32];

    /* for agent request */
private:
	CAgentMultiRequest * agentMultiReq;
	CClientAgent * ownerClient;
	char * recvBuff;
	int recvLen;
	int recvPacketCnt;
	std::string result;
	uint32_t seq_number;
	uint16_t request_cmd;
	std::string m_sql;
	uint8_t mysql_seq_id;

	char* send_buff;
	int send_len;

public:
	/* for agent request */
	void SetOwnerClient(CClientAgent * client);
	CClientAgent * OwnerClient();
	void ClearOwnerClient();

	int DecodeAgentRequest();
	inline void SaveRecvedResult(char * buff, int len, int pktcnt)
	{
		recvBuff = buff; recvLen = len; recvPacketCnt = pktcnt;
	}
	inline void setResult(const std::string &value){
		result = value;
	}
	inline void set_seq_number(uint32_t seq_nb){
		seq_number = seq_nb;
	}
	inline void SetReqCmd(uint16_t cmd){
		request_cmd = cmd;
	}
	uint8_t get_seq_num() {
		return mysql_seq_id;
	}

	const char* get_result(void);
	int get_result_len()
	{
		if(send_buff && send_len > 0)
			return send_len;
		else
			return result.length();
	}

	inline uint32_t get_seq_number(void) const{
		return seq_number;
	}
	inline uint16_t GetReqCmd(void) const{
		return request_cmd;
	}
	bool copyOnePacket(const char *packet, int pktLen);
	std::string parse_request_sql();
	bool IsAgentRequestCompleted();
	void DoneOneAgentSubRequest();

	void LinkToOwnerClient(CListObject<CAgentMultiRequest> & head);

	int AgentSubTaskCount();
	void CopyReplyForAgentSubTask();
	CTaskRequest * CurrAgentSubTask(int index);

	int get_db_layer_level();
	int do_mysql_protocol_parse();

	bool IsCurrSubTaskProcessed(int index);
private:
	void PassRecvedResultToAgentMutiReq();
	
public:	// timing
	stopwatch_usec_t responseTimer;
	void ResponseTimerStart(void) { responseTimer.start(); }
	unsigned int resourceId;
	uint32_t resourceSeq;
};

#endif
