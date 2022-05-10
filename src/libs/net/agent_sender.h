/*
 * =====================================================================================
 *
 *       Filename:  agent_sender.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/31/2010 08:43:49 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  newmanwang (nmwang), newmanwang@tencent.com
 *        Company:  Tencent, China
 *
 * =====================================================================================
 */

#ifndef __AGENT_SENDER_H__
#define __AGENT_SENDER_H__

#include <stdint.h>

#define SENDER_MAX_VEC 1024

class CPacket;
class CAgentSender
{
    private:
	int fd;
	struct iovec * vec;
	uint32_t totalVec;
	uint32_t currVec;
	CPacket ** packet;
	uint32_t totalPacket;
	uint32_t currPacket;

	uint32_t totalLen;
	uint32_t sended;
	uint32_t leftLen;

	int broken;

    public:
	CAgentSender(int f);
	virtual ~CAgentSender();

	int Init();
	int IsBroken() { return broken; }
	int AddPacket(CPacket * pkt);
	int Send();
	inline uint32_t TotalLen() { return totalLen; }
	inline uint32_t Sended() { return sended; }
	inline uint32_t LeftLen() { return leftLen; }
	inline uint32_t VecCount() { return currVec; }
};

#endif
