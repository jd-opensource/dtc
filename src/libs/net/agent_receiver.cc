#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <string.h>

#include "agent_receiver.h"
#include "log.h"
#include "memcheck.h"
#include "protocol.h"
#include "task_request.h"

CAgentReceiver::CAgentReceiver(int f):
	fd(f),
	buffer(NULL),
	offset(0),
	buffSize(0),
	pktTail(0),
	pktCnt(0)
{
}

CAgentReceiver::~CAgentReceiver()
{
	/* fd will closed by CClientAgent */
	if(buffer)
		free(buffer);
}

int CAgentReceiver::Init()
{
	buffer = (char *)malloc(AGENT_INIT_RECV_BUFF_SIZE);
	if(NULL == buffer)
		return -1;
	buffSize = AGENT_INIT_RECV_BUFF_SIZE;
	return 0;
}

/*
discription: recv
output:
<0: recv error or remote close
=0: nothing recved
>0: recved 
*/
int CAgentReceiver::RecvOnce()
{
	int rv;

	rv = recv(fd, offset + buffer, buffSize - offset, 0); 
	if(rv < 0)
	{
		if(EAGAIN == errno || EINTR == errno || EINPROGRESS == errno)
			return 0;
		log4cplus_error("agent receiver recv error: %m, %d", errno);
		return -errno;
	}else if(0 == rv)
	{
		log4cplus_debug("remote close connection, fd[%d]", fd);
		errno = ECONNRESET;
		return -errno;
	}

	offset += rv;
   
	return rv;
}

/*
return value:
<0: error
=0: nothing recved or have no completed packet
>0: recved and have completed packet
*/
RecvedPacket CAgentReceiver::Recv()
{
    int err = 0;
    RecvedPacket packet;

    memset((void *)&packet, 0, sizeof(packet));

    if((err = RealRecv()) < 0 )
    {
		packet.err = -1;
		return packet;
    }
    else if(err == 0)
		return packet;

    if((err = RecvAgain()) < 0)
    {
		packet.err = -1;
		return packet;
    }

    SetRecvedInfo(packet);

    return packet;
}

/*
discription: recv data
output:
<0: recv error or remote close
=0: nothing recved
>0: received
*/
int CAgentReceiver::RealRecv()
{
	int rv = 0;
 
        if(IsNeedEnlargeBuffer())
	{
		if(EnlargeBuffer() < 0)
		{
			log4cplus_error("no mme enlarge recv buffer error");
			return -ENOMEM;
		}	
	}
		
	if((rv = RecvOnce()) < 0)
		return -1;

	return rv;
}

/*
output:
=0: no need enlarge recv buffer or enlarge ok but recv nothing
<0: error
>0: recved
*/
int CAgentReceiver::RecvAgain()
{
	int rv = 0;

	if(!IsNeedEnlargeBuffer())
		return 0;

	if(EnlargeBuffer() < 0)
	{
		log4cplus_error("no mme enlarge recv buffer error");
		return -ENOMEM;
	}

	if((rv = RecvOnce()) < 0)
		return -1;

	return rv;
}

int CAgentReceiver::DecodeHeader(DTC_HEADER_V2 * header)
{
    int pktbodylen = 0;

	if(header->version != 2)
	{
		log4cplus_error("dtc header version error: %d", header->version);
    	return -1;
	}

    pktbodylen = header->packet_len;
	log4cplus_debug("version: %d, pkt len: %d, id: %d, layer: %d, admin: %d", header->version, header->packet_len, header->id, header->layer, header->admin);
    if(pktbodylen > MAXPACKETSIZE)
    {
    	log4cplus_error("packet len > MAXPACKETSIZE 20M");
    	return -1;
    }

    return pktbodylen - sizeof(DTC_HEADER_V2);
}

int CAgentReceiver::CountPacket()
{
    char * pos = buffer;
    int leftlen = offset;
    
    pktCnt = 0;

    if(pos == NULL || leftlen == 0)
        return 0;

    while(1)
    {
        int pktbodylen = 0;
        DTC_HEADER_V2* header = NULL;

        if(leftlen < (int)sizeof(DTC_HEADER_V2))
            break;

        header = (DTC_HEADER_V2 *)pos;
        pktbodylen = DecodeHeader(header);
        if(pktbodylen < 0)
        	return -1;

        if(leftlen < (int)sizeof(DTC_HEADER_V2) + pktbodylen)
            break;

		pos += sizeof(DTC_HEADER_V2) + pktbodylen;
        leftlen -= sizeof(DTC_HEADER_V2) + pktbodylen;
        pktCnt++;
    }

    pktTail = pos - buffer;
    return 0;
}

/*
return value:
<0: error
=0: contain no packet
>0: contain packet
*/
void CAgentReceiver::SetRecvedInfo(RecvedPacket & packet)
{
    if(CountPacket() < 0)
    {
		packet.err = -1;
		return;
    }

    /* not even ont packet recved, do nothing */
    if(pktCnt == 0)
        return;

    char * tmpbuff;
    if(NULL == (tmpbuff = (char *)malloc(buffSize)))
    {
	    /* recved buffer willbe free by outBuff */
	    log4cplus_error("no mem malloc new recv buffer");
	    packet.err = -1;
	    return;
    }

    if(pktTail < offset)
        memcpy(tmpbuff, buffer + pktTail, offset - pktTail);

    packet.buff = buffer;
    packet.len = pktTail;
    packet.pktCnt = pktCnt;

    buffer = tmpbuff;
    offset -= pktTail;

    return;
}

/*
output:
=0: enlarge ok
<0: enlarge error
*/
int CAgentReceiver::EnlargeBuffer()
{
	buffer = (char *)REALLOC(buffer, buffSize * 2);
	if(buffer == NULL)
		return -1;
	buffSize *= 2;
	return 0;
}

bool CAgentReceiver::IsNeedEnlargeBuffer()
{
    return offset == buffSize;
}
