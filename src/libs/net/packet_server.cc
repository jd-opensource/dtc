#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <new>

#include "packet.h"
#include "task_request.h"

#include "log.h"

int CPacket::EncodeResult(CTaskRequest &task, int mtu)
{
	CPacketHeader header;

	header.magic = htons(0xFDFC);
	header.cmd = htons(task.GetReqCmd());
	header.len = htonl(task.get_result_len());
	header.seq_num = htonl(task.get_seq_number());
	bytes = EncodeHeader(header);
	const int len = bytes;

	/* pool, exist and large enough, use. else free and malloc */
	int total_len = sizeof(CBufferChain)+sizeof(struct iovec)+len;
	if(buf == NULL)
	{
	    buf = (CBufferChain *)MALLOC(total_len);
	    if(buf==NULL)
	    {
		return -ENOMEM;
	    }
        buf->totalBytes = total_len - sizeof(CBufferChain);
	}else if(buf && buf->totalBytes < total_len - (int)sizeof(CBufferChain))
	{
	    FREE_IF(buf);
	    buf = (CBufferChain *)MALLOC(total_len);
	    if(buf==NULL)
	    {
		 return -ENOMEM;
	    }
        buf->totalBytes = total_len - sizeof(CBufferChain);
	}

	buf->nextBuffer = NULL;
	v = (struct iovec *)buf->data;
	nv = 1;
	char *p = buf->data + sizeof(struct iovec);
	v->iov_base = p;
	v->iov_len = len;
	memcpy(p, &header, sizeof(header));
	p += sizeof(header);

	memcpy(p, task.get_result(), task.get_result_len());//copy the result string to the packet

	return len;
}

void CPacket::FreeResultBuff()
{
	CBufferChain * resbuff = buf->nextBuffer;
	buf->nextBuffer = NULL;

	while(resbuff) {
              char *p = (char *)resbuff;
               resbuff = resbuff->nextBuffer;
               FREE(p);
        }    
}

int CPacket::Bytes(void) 
{
	int sendbytes = 0;
	for(int i = 0; i < nv; i++)
	{
		sendbytes += v[i].iov_len;
	} 
	return sendbytes;
}

int CPacket::EncodeHeader(const CPacketHeader &header)
{
	int len = sizeof(header);
	len += ntohl(header.len);
	return len;
}
