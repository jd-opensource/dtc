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
	int nrp = 0, lrp = 0;
	log4cplus_debug("encode result entry.");

	DTC_HEADER_V2 dtc_header = { 0 };
	dtc_header.version = 2;
	dtc_header.id = task.get_dtc_header_id();
	dtc_header.packet_len = 0;
	dtc_header.admin = 0;
	dtc_header.dbname_len = 0;

	CBufferChain* rb = task.get_buffer_chain();
	if (!rb)
		return -3;

	rb->Count(nrp, lrp);

	CBufferChain* rbp = rb;
	for (int i = 1; i <= nrp; i++, rbp = rbp->nextBuffer) {
		dtc_header.packet_len += rbp->usedBytes;
	}
	log4cplus_debug("dtc header id: %d, packet nrp: %d, lrp: %d, packet_len: %d", dtc_header.id, nrp, lrp, dtc_header.packet_len);

	/* pool, exist and large enough, use. else free and malloc */
	int first_packet_len = sizeof(CBufferChain) +
			       sizeof(struct iovec) * (nrp + 1) +
			       sizeof(dtc_header);
	if (buf == NULL) {
		buf = (CBufferChain *)MALLOC(first_packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = first_packet_len - sizeof(CBufferChain);
		buf->nextBuffer = NULL;
	} else if (buf && first_packet_len - (int)sizeof(CBufferChain) >
				  buf->totalBytes) {
		FREE_IF(buf);
		buf = (CBufferChain *)MALLOC(first_packet_len);
		if (buf == NULL) {
			return -ENOMEM;
		}
		buf->totalBytes = first_packet_len - sizeof(CBufferChain);
		buf->nextBuffer = NULL;
	}

	//设置要发送的第一个包
	char *p = buf->data + sizeof(struct iovec) * (nrp + 1);
	v = (struct iovec *)buf->data;
	v->iov_base = p;
	v->iov_len = sizeof(dtc_header);
	nv = nrp + 1;
	buf->usedBytes = sizeof(struct iovec) * (nrp + 1) + sizeof(dtc_header);

	//修改第一个包的内容
	memcpy(p, &dtc_header, sizeof(dtc_header));
	p += sizeof(dtc_header);
	if (p - (char *)v->iov_base != sizeof(dtc_header))
		fprintf(stderr, "%s(%d): BAD ENCODER len=%ld must=%d\n",
			__FILE__, __LINE__, (long)(p - (char *)v->iov_base),
			sizeof(dtc_header));

	buf->nextBuffer = rb;
	for (int i = 1; i <= nrp; i++, rb = rb->nextBuffer) {
		v[i].iov_base = rb->data;
		v[i].iov_len = rb->usedBytes;
	}

	log4cplus_debug("encode result leave.");

	return 0;
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
