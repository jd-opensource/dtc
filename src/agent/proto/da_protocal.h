/*
 * da_protocal.h
 *
 *  Created on: 2014Äê12ÔÂ23ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_PROTOCAL_H_
#define DA_PROTOCAL_H_
#include <stdlib.h>
#include <stdint.h>
#include "da_string.h"

struct msg;
struct msg_tqh;

struct CPacketHeader {
	uint8_t version;
	uint8_t scts;
	uint8_t flags;
	uint8_t cmd;
	uint32_t len[8];
};

typedef union CValue {
// member
	int64_t s64;
	uint64_t u64;
	double flt;
	struct string str;

}CValue;

void dtc_parse_req(struct msg *r);
void dtc_parse_rsp(struct msg *r);
int dtc_coalesce(struct msg *r);
int dtc_fragment(struct msg *r,uint32_t ncontinuum,struct msg_tqh *frag_msgq);
int dtc_error_reply(struct msg *smsg,struct msg *dmsg);


#endif /* DA_PROTOCAL_H_ */
