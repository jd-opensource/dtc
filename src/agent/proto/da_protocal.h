/*
 * Copyright JD.com, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DA_PROTOCAL_H_
#define DA_PROTOCAL_H_
#include "da_string.h"
#include <stdint.h>
#include <stdlib.h>

#define MYSQL_ERRMSG_SIZE 512

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

} CValue;

void dtc_parse_req(struct msg *r);
void dtc_parse_rsp(struct msg *r);
int dtc_coalesce(struct msg *r);
int dtc_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq);
int dtc_error_reply(struct msg *smsg, struct msg *dmsg);

#endif /* DA_PROTOCAL_H_ */
