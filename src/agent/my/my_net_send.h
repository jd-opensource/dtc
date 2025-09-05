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

#ifndef _MY_NET_SEND_H_
#define _MY_NET_SEND_H_
#include "da_string.h"
#include <stdint.h>
#include <stdlib.h>

struct msg;
struct msg_tqh;

/*
MYSQL Protocol Definition, See more detail: 
  https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet
*/

int net_send_ok(struct msg *smsg, struct conn *c_conn);
int net_send_error(struct msg *smsg, struct conn *c_conn);
int net_send_switch(struct msg *smsg, struct conn *c_conn);
int net_send_server_greeting(struct conn* c, struct msg *smsg);

struct msg *net_send_desc_dtctable(struct conn *c_conn);

#endif /* _MY_NET_SEND_H_ */
