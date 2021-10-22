/*
 * Copyright [2021] JD.com, Inc.
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

#ifndef DA_MYSQL_PROTOCOL_H_
#define DA_MYSQL_PROTOCOL_H_
#include "da_string.h"
#include <stdint.h>
#include <stdlib.h>

struct msg;
struct msg_tqh;

/*
MYSQL Protocol Definition, See more detail: 
  https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet
*/

void mysql_parse_req(struct msg *r);
int my_ok_reply(struct msg *smsg, struct msg *dmsg);
int my_server_greeting_reply(struct msg *smsg, struct msg *dmsg);

#endif /* DA_PROTOCAL_H_ */
