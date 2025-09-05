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

#ifndef _MY_PARSE_H_
#define _MY_PARSE_H_
#include "da_string.h"
#include <stdint.h>
#include <stdlib.h>

struct msg;
struct msg_tqh;

/*
MYSQL Protocol Definition, See more detail: 
  https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet
*/

void my_parse_req(struct msg *r);
void my_parse_rsp(struct msg *r);

int my_do_command(struct msg *msg);
int my_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq);

int my_get_route_key(uint8_t *sql, int sql_len, int *start_offset,
		     int *end_offset, const char* dbsession, struct msg* r);

int my_get_command(uint8_t *input_raw_packet, uint32_t input_packet_length,
		   struct msg *r, enum enum_server_command *cmd);

#ifdef __cplusplus
extern "C" {
#endif
int rule_sql_match(const char* szsql, const char* osql, const char* dbsession, char* out_dtckey, int* out_keytype);
int get_statement_value(char* str, int len, const char* strkey, int* start_offset, int* end_offset);
#ifdef __cplusplus
}
#endif

#endif /* _MY_PARSE_H_ */
