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

#ifndef _MY_COMM_H
#define _MY_COMM_H
#include "da_string.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "my_inttypes.h"

/*
MYSQL Protocol Definition, See more detail: 
  https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet
*/

#define HEADER_SIZE 4
#define MAX_PACKET_LENGTH (256L * 256L * 256L - 1)

/**
  int3store

  Stores an unsinged integer in a platform independent way

  @param T  The destination buffer. Must be at least 3 bytes long
  @param A  The integer to store.

  _Example:_
  A @ref a_protocol_type_int3 "int \<3\>" with the value 1 is stored as:
  ~~~~~~~~~~~~~~~~~~~~~
  01 00 00
  ~~~~~~~~~~~~~~~~~~~~~
*/
static inline void int3store(uchar *T, uint A)
{
	*(T) = (uchar)(A);
	*(T + 1) = (uchar)(A >> 8);
	*(T + 2) = (uchar)(A >> 16);
}

#endif /* _MY_COMM_H */
