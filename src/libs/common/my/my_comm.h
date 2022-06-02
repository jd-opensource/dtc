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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "my_inttypes.h"

/*
MYSQL Protocol Definition, See more detail: 
  https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet
*/

#define MYSQL_HEADER_SIZE 4
#define MAX_PACKET_LENGTH (256L * 256L * 256L - 1)
#define DA_PROTOCOL_VERSION 2

enum enum_agent_admin { CMD_NOP = 0, CMD_KEY_DEFINE };

enum AGENT_NEXT_OPERATION { NEXT_FORWARD = 0, NEXT_RSP_OK, NEXT_RSP_ERROR };

static inline int32 int_trans_3(const uchar *A)
{
	return ((int32)(((A[2]) & 128) ?
				(((uint32)255L << 24) | (((uint32)A[2]) << 16) |
				 (((uint32)A[1]) << 8) | ((uint32)A[0])) :
				(((uint32)A[2]) << 16) | (((uint32)A[1]) << 8) |
					((uint32)A[0])));
}

static inline uint32 uint_trans_3(const uchar *A)
{
	return (uint32)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) +
			(((uint32)(A[2])) << 16));
}

static inline void int_conv_3(uchar *T, uint A)
{
	*(T) = (uchar)(A);
	*(T + 1) = (uchar)(A >> 8);
	*(T + 2) = (uchar)(A >> 16);
}

static inline uint16 uint_conv_2(const uchar *A)
{
	uint16 ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline uint32 uint_conv_4(const uchar *A)
{
	uint32 ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline void int2store_big_endian(uchar *T, uint16 A)
{
	uint def_temp = A;
	*(T) = (uchar)(def_temp);
	*(T + 1) = (uchar)(def_temp >> 8);
}

static inline void int4store_big_endian(uchar *T, uint32 A)
{
	*(T) = (uchar)(A);
	*(T + 1) = (uchar)(A >> 8);
	*(T + 2) = (uchar)(A >> 16);
	*(T + 3) = (uchar)(A >> 24);
}

#endif /* _MY_COMM_H */
