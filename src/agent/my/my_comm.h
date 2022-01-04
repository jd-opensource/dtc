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

#define MYSQL_HEADER_SIZE 4
#define MAX_PACKET_LENGTH (256L * 256L * 256L - 1)
#define DA_PROTOCOL_VERSION 2

enum enum_agent_admin { CMD_NOP = 0, CMD_KEY_DEFINE };

struct DTC_HEADER_V2 {
	uint8_t version;
	uint8_t admin;
	uint8_t reserved[2];
	uint32_t packet_len;
	uint64_t id;
};

enum AGENT_NEXT_OPERATION { NEXT_FORWARD = 0, NEXT_RSP_OK, NEXT_RSP_ERROR };

static inline int32 sint3korr(const uchar *A)
{
	return ((int32)(((A[2]) & 128) ?
				(((uint32)255L << 24) | (((uint32)A[2]) << 16) |
				 (((uint32)A[1]) << 8) | ((uint32)A[0])) :
				(((uint32)A[2]) << 16) | (((uint32)A[1]) << 8) |
					((uint32)A[0])));
}

static inline uint32 uint3korr(const uchar *A)
{
	return (uint32)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) +
			(((uint32)(A[2])) << 16));
}

static inline ulonglong uint5korr(const uchar *A)
{
	return ((ulonglong)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) +
			    (((uint32)(A[2])) << 16) +
			    (((uint32)(A[3])) << 24)) +
		(((ulonglong)(A[4])) << 32));
}

static inline ulonglong uint6korr(const uchar *A)
{
	return ((ulonglong)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) +
			    (((uint32)(A[2])) << 16) +
			    (((uint32)(A[3])) << 24)) +
		(((ulonglong)(A[4])) << 32) + (((ulonglong)(A[5])) << 40));
}

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

/*
  Since the pointers may be misaligned, we cannot do a straight read out of
  them. (It usually works-by-accident on x86 and on modern ARM, but not always
  when the compiler chooses unusual instruction for the read, e.g. LDM on ARM
  or most SIMD instructions on x86.) memcpy is safe and gets optimized to a
  single operation, since the size is small and constant.
*/

static inline int16 sint2korr(const uchar *A)
{
	int16 ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline int32 sint4korr(const uchar *A)
{
	int32 ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline uint16 uint2korr(const uchar *A)
{
	uint16 ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline uint32 uint4korr(const uchar *A)
{
	uint32 ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline ulonglong uint8korr(const uchar *A)
{
	ulonglong ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline longlong sint8korr(const uchar *A)
{
	longlong ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

#endif /* _MY_COMM_H */
