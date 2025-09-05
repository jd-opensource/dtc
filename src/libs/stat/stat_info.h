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
* 
*/
#ifndef __H_STAT_INFO_INFO_H_
#define __H_STAT_INFO_INFO_H_

#include <stddef.h>
#include <stdint.h>

enum { SA_VALUE = 0,
       SA_COUNT,
       SA_SAMPLE,
       SA_CONST,
       SA_EXPR,
};

enum { SC_CUR = 0,
       SC_10S,
       SC_10M,
       SC_ALL,
       SCC_10S,
       SCC_10M,
       SCC_ALL,
};

enum { SU_HIDE = 0,
       SU_INT,
       SU_INT_1,
       SU_INT_2,
       SU_INT_3,
       SU_INT_4,
       SU_INT_5,
       SU_INT_6,
       SU_DATETIME,
       SU_VERSION,
       SU_DATE,
       SU_TIME,
       SU_MSEC,
       SU_USEC,
       SU_PERCENT,
       SU_PERCENT_1,
       SU_PERCENT_2,
       SU_PERCENT_3,
       SU_BOOL,
};

#define STAT_CREATE_TIME 1
#define STAT_STARTUP_TIME 2
#define STAT_CHECKPOINT_TIME 3

#define MIN_STAT_ID 10
#define MAX_STAT_ID 100000000

#define EXPR_NUM(x) (((int64_t)(x)) << 32)
#define EXPR_ID_(x, y) ((x)*20 + (y))
#define EXPR_IDV(x, y, z) (EXPR_ID_(x, y) + EXPR_NUM(z))
#define EXPR_ID2(x, y, x1, y1)                                                 \
	(EXPR_ID_(x, y) + 0x80000000 + EXPR_NUM(EXPR_ID_(x1, y1)))

struct DTCStatInfo {
	// stat id
	unsigned int id;
	// offset from data file
	unsigned int offset;
	// stat attr
	unsigned char type;
	// item count
	unsigned char unit;
	// item count
	unsigned char before_count;
	// item count
	unsigned char after_count;
	// item count
	unsigned int resv;
	char name[32];
	int64_t vptr[0];

	int is_sample(void) const
	{
		return type == SA_SAMPLE;
	}
	int is_counter(void) const
	{
		return type == SA_COUNT;
	}
	int is_value(void) const
	{
		return type == SA_VALUE;
	}
	int is_const(void) const
	{
		return type == SA_CONST;
	}
	int is_expr(void) const
	{
		return type == SA_EXPR;
	}
	int data_size(void) const
	{
		return is_sample() ? sizeof(int64_t) * (16 + 2) :
				     sizeof(int64_t);
	}
	DTCStatInfo *next(void) const
	{
		return (DTCStatInfo *)((char *)this +
				       offsetof(DTCStatInfo, vptr) +
				       (is_sample() ? 16 * sizeof(int64_t) :
						      0) +
				       (is_expr() ?
						(before_count + after_count) *
							sizeof(int64_t) :
						0));
	}
};

struct DTCStatHeader {
	unsigned int signature;
	unsigned char version;
	unsigned char zero[3];
	unsigned int num_info;
	unsigned int index_size;
	unsigned int data_size;
	unsigned int creation_time;
	unsigned int reserved[2];
	char name[64];

	DTCStatInfo *first(void) const
	{
		return (DTCStatInfo *)(this + 1);
	}
	DTCStatInfo *last(void) const
	{
		return (DTCStatInfo *)((char *)this + index_size);
	}
};

struct DTCStatDefinition {
	unsigned int id;
	const char *name;
	unsigned char type;
	unsigned char unit;
	unsigned char before_count;
	unsigned char after_count;
	int64_t arg[16];
};

#endif
