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
#ifndef __H_DTC_TASK_PKEY_H__
#define __H_DTC_TASK_PKEY_H__

#include <stdint.h>

class DTCTableDefinition;
union DTCValue;

class TaskPackedKey {
    public:
	static int packed_key_size(const char *p, int keyformat)
	{
		return keyformat ?: 1 + *(uint8_t *)p;
	}
	static void unpack_key(const DTCTableDefinition *, const char *,
			       DTCValue *);
	static int store_value(char *p, uint64_t val, int type, int size)
	{
		static uint64_t intoff[16] = {
			0x0000000000000080ULL, // 1s
			0x0000000000008000ULL, // 2s
			0x0000000000800000ULL, // 3s
			0x0000000080000000ULL, // 4s
			0x0000008000000000ULL, // 5s
			0x0000800000000000ULL, // 6s
			0x0080000000000000ULL, // 7s
			0x0000000000000000ULL, // 8s
			0x0000000000000000ULL, // 1u
			0x0000000000000000ULL, // 2u
			0x0000000000000000ULL, // 3u
			0x0000000000000000ULL, // 4u
			0x0000000000000000ULL, // 5u
			0x0000000000000000ULL, // 6u
			0x0000000000000000ULL, // 7u
			0x0000000000000000ULL, // 8u
		};

		static uint64_t intmask[8] = {
			0xFFFFFFFFFFFFFF00ULL, // 1
			0xFFFFFFFFFFFF0000ULL, // 2
			0xFFFFFFFFFF000000ULL, // 3
			0xFFFFFFFF00000000ULL, // 4
			0xFFFFFF0000000000ULL, // 5
			0xFFFF000000000000ULL, // 6
			0xFF00000000000000ULL, // 7
			0x0000000000000000ULL, // 8
		};

		if (((val + intoff[type * 8 + size - 9]) & intmask[size - 1]) !=
		    0)
			return -1; // overflow;
#if __BYTE_ORDER == __LITTLE_ENDIAN
		if (size == 4)
			*(uint32_t *)p = val;
		else if (size == 8)
			*(uint64_t *)p = val;
		else
			memcpy(p, &val, size);
#elif __BYTE_ORDER == __BIG_ENDIAN
		memcpy(p, (char *)&val + (sizeof(uint64_t) - size), size);
#else
#error unkown endian
#endif
		return 0;
	}

	static int build_packed_key(const DTCTableDefinition *,
				    const DTCValue *, unsigned int bufsz,
				    char packKey[]);
	static unsigned long
	calculate_barrier_key(const DTCTableDefinition *tdef, const char *pkey);
};

#endif
