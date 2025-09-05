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
#ifndef __DTC_KEY_CODEC_H
#define __DTC_KEY_CODEC_H

#include <string.h>
#include <stdint.h>
#include "namespace.h"

DTC_BEGIN_NAMESPACE

class KeyCodec {
    public:
	KeyCodec(const unsigned t)
	{
		set_key_type(t);
	}

	~KeyCodec()
	{
	}

	void set_key_type(const unsigned t)
	{
		type = t;
	}
	unsigned key_length(const char *ptr)
	{
		return type > 0 ? type : *(unsigned char *)ptr;
	}
	unsigned total_length(const char *ptr)
	{
		return type > 0 ? key_length(ptr) : key_length(ptr) + 1;
	}
	const char *key_pointer(const char *ptr)
	{
		return type > 0 ? ptr : ptr + 1;
	}
	int key_compare(const char *a, const char *b)
	{
		switch (type) {
		case 1:
			return *(uint8_t *)a - *(uint8_t *)b;
		case 2:
			return *(uint16_t *)a - *(uint16_t *)b;
		case 4:
			return *(uint32_t *)a - *(uint32_t *)b;
		}

		return memcmp(a, b, total_length(a));
	}

	unsigned key_hash(const char *ptr)
	{
		unsigned hash = 0x123;
		unsigned len = total_length(ptr);

		do {
			unsigned char c = *ptr++;
			//c = icase_hash(c);
			hash = hash * 101 + c;
		} while (--len);

		return hash;
	}

    private:
	/*
		 * This removes bit 5 if bit 6 is set.  (from git name-hash.c)
		 *
		 * That will make US-ASCII characters hash to their upper-case
		 * equivalent. We could easily do this one whole word at a time,
		 * but that's for future worries.
		 */
	static inline unsigned char icase_hash(unsigned char c)
	{
		return c & ~((c & 0x40) >> 1);
	}

    private:
	unsigned type;
};

DTC_END_NAMESPACE
#endif
