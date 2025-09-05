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
#ifndef __DTC_JOURNAL_ID
#define __DTC_JOURNAL_ID

#include <stdint.h>

struct JournalID {
	uint32_t serial;
	uint32_t offset;

	JournalID()
	{
		serial = 0;
		offset = 0;
	}

	JournalID(const JournalID &v)
	{
		serial = v.serial;
		offset = v.offset;
	}

	JournalID(uint32_t s, uint32_t o)
	{
		serial = s;
		offset = o;
	}

	JournalID &operator=(const JournalID &v)
	{
		serial = v.serial;
		offset = v.offset;
		return *this;
	}

	uint32_t Serial() const
	{
		return serial;
	}

	uint32_t get_offset() const
	{
		return offset;
	}

	/*
         * All external interfaces are packaged as uint64_t for convenience.
         */
	JournalID &operator=(const uint64_t v)
	{
		serial = v >> 32;
		offset = v & 0xFFFFFFFFULL;
		return *this;
	}

	JournalID(uint64_t v)
	{
		serial = v >> 32;
		offset = v & 0xFFFFFFFFULL;
	}

	operator uint64_t() const
	{
		uint64_t v = serial;
		v <<= 32;
		v += offset;
		return v;
	}

	int Zero() const
	{
		return serial == 0 && offset == 0;
	}

	int GE(const JournalID &v)
	{
		return serial > v.serial ||
		       (serial == v.serial && offset >= v.offset);
	}

} __attribute__((packed));

#endif
