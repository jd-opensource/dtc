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
* 
*/
#ifndef __ROCKSDB_KEY_COMPARATOR_H__
#define __ROCKSDB_KEY_COMPARATOR_H__

#include "log/log.h"
#include "key_format.h"
#include "rocksdb_conn.h"

#include "rocksdb/comparator.h"
using namespace rocksdb;

// compare the string with length of n, not the end of '\0'
extern int my_strn_case_cmp(const char *s1, const char *s2, size_t n)
{
	for (size_t idx = 0; idx < n; idx++) {
		char c1 = ::toupper(*(s1 + idx));
		char c2 = ::toupper(*(s2 + idx));
		if (c1 == c2)
			continue;
		if (c1 < c2)
			return -1;
		return 1;
	}

	return 0;
}

class CommKeyComparator {
    private:
	bool m_case_sensitive_key; // key need to be compared with case sensitive
	bool m_has_virtual_field;

    public:
	void set_compare_flag(bool key_sensitive, bool has_virtual_field)
	{
		m_case_sensitive_key = key_sensitive;
		m_has_virtual_field = has_virtual_field;
	}

	bool operator()(const std::string &lk, const std::string &rk) const
	{
		int ret = internal_compare(lk, rk);
		return ret < 0;
	}

	int internal_compare(const std::string &lk, const std::string &rk) const
	{
		int r;
		size_t lkLen = lk.length();
		size_t rkLen = rk.length();
		if (m_has_virtual_field) {
			std::string lk_next_str =
				lk.substr(lkLen - sizeof(uint64_t));
			std::string rk_next_str =
				rk.substr(rkLen - sizeof(uint64_t));
			uint64_t lk_next_len = 0, rk_next_len = 0;
			key_format::DecodeBytes(lk_next_str, lk_next_len);
			key_format::DecodeBytes(rk_next_str, rk_next_len);

			size_t lk_prev_len =
				lkLen - lk_next_len - sizeof(uint64_t);
			size_t rk_prev_len =
				rkLen - rk_next_len - sizeof(uint64_t);
			size_t min_len = lk_prev_len < rk_prev_len ?
						 lk_prev_len :
						 rk_prev_len;
			assert(min_len > 0);
			if (m_case_sensitive_key)
				r = lk.compare(0, min_len, rk);
			else
				r = my_strn_case_cmp(lk.data(), rk.data(),
						     min_len);

			if (r == 0) {
				if (lk_prev_len < rk_prev_len)
					r = -1;
				else if (lk_prev_len > rk_prev_len)
					r = 1;
			}

			if (r == 0) {
				assert(lk_prev_len == rk_prev_len);

				// need to compare the case sensitive free zone
				min_len = lk_next_len < rk_next_len ?
						  lk_next_len :
						  rk_next_len;
				if (!m_case_sensitive_key)
					r = memcmp(lk.data() + lk_prev_len,
						   rk.data() + rk_prev_len,
						   min_len);
				else
					r = my_strn_case_cmp(
						lk.data() + lk_prev_len,
						rk.data() + rk_prev_len,
						min_len);
			}
		} else {
			size_t min_len = lkLen < rkLen ? lkLen : rkLen;
			assert(min_len > 0);
			if (m_case_sensitive_key)
				r = lk.compare(0, min_len, rk);
			else
				r = my_strn_case_cmp(lk.data(), rk.data(),
						     min_len);
		}

		if (r == 0) {
			if (lkLen < rkLen)
				r = -1;
			else if (lkLen > rkLen)
				r = 1;
		}

		return r;
	}

	// check whether the rocksdb key, atten that, it's internal rocksdb key, not the 'key'
	// in DTC user space
	bool case_sensitive_rockskey()
	{
		return m_case_sensitive_key == true &&
		       m_has_virtual_field == false;
	}
};

// whether ignoring the case of the characters when compare the rocksdb entire key base
// on the user definition key
extern CommKeyComparator gInternalComparator;
class CaseSensitiveFreeComparator : public rocksdb::Comparator {
    public:
	CaseSensitiveFreeComparator()
	{
	}

	const char *Name() const
	{
		return "CaseSensitiveFreeComparator";
	}

	int Compare(const rocksdb::Slice &lhs, const rocksdb::Slice &rhs) const
	{
		assert(lhs.data_ != nullptr && rhs.data_ != nullptr);

		return gInternalComparator.internal_compare(lhs.ToString(),
							    rhs.ToString());
	}

	void FindShortestSeparator(std::string *start,
				   const Slice &limit) const override
	{
		// not implement now
		return;
	}

	void FindShortSuccessor(std::string *key) const override
	{
		// not implement now
		return;
	}

	// user define interface to using hash index feature
	virtual bool CanKeysWithDifferentByteContentsBeEqual() const
	{
		// 1.rocksdb will use hashtable to index the binary seach key module that reside in the
		//   tail of the datablock, it use case sensitive hash function to create hash code, so
		//   if user want to use this feature, it must ensure that keys should be case sensitive
		// 2.data block hash index only support point lookup, Range lookup request will fall back
		//   to BinarySeek
		if (gInternalComparator.case_sensitive_rockskey()) {
			log4cplus_info("use hash index feature!!!!!!!!!");
			return false;
		}

		log4cplus_info("use binary search index feature!!!!!!!!!");
		return true;
	}
};

#endif // __ROCKSDB_KEY_COMPARATOR_H__
