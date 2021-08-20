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
#ifndef __ADAPTIVE_PREFIX_EXTRACTOR_H__
#define __ADAPTIVE_PREFIX_EXTRACTOR_H__

#include "rocksdb/slice_transform.h"
#include "key_format.h"
#include "log/log.h"

#include <algorithm>

// prefix extractor for prefix search
class AdaptivePrefixExtractor : public SliceTransform {
    private:
	int mkey_type; // dtc key key type

    public:
	AdaptivePrefixExtractor(int key_type)
	{
		mkey_type = key_type;
	}

	virtual const char *Name() const
	{
		return "AdaptivePrefixExtractor";
	}

	// extract the prefix key base on customer define, and this function is used
	// when create the bloom filter
	virtual Slice Transform(const Slice &key) const
	{
		static bool printLog = true;
		if (printLog) {
			log4cplus_error("use prefix extractor!!!!!!");
			printLog = false;
		}

		// extract the key field as the prefix slice
		int keyLen = key_format::get_field_len(key.data_, mkey_type);
		return Slice(key.data_, (size_t)keyLen);
	}

	// 1.before call 'Transform' function to do logic extract, rocksdb firstly call
	//   'InDomain' to determine whether the key compatible with the logic specified
	//   in the Transform method.
	// 2.inserted key always contains the prefix, so just return true
	virtual bool InDomain(const Slice &key) const
	{
		return key.size_ > 0;
	}

	// This is currently not used and remains here for backward compatibility.
	virtual bool InRange(const Slice & /*dst*/) const
	{
		return false;
	}

	virtual bool FullLengthEnabled(size_t *len) const
	{
		log4cplus_error("unexpected call !!!!!");

		// unfixed dynamic key always return false to disable this function
		return false;
	}

	// this function is only used by customer user to check whether the 'prefixKey'
	// can use the current extractor, always turn true
	virtual bool SameResultWhenAppended(const Slice &prefixKey) const
	{
		return true;
	}
};

#endif // __ADAPTIVE_PREFIX_EXTRACTOR_H__
