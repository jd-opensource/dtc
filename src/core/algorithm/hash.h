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

#ifndef __DTC_HASH_H
#define __DTC_HASH_H

#include "namespace.h"
#include "algorithm/singleton.h"
#include "global.h"
#include "node/node.h"
#include "algorithm/new_hash.h"

DTC_BEGIN_NAMESPACE

struct _hash {
	uint32_t hh_size; // hash size
	uint32_t hh_free; // number of free hash slots
	uint32_t hh_node; // total number of attached nodes
	uint32_t hh_fixedsize; // key size: for variable-length keys, hh_fixedsize = 0; otherwise it's the actual length
	uint32_t hh_buckets[0]; // hash bucket start
};
typedef struct _hash HASH_T;

class DTCHash {
    public:
	DTCHash();
	~DTCHash();

	static DTCHash *instance()
	{
		return Singleton<DTCHash>::instance();
	}
	static void destroy()
	{
		Singleton<DTCHash>::destory();
	}

	inline HASH_ID_T new_hash_slot(const char *key)
	{
		//the previous byte of variable-length key encodes the key length
		uint32_t size = _hash->hh_fixedsize ? _hash->hh_fixedsize :
						      *(unsigned char *)key++;

		//currently only supports 1, 2, 4-byte fixed-length keys
		switch (size) {
		case sizeof(unsigned char):
			return (*(unsigned char *)key) % _hash->hh_size;
		case sizeof(unsigned short):
			return (*(unsigned short *)key) % _hash->hh_size;
		case sizeof(unsigned int):
			return (*(unsigned int *)key) % _hash->hh_size;
		}

		unsigned int h = new_hash(key, size);
		return h % _hash->hh_size;
	}

	inline HASH_ID_T hash_slot(const char *key)
	{
		//the previous byte of variable-length key encodes the key length
		uint32_t size = _hash->hh_fixedsize ? _hash->hh_fixedsize :
						      *(unsigned char *)key++;

		//currently only supports 1, 2, 4-byte fixed-length keys
		switch (size) {
		case sizeof(unsigned char):
			return (*(unsigned char *)key) % _hash->hh_size;
		case sizeof(unsigned short):
			return (*(unsigned short *)key) % _hash->hh_size;
		case sizeof(unsigned int):
			return (*(unsigned int *)key) % _hash->hh_size;
		}

		unsigned int h = 0, g = 0;
		const char *arEnd = key + size;

		//variable-length key hash algorithm, currently 8-byte fixed-length integer keys are also treated as variable-length hash.
		while (key < arEnd) {
			h = (h << 4) + *key++;
			if ((g = (h & 0xF0000000))) {
				h = h ^ (g >> 24);
				h = h ^ g;
			}
		}
		return h % _hash->hh_size;
	}

	NODE_ID_T &hash_to_node(const HASH_ID_T);

	const MEM_HANDLE_T get_handle() const
	{
		return M_HANDLE(_hash);
	}
	const char *error() const
	{
		return errmsg_;
	}

	//create and format physical memory
	int do_init(const uint32_t hsize, const uint32_t fixedsize);
	//bind to physical memory
	int do_attach(MEM_HANDLE_T handle);
	//detach from physical memory
	int do_detach(void);

	uint32_t hash_size() const
	{
		return _hash->hh_size;
	}
	uint32_t free_bucket() const
	{
		return _hash->hh_free;
	}
	void inc_free_bucket(int v)
	{
		_hash->hh_free += v;
	}
	void inc_node_cnt(int v)
	{
		_hash->hh_node += v;
	}

    private:
	HASH_T *_hash;
	char errmsg_[256];
};

DTC_END_NAMESPACE

#endif
