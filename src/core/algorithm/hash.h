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

#ifndef __DTC_HASH_H
#define __DTC_HASH_H

#include "namespace.h"
#include "algorithm/singleton.h"
#include "global.h"
#include "node/node.h"
#include "algorithm/new_hash.h"

DTC_BEGIN_NAMESPACE

struct _hash {
	uint32_t hh_size; // hash 大小
	uint32_t hh_free; // 空闲的hash数量
	uint32_t hh_node; // 挂接的node总数量
	uint32_t hh_fixedsize; // key大小：变长key时，hh_fixedsize = 0;其他就是其实际长度
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
		//变长key的前一个字节编码的是key的长度
		uint32_t size = _hash->hh_fixedsize ? _hash->hh_fixedsize :
						      *(unsigned char *)key++;

		//目前仅支持1、2、4字节的定长key
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
		//变长key的前一个字节编码的是key的长度
		uint32_t size = _hash->hh_fixedsize ? _hash->hh_fixedsize :
						      *(unsigned char *)key++;

		//目前仅支持1、2、4字节的定长key
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

		//变长key hash算法, 目前8字节的定长整型key也是作为变长hash的。
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

	//创建物理内存并格式化
	int do_init(const uint32_t hsize, const uint32_t fixedsize);
	//绑定到物理内存
	int do_attach(MEM_HANDLE_T handle);
	//脱离物理内存
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
