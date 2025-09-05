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

#include <string.h>
#include <stdio.h>
#include "hash.h"
#include "global.h"

DTC_USING_NAMESPACE

DTCHash::DTCHash() : _hash(NULL)
{
	memset(errmsg_, 0, sizeof(errmsg_));
}

DTCHash::~DTCHash()
{
}

NODE_ID_T &DTCHash::hash_to_node(const HASH_ID_T v)
{
	return _hash->hh_buckets[v];
}

int DTCHash::do_init(const uint32_t hsize, const uint32_t fixedsize)
{
	size_t size = sizeof(NODE_ID_T);
	size *= hsize;
	size += sizeof(HASH_T);

	MEM_HANDLE_T v = M_CALLOC(size);
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "init hash bucket failed, %s", M_ERROR());
		return -1;
	}

	_hash = M_POINTER(HASH_T, v);
	_hash->hh_size = hsize;
	_hash->hh_free = hsize;
	_hash->hh_node = 0;
	_hash->hh_fixedsize = fixedsize;

	/* init each nodeid to invalid */
	for (uint32_t i = 0; i < hsize; i++) {
		_hash->hh_buckets[i] = INVALID_NODE_ID;
	}

	return 0;
}

int DTCHash::do_attach(MEM_HANDLE_T handle)
{
	if (INVALID_HANDLE == handle) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "attach hash bucket failed, memory handle = 0");
		return -1;
	}

	_hash = M_POINTER(HASH_T, handle);
	return 0;
}

int DTCHash::do_detach(void)
{
	_hash = (HASH_T *)(0);
	return 0;
}
