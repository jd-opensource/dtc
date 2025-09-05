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

#ifndef __DTC_EMPTY_FILTER_H
#define __DTC_EMPTY_FILTER_H

#include "namespace.h"
#include "algorithm/singleton.h"
#include "global.h"

DTC_BEGIN_NAMESPACE

#define DF_ENF_TOTAL 0 /* 0 = unlimited */
#define DF_ENF_STEP 512 /* byte */
#define DF_ENF_MOD 30000

struct _enf_table {
	MEM_HANDLE_T t_handle;
	uint32_t t_size;
};
typedef struct _enf_table ENF_TABLE_T;

struct _empty_node_filter {
	uint32_t enf_total; // Total memory occupied
	uint32_t enf_step; // Table growth step size
	uint32_t enf_mod; // Table partitioning modulus

	ENF_TABLE_T enf_tables[0]; // Bitmap tables
};
typedef struct _empty_node_filter ENF_T;

class EmptyNodeFilter {
    public:
	void SET(uint32_t key);
	void CLR(uint32_t key);
	int ISSET(uint32_t key);

    public:
	/* 0 = use default value */
	int do_init(uint32_t total = 0, uint32_t step = 0, uint32_t mod = 0);
	int do_attach(MEM_HANDLE_T);
	int do_detach(void);

    public:
	EmptyNodeFilter();
	~EmptyNodeFilter();
	static EmptyNodeFilter *instance()
	{
		return Singleton<EmptyNodeFilter>::instance();
	}
	static void destory()
	{
		Singleton<EmptyNodeFilter>::destory();
	}
	const char *error() const
	{
		return errmsg_;
	}
	const MEM_HANDLE_T get_handle() const
	{
		return M_HANDLE(_enf);
	}

    private:
	/* Calculate table ID */
	uint32_t get_index(uint32_t key)
	{
		return key % _enf->enf_mod;
	}
	/* Calculate bitmap offset in table */
	uint32_t get_offset(uint32_t key)
	{
		return key / _enf->enf_mod;
	}

    private:
	ENF_T *_enf;
	char errmsg_[256];
};

DTC_END_NAMESPACE

#endif
