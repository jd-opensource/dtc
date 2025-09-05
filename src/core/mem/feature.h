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

#ifndef __DTC_FEATURE_H
#define __DTC_FEATURE_H

#include "namespace.h"
#include "global.h"

DTC_BEGIN_NAMESPACE

// feature type
enum feature_id {
	NODE_GROUP = 10, //DTC begin feature id
	NODE_INDEX,
	HASH_BUCKET,
	TABLE_INFO,
	EMPTY_FILTER,
	HOT_BACKUP,
	COL_EXPAND,
};
typedef enum feature_id FEATURE_ID_T;

struct feature_info {
	uint32_t fi_id; // feature id
	uint32_t fi_attr; // feature attribute
	MEM_HANDLE_T fi_handle; // feature handler
};
typedef struct feature_info FEATURE_INFO_T;

struct base_info {
	uint32_t bi_total; // total features
	FEATURE_INFO_T bi_features[0];
};
typedef struct base_info BASE_INFO_T;

class Feature {
    public:
	static Feature *instance();
	static void destroy();

	MEM_HANDLE_T get_handle() const
	{
		return M_HANDLE(_baseInfo);
	}
	const char *error() const
	{
		return errmsg_;
	}

	int modify_feature(FEATURE_INFO_T *fi);
	int delete_feature(FEATURE_INFO_T *fi);
	int add_feature(const uint32_t id, const MEM_HANDLE_T v,
			const uint32_t attr = 0);
	FEATURE_INFO_T *get_feature_by_id(const uint32_t id);

	//Create physical memory and format
	int do_init(const uint32_t num = MIN_FEATURES);
	//Bind to physical memory
	int do_attach(MEM_HANDLE_T handle);
	//Detach from physical memory
	int do_detach(void);

    public:
	Feature();
	~Feature();

    private:
	BASE_INFO_T *_baseInfo;
	char errmsg_[256];
};

DTC_END_NAMESPACE

#endif
