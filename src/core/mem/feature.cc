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

#include <stdio.h>
#include <string.h>
#include "algorithm/singleton.h"
#include "feature.h"
#include "global.h"

DTC_USING_NAMESPACE

Feature *Feature::instance()
{
	return Singleton<Feature>::instance();
}

void Feature::destroy()
{
	return Singleton<Feature>::destory();
}

Feature::Feature() : _baseInfo(NULL)
{
	memset(errmsg_, 0, sizeof(errmsg_));
}

Feature::~Feature()
{
}
/* feature id -> feature. Copy input feature to found feature
 */
int Feature::modify_feature(FEATURE_INFO_T *fi)
{
	if (!fi)
		return -1;

	FEATURE_INFO_T *p = get_feature_by_id(fi->fi_id);
	if (!p) {
		snprintf(errmsg_, sizeof(errmsg_), "not found feature[%d]",
			 fi->fi_id);
		return -2;
	}

	*p = *fi;
	return 0;
}
/* feature id -> feature. Clear this feature 
 */
int Feature::delete_feature(FEATURE_INFO_T *fi)
{
	if (!fi)
		return -1;

	FEATURE_INFO_T *p = get_feature_by_id(fi->fi_id);
	if (!p) {
		snprintf(errmsg_, sizeof(errmsg_), "not found feature[%d]",
			 fi->fi_id);
		return -2;
	}

	//delete feature
	p->fi_id = 0;
	p->fi_attr = 0;
	p->fi_handle = INVALID_HANDLE;

	return 0;
}
/* Find a free feature, assign value 
 */
int Feature::add_feature(const uint32_t id, const MEM_HANDLE_T v,
			 const uint32_t attr)
{
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_), "handle is invalid");
		return -1;
	}

	//find freespace
	FEATURE_INFO_T *p = get_feature_by_id(0);
	if (!p) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "have no free space to add a new feature");
		return -2;
	}

	p->fi_id = id;
	p->fi_attr = attr;
	p->fi_handle = v;

	return 0;
}
/* feature id -> feature. 
 * 1. feature id == 0: means find a free feature.
 * 2. Otherwise find corresponding feature based on feature id
 */
FEATURE_INFO_T *Feature::get_feature_by_id(const uint32_t fd)
{
	if (!_baseInfo || _baseInfo->bi_total == 0) {
		goto EXIT;
	}

	for (uint32_t i = 0; i < _baseInfo->bi_total; i++) {
		if (_baseInfo->bi_features[i].fi_id == fd) {
			return (&(_baseInfo->bi_features[i]));
		}
	}

EXIT:
	return (FEATURE_INFO_T *)(0);
}
/* 1. Create num empty features
 * 2. Initialize header information (baseInfo)
 */
int Feature::do_init(const uint32_t num)
{
	size_t size = sizeof(FEATURE_INFO_T);
	size *= num;
	size += sizeof(BASE_INFO_T);

	MEM_HANDLE_T v = M_CALLOC(size);
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_), "init features failed, %s",
			 M_ERROR());
		return -1;
	}

	_baseInfo = M_POINTER(BASE_INFO_T, v);
	_baseInfo->bi_total = num;

	return 0;
}
/* Feature already exists, memory handle of first feature. Directly initialize header information pointer 
 */
int Feature::do_attach(MEM_HANDLE_T handle)
{
	if (INVALID_HANDLE == handle) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "attach features failed, memory handle=0");
		return -1;
	}

	_baseInfo = M_POINTER(BASE_INFO_T, handle);
	return 0;
}

int Feature::do_detach(void)
{
	_baseInfo = NULL;
	return 0;
}
