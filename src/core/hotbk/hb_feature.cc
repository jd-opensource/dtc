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
#include <errno.h>
#include "hb_feature.h"
#include "global.h"

DTC_USING_NAMESPACE

HBFeature::HBFeature() : hb_info_(NULL), handle_(INVALID_HANDLE)
{
	memset(errmsg_, 0, sizeof(errmsg_));
}

HBFeature::~HBFeature()
{
}

int HBFeature::init(time_t tMasterUptime)
{
	handle_ = M_CALLOC(sizeof(HB_FEATURE_INFO_T));
	if (INVALID_HANDLE == handle_) {
		snprintf(errmsg_, sizeof(errmsg_), "init hb_feature fail, %s",
			 M_ERROR());
		return -ENOMEM;
	}

	hb_info_ = M_POINTER(HB_FEATURE_INFO_T, handle_);
	hb_info_->master_up_time = tMasterUptime;
	hb_info_->slave_up_time = 0;

	return DTC_CODE_SUCCESS;
}

int HBFeature::attach(MEM_HANDLE_T handle)
{
	if (INVALID_HANDLE == handle) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "attach hb feature failed, memory handle = 0");
		return DTC_CODE_FAILED;
	}

	handle_ = handle;
	hb_info_ = M_POINTER(HB_FEATURE_INFO_T, handle_);

	return DTC_CODE_SUCCESS;
}

void HBFeature::detach(void)
{
	hb_info_ = NULL;
	handle_ = INVALID_HANDLE;
}
