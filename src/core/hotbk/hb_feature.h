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

#ifndef __DTC_HB_FEATURE_H
#define __DTC_HB_FEATURE_H

#include <stdio.h>
#include <stdint.h>
#include <time.h>

#include "namespace.h"
#include "algorithm/singleton.h"
#include "global.h"

struct hb_feature_info {
	int64_t master_up_time;
	int64_t slave_up_time;
};
typedef struct hb_feature_info HB_FEATURE_INFO_T;

class HBFeature {
    public:
	HBFeature();
	~HBFeature();

	static HBFeature *instance()
	{
		return Singleton<HBFeature>::instance();
	}
	static void destory()
	{
		Singleton<HBFeature>::destory();
	}

	int init(time_t tMasterUptime);
	int attach(MEM_HANDLE_T handle);
	void detach(void);

	const char *error() const
	{
		return errmsg_;
	}

	MEM_HANDLE_T get_handle() const
	{
		return handle_;
	}

	int64_t &master_uptime()
	{
		return hb_info_->master_up_time;
	}
	int64_t &slave_uptime()
	{
		return hb_info_->slave_up_time;
	}

    private:
	HB_FEATURE_INFO_T *hb_info_;
	MEM_HANDLE_T handle_;
	char errmsg_[256];
};

#endif
