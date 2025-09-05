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

#ifndef __DTC_BLACKLIST_UNIT_H
#define __DTC_BLACKLIST_UNIT_H

#include "namespace.h"
#include "timer/timer_list.h"
#include "blacklist.h"
#include "log/log.h"
#include "stat_dtc.h"

DTC_BEGIN_NAMESPACE

class TimerObject;
class BlackListUnit : public BlackList, private TimerObject {
    public:
	BlackListUnit(TimerList *timer);
	virtual ~BlackListUnit(void);

	int init_blacklist(const unsigned max, const unsigned type,
			   const unsigned expired = 10 * 60 /*10 mins*/)
	{
		/* init statisitc item */
		stat_blacksize = g_stat_mgr.get_sample(BLACKLIST_SIZE);
		stat_blslot_count =
			g_stat_mgr.get_stat_int_counter(BLACKLIST_CURRENT_SLOT);

		return BlackList::init_blacklist(max, type, expired);
	}

	int add_blacklist(const char *key, const unsigned vsize)
	{
		int ret = BlackList::add_blacklist(key, vsize);
		if (0 == ret) {
			/* statistic */
			stat_blacksize.push(vsize);
			stat_blslot_count = current_blslot_count;
		}

		return ret;
	}

	int try_expired_blacklist(void)
	{
		int ret = BlackList::try_expired_blacklist();
		if (0 == ret) {
			/* statistic */
			stat_blslot_count = current_blslot_count;
		}

		return ret;
	}

	void start_blacklist_expired_task(void)
	{
		log4cplus_info("start blacklist-expired job");

		attach_timer(timer);

		return;
	}

    public:
	virtual void job_timer_procedure(void);

    private:
	TimerList *timer;

	/* for statistic */
	StatSample stat_blacksize; /* black size distribution */
	StatCounter stat_blslot_count;
};

DTC_END_NAMESPACE

#endif
