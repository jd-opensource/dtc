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
#ifndef _RELATIVE_HOUR_CALCULATOR_H_
#define _RELATIVE_HOUR_CALCULATOR_H_

#include <string>
#include "algorithm/singleton.h"
#include <sys/time.h>

class RelativeHourCalculator {
    public:
	RelativeHourCalculator()
	{
	}
	~RelativeHourCalculator()
	{
	}
	/*Due to mktimehelper, need to subtract eight hours*/
	void set_base_hour(uint64_t ddwBaseYear)
	{
		uint64_t ddwRelativeTime =
			mktime_helper(ddwBaseYear, 1, 1, 0, 0, 0);
		m_BaseHour = (ddwRelativeTime / 3600 - 8);
	}
	uint64_t get_relative_hour()
	{
		uint64_t ddwCurHour = time(NULL) / 3600;

		return (ddwCurHour - m_BaseHour);
	}
	uint64_t get_base_hour()
	{
		return m_BaseHour;
	}

	uint64_t mktime_helper(uint64_t year, uint64_t mon, uint64_t day,
			       uint64_t hour, uint64_t min, uint64_t sec)
	{
		if (0 >= (int)(mon -= 2)) { /**/ /* 1..12 -> 11,12,1..10 */
			mon += 12;
				/**/ /* Puts Feb last since it has leap day */
			year -= 1;
		}

		return (((((unsigned long)(year / 4 - year / 100 + year / 400 +
					   367 * mon / 12 + day) +
			   year * 365 - 719499) *
				  24 +
			  hour /**/ /* now have hours */
			  ) * 60 +
			 min /**/ /* now have minutes */
			 ) * 60 +
			sec);
		/**/ /* finally seconds */
	}

    private:
	uint64_t m_BaseHour; /*ModuleId corresponding to this business*/
};
#define RELATIVE_HOUR_CALCULATOR Singleton<RelativeHourCalculator>::instance()
#endif
