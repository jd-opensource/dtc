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
* 
*/
#ifndef __STAT_MGR_H
#define __STAT_MGR_H

#include "stat_manager.h"

class StatClient : public StatManager {
    public:
	int init_stat_info(const char *name, const char *index_file);

	typedef StatInfo *Iterator_;
	int check_point(void);
	inline Iterator_ get_begin_stat_info(void)
	{
		return stat_info_;
	}
	inline Iterator_ get_end_stat_info(void)
	{
		return stat_info_ + stat_num_info_;
	}

	int64_t read_counter_value(Iterator_ stat_info, unsigned int cat);
	int64_t read_sample_counter(Iterator_ stat_info, unsigned int cat,
				    unsigned int count = 0);
	int64_t read_sample_average(Iterator_ stat_info, unsigned int cat);
	Iterator_ operator[](unsigned int id)
	{
		return id_map_[id];
	}

    public:
	// client/tools access
	StatClient();
	~StatClient();

    private:
	int last_serial_number_;
};

#endif
