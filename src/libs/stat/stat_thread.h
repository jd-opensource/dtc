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
#ifndef __H_STAT_THREAD___
#define __H_STAT_THREAD___

#include "stat_manager.h"

class StatThread : public StatManager {
    public:
	// public method
	StatThread(void);
	~StatThread(void);

    public:
	// background access
	int start_background_thread(void);
	int stop_background_thread(void);
	int init_stat_info(const char *name, const char *fn)
	{
		return StatManager::init_stat_info(name, fn, 0);
	}

    private:
	pthread_t thread_id_;
	pthread_mutex_t wake_lock_;

	static void *__thread_entry(void *);
	void thread_loop(void);
};

#endif
