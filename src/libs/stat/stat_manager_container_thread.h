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
#ifndef __H_STAT_MANAGER_CONTAINER_THREAD___
#define __H_STAT_MANAGER_CONTAINER_THREAD___

#include "stat_manager.h"
#include "lock/lock.h"

class StatManagerContainerThread : protected StatLock {
    public:
	// public method
	StatManagerContainerThread(void);
	~StatManagerContainerThread(void);
	static StatManagerContainerThread *getInstance();

    public:
	// background access
	int start_background_thread(void);
	int stop_background_thread(void);
	int add_stat_manager(uint32_t moudleId, StatManager *stat);
	int delete_stat_manager(uint32_t moudleId);
	StatCounter operator()(int moduleId, int cId);

	//int init_stat_info(const char *name, const char *fn) { return StatManager::init_stat_info(name, fn, 0); }
    private:
	pthread_t thread_id_;
	pthread_mutex_t wake_lock_;
	std::map<uint32_t, StatManager *> stat_Managers_map_;
	Mutex stat_manager_container_thread_mutex_lock_;

	static void *enter_thread(void *);
	void loop_thread(void);
};

#endif
