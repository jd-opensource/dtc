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
#ifndef __PLUGIN_TIMER_H__
#define __PLUGIN_TIMER_H__

#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <linux/unistd.h>

#include "../thread/thread.h"

class PluginTimer : public Thread {
    public:
	PluginTimer(const char *name, const int timeout);
	virtual ~PluginTimer();

    private:
	virtual void *do_process(void);
	virtual int initialize(void);
	virtual void interrupt(void);

    private:
	dll_func_t *_dll;
	const int _interval;
	pthread_mutex_t _wake_lock;
};

#endif
