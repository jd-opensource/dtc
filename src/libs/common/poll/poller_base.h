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
#ifndef __POLLTHREAD_H__
#define __POLLTHREAD_H__

#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <linux/unistd.h>
#include <map>

#include "algorithm/timestamp.h"
#include "poller.h"
#include "timer/timer_list.h"
#include "thread/thread.h"

class PollerBase : public Thread,
		   public EpollOperation,
		   public TimerUnit,
		   public ReadyUnit {
    public:
	PollerBase(const char *name);
	virtual ~PollerBase();

    protected:
	int poll_timeout;

	// IMPORTANT! largest loop in project.
	virtual void *do_process(void);

	virtual int initialize(void);
};

#endif
