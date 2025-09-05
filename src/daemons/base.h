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
#ifndef __H_WATCHDOG_DAEMON_H__
#define __H_WATCHDOG_DAEMON_H__

#include "daemons.h"

class WatchDogDaemon : public WatchDogObject,
					   private TimerObject
{
private:
	TimerList *timer_list_;

private:
	virtual void killed_notify(int signo, int coredumped);
	virtual void exited_notify(int retval);
	virtual void job_timer_procedure();

public:
	WatchDogDaemon(WatchDog *watchdog, int sec);
	~WatchDogDaemon();

	virtual int new_proc_fork();
	virtual void exec() = 0;
};

#endif
