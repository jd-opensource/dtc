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
#ifndef __H_WATCHDOG_UNIT_H__
#define __H_WATCHDOG_UNIT_H__
#include <stdlib.h>
#include <string.h>
#include <map>

class WatchDogUnit;

class WatchDogObject
{
protected:
	friend class WatchDogUnit;
	WatchDogUnit *watchdog_object_owner_;
	int watchdog_object_pid_;
	char watchdog_object_name_[16];

public:
	WatchDogObject(void) : watchdog_object_owner_(NULL), watchdog_object_pid_(0) { watchdog_object_name_[0] = '0'; }
	WatchDogObject(WatchDogUnit *u) : watchdog_object_owner_(u), watchdog_object_pid_(0) { watchdog_object_name_[0] = '0'; }
	WatchDogObject(WatchDogUnit *u, const char *n) : watchdog_object_owner_(u), watchdog_object_pid_(0)
	{
		strncpy(watchdog_object_name_, n, sizeof(watchdog_object_name_));
	}
	WatchDogObject(WatchDogUnit *u, const char *n, int i) : watchdog_object_owner_(u), watchdog_object_pid_(i)
	{
		strncpy(watchdog_object_name_, n, sizeof(watchdog_object_name_));
	}
	virtual ~WatchDogObject();
	virtual void exited_notify(int retval);
	virtual void killed_notify(int signo, int coredumped);
	const char *Name() const { return watchdog_object_name_; }
	int get_watchdog_pid() const { return watchdog_object_pid_; }
	int attach_watch_dog(WatchDogUnit *o = 0);
	void report_kill_alarm(int signo, int coredumped);
};



class WatchDogUnit
{
private:
	friend class WatchDogObject;
	typedef std::map<int, WatchDogObject *> pidmap_t;
	int pid_count_;
	pidmap_t pid_map_watchdog_object_;

private:
	int attach_process(WatchDogObject *);

public:
	WatchDogUnit();
	virtual ~WatchDogUnit();
	/* return #pid monitored */
	int check_watchdog(); 
	int kill_allwatchdog();
	int force_kill_allwatchdog();
	int get_process_count() const { return pid_count_; }
};

#endif
