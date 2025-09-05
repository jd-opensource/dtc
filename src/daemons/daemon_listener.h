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
#ifndef __H_WATCHDOG_LISTENER_H__
#define __H_WATCHDOG_LISTENER_H__

#include "daemons.h"

#define ENV_WATCHDOG_SOCKET_FD "WATCHDOG_SOCKET_FD"

#define WATCHDOG_INPUT_OBJECT 0
#define WATCHDOG_INPUT_HELPER 1

#define DBHELPER_TABLE_ORIGIN 0
#define DBHELPER_TABLE_NEW 1

struct StartHelperPara
{
	uint8_t type;
	uint8_t mach;
	uint8_t role;
	uint8_t conf;
	uint16_t num;
	uint16_t backlog;
};

class WatchDogListener : public EpollBase
{
private:
	WatchDog *owner_;
	int peerfd_;
	int delay_;

public:
	WatchDogListener(WatchDog *watchdog, int sec);
	virtual ~WatchDogListener(void);
	int attach_watch_dog();
	virtual void input_notify();
};

extern int watch_dog_fork(const char *name, int (*entry)(void *), void *args);
#endif
