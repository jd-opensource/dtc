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
#ifndef __H_WATCHDOG__H__
#define __H_WATCHDOG__H__

#include "unit.h"
#include "poll/poller.h"
#include "timer/timer_list.h"

extern char daemons_cache_file[256];
extern char daemons_table_file[256];

class WatchDogPipe : public EpollBase
{
private:
	int peerfd_;

public:
	WatchDogPipe();
	virtual ~WatchDogPipe();

public:
	void wake();
	virtual void input_notify();
};

class WatchDog : public WatchDogUnit,
				 public TimerUnit
{
private:
	EpollBase *listener_;

public:
	WatchDog();
	virtual ~WatchDog();

	void set_listener(EpollBase *listener) { listener_ = listener; }
	void run_loop();
};

extern int start_dtc(int (*entry)(void *), void *);
#endif
