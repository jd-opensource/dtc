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
#ifndef __H_DTC_ENTRY_H__
#define __H_DTC_ENTRY_H__

#include "daemons.h"

class MainEntry : private WatchDogObject
{
private:
	int (*entry)(void *);
	void *args_;
	int recovery_;
	int core_count_;

private:
	virtual void killed_notify(int signo, int coredumped);
	virtual void exited_notify(int retval);

public:
	MainEntry(WatchDogUnit *watchdog, int (*entry)(void *), void *args_, int recovery);  
	virtual ~MainEntry();
	
	int fork_main(int enCoreDump = 0);
};

#endif
