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
#include <signal.h>
#include <string.h>
#include "main_entry.h"
#include "daemon/daemon.h"
#include "log/log.h"
#include "proc_title.h"


/**
  * recovery_ value:
  *    0 -- don't refork
  *    1 -- fork again if crashed
  *    2 -- fork & enable core dump if crashed
  *    3 -- fork if killed
  *    4 -- fork again if exit != 0
  *    5 -- always fork again
  */
static const char *recovery_Info_str[] = {
	"None", "Crash", "CrashDebug", "Killed", "Error", "Always"};


MainEntry::MainEntry(WatchDogUnit *watchdog, int (*e)(void *), void *args, int recovery)
	: WatchDogObject(watchdog), entry(e), args_(args), recovery_(recovery), core_count_(0)
{
	strncpy(watchdog_object_name_, "main", sizeof(watchdog_object_name_));
	if (recovery_ > 3) {
		char comm[24] = "wdog-";
		get_proc_name(comm + 5);
		set_proc_name(comm);
	}
	log4cplus_info("Main Process Recovery Mode: %s", recovery_Info_str[recovery_]);
}

MainEntry::~MainEntry(void)
{
}

int MainEntry::fork_main(int enCoreDump)
{
	if ((watchdog_object_pid_ = fork()) == 0) {
		//child process in.
		if (enCoreDump)
			init_core_dump();
		exit(entry(args_));
	}

    /* cann't fork main process */
	if (watchdog_object_pid_ < 0)
		return -1; 

	//parent process in.
	attach_watch_dog();
	return watchdog_object_pid_;
}

void MainEntry::killed_notify(int signo, int coredumped)
{
	int corenable = 0;

	if (recovery_ == 0 || ((recovery_ <= 2 && signo == SIGKILL) || signo == SIGTERM))
		/* main cache exited, stopping all children */
		stop = 1;
	else {
		++core_count_;
		if (core_count_ == 1 && recovery_ >= 2)
			corenable = 1;
		sleep(2);
		if (fork_main(corenable) < 0)
			/* main fork error, stopping all children */
			stop = 1;
	}
	report_kill_alarm(signo, coredumped);
}

void MainEntry::exited_notify(int retval)
{
	if (recovery_ < 4 || retval == 0)
		/* main cache exited, stopping all children */
		stop = 1;
	else {
		/* sync sleep */
		sleep(2); 
		if (fork_main() < 0)
			/* main fork error, stopping all children */
			stop = 1;
	}
}
