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
#include <fcntl.h>
#include <errno.h>

#include "base.h"
#include "log/log.h"

WatchDogDaemon::WatchDogDaemon(WatchDog *watchdog, int sec)
	: WatchDogObject(watchdog)
{
	if (watchdog)
		timer_list_ = watchdog->get_timer_list(sec);
}

WatchDogDaemon::~WatchDogDaemon(void)
{
}

int WatchDogDaemon::new_proc_fork()
{
	/* an error detection pipe */
	// int err, fd[2];
	// fd[0] = 0;
	// fd[1] = 0;
	// int unused = 0;
	int err, fd[2];
	int unused;

	unused = pipe(fd);
	/* fork child process */
	watchdog_object_pid_ = fork();
	if (watchdog_object_pid_ == -1)
		return watchdog_object_pid_;
	if (watchdog_object_pid_ == 0) {
		// child process in.
		/* close pipe if exec succ */
		close(fd[0]);
		fcntl(fd[1], F_SETFD, FD_CLOEXEC);
		exec();
		err = errno;
		log4cplus_error("%s: exec(): %m", watchdog_object_name_);
		unused = write(fd[1], &err, sizeof(err));
		exit(-1);
	}

	// parent process in.
	close(fd[1]);
	if (read(fd[0], &err, sizeof(err)) == sizeof(err)) {
		errno = err;
		return -1;
	}
	close(fd[0]);
	attach_watch_dog();
	return watchdog_object_pid_;
}

void WatchDogDaemon::job_timer_procedure()
{
	log4cplus_debug("enter timer procedure");
	if (new_proc_fork() < 0)
		if (timer_list_)
			attach_timer(timer_list_);
	log4cplus_debug("leave timer procedure");
}

void WatchDogDaemon::killed_notify(int signo, int coredumped)
{
	if (timer_list_)
		attach_timer(timer_list_);
	report_kill_alarm(signo, coredumped);
}

void WatchDogDaemon::exited_notify(int retval)
{
	if (timer_list_)
		attach_timer(timer_list_);
}
