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
#include <sys/wait.h>
#include <errno.h>

#include "config/config.h"
#include "unit.h"
#include "log/log.h"
#include "stat_alarm_reporter.h"
#include <sstream>
#include <dtc_global.h>

WatchDogObject::~WatchDogObject()
{
}

int WatchDogObject::attach_watch_dog(WatchDogUnit *u)
{
	if (u && watchdog_object_owner_ == NULL)
		watchdog_object_owner_ = u;
	return watchdog_object_owner_ ? watchdog_object_owner_->attach_process(this) : -1;
}

void WatchDogObject::exited_notify(int retval)
{
	delete this;
}

void WatchDogObject::killed_notify(int signo, int coredumped)
{
	delete this;
}

WatchDogUnit::WatchDogUnit()
	: pid_count_(0){};

WatchDogUnit::~WatchDogUnit()
{
	pidmap_t::iterator i;
	for (i = pid_map_watchdog_object_.begin(); i != pid_map_watchdog_object_.end(); i++) {
		WatchDogObject *obj = i->second;
		delete obj;
	}
};

int WatchDogUnit::check_watchdog()
{
	while (1) {
		int status;
		int pid = waitpid(-1, &status, WNOHANG);
		int err = errno;
		if (pid < 0) {
			switch (err) {
			case EINTR:
			case ECHILD:
				break;
			default:
				log4cplus_info("wait() return pid %d errno %d", pid, err);
				break;
			}
			break;
		} else if (pid == 0) {
			break;
		} else {
			pidmap_t::iterator itr = pid_map_watchdog_object_.find(pid);
			if (itr == pid_map_watchdog_object_.end()) {
				log4cplus_info("wait() return unknown pid %d status 0x%x", pid, status);
			} else {
				WatchDogObject *obj = itr->second;
				const char *const name = obj->Name();
				pid_map_watchdog_object_.erase(itr);
				/* special exit value return-ed by CrashProtector */
				if (WIFEXITED(status) && WEXITSTATUS(status) == 85 && strncmp(name, "main", 5) == 0) {   
					/* treat it as a fake SIGSEGV */
					status = W_STOPCODE(SIGSEGV);
				}
				if (WIFSIGNALED(status)) {
					const int sig = WTERMSIG(status);
					const int core = WCOREDUMP(status);
					log4cplus_fatal("%s: killed by signal %d", name, sig);
					log4cplus_error("child %.16s pid %d killed by signal %d%s",
							  name, pid, sig,
							  core ? " (core dumped)" : "");
					pid_count_--;
					obj->killed_notify(sig, core);
				} else if (WIFEXITED(status)) {
					const int retval = (signed char)WEXITSTATUS(status);
					if (retval == 0)
						log4cplus_debug("child %.16s pid %d exit status %d",
								  name, pid, retval);
					else
						log4cplus_info("child %.16s pid %d exit status %d",
								 name, pid, retval);
					pid_count_--;
					obj->exited_notify(retval);
				}
			}
		}
	}
	return pid_count_;
}

int WatchDogUnit::attach_process(WatchDogObject *obj)
{
	const int pid = obj->watchdog_object_pid_;
	pidmap_t::iterator itr = pid_map_watchdog_object_.find(pid);
	if (itr != pid_map_watchdog_object_.end() || pid <= 1)
		return -1;
	pid_map_watchdog_object_[pid] = obj;
	pid_count_++;
	return 0;
}

int WatchDogUnit::kill_allwatchdog()
{
	pidmap_t::iterator i;
	int n = 0;
	for (i = pid_map_watchdog_object_.begin(); i != pid_map_watchdog_object_.end(); i++) {
		WatchDogObject *obj = i->second;
		if (obj->get_watchdog_pid() > 1) {
			log4cplus_debug("killing child %.16s pid %d SIGTERM",
					  obj->Name(), obj->get_watchdog_pid());
			kill(obj->get_watchdog_pid(), SIGTERM);
			pid_count_--;
			n++;
		}
	}
	return n;
}

int WatchDogUnit::force_kill_allwatchdog()
{
	pidmap_t::iterator i;
	int n = 0;
	for (i = pid_map_watchdog_object_.begin(); i != pid_map_watchdog_object_.end(); i++) {
		WatchDogObject *obj = i->second;
		if (obj->get_watchdog_pid() > 1) {
			log4cplus_error("child %.16s pid %d didn't exit in timely, sending SIGKILL.",
					  obj->Name(), obj->get_watchdog_pid());
			kill(obj->get_watchdog_pid(), SIGKILL);
			n++;
		}
	}
	return n;
}
