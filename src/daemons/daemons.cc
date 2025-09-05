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
#include <fcntl.h>
#include <sys/poll.h>

#include "daemons.h"
#include "daemon/daemon.h"
#include "log/log.h"
#include "dtc_global.h"

char daemons_cache_file[256] = CACHE_CONF_NAME;
char daemons_table_file[256] = TABLE_CONF_NAME;


WatchDogPipe::WatchDogPipe()
{
	int fd[2];
	int unused;

	unused = pipe(fd);
	netfd = fd[0];
	peerfd_ = fd[1];
	fcntl(netfd, F_SETFL, O_NONBLOCK);
	fcntl(peerfd_, F_SETFL, O_NONBLOCK);
	fcntl(netfd, F_SETFD, FD_CLOEXEC);
	fcntl(peerfd_, F_SETFD, FD_CLOEXEC);
	enable_input();
}

WatchDogPipe::~WatchDogPipe()
{
	if (peerfd_ >= 0)
		close(peerfd_);
}

void WatchDogPipe::input_notify()
{
	char buf[100];
	while (read(netfd, buf, sizeof(buf)) == sizeof(buf))
		;
}

void WatchDogPipe::wake()
{
	char c = 0;
	c = write(peerfd_, &c, 1);
}

static WatchDogPipe *notifier;
static void sighdlr(int signo) 
{ 
	notifier->wake(); 
}

WatchDog::WatchDog()
{
	/* Immediately register process exit handler to resolve the issue where too many processes created during startup cause some processes to exit without receiving signals linjinming 2014-06-14*/
	notifier = new WatchDogPipe;
	signal(SIGCHLD, sighdlr);
}

WatchDog::~WatchDog(void)
{
}

void WatchDog::run_loop()
{
	struct pollfd pfd[2];
	notifier->init_poll_fd(&pfd[0]);

	if (listener_) {
		listener_->init_poll_fd(&pfd[1]);
	} else {
		pfd[1].fd = -1;
		pfd[1].events = 0;
		pfd[1].revents = 0;
	}

	while (!stop) {
		int timeout = expire_micro_seconds(3600 * 1000, 1);
		int interrupted = poll(pfd, 2, timeout);
		update_now_time(timeout, interrupted);
		if (stop)
			break;

		if (pfd[0].revents & POLLIN)
			notifier->input_notify();
		if (pfd[1].revents & POLLIN)
			listener_->input_notify();

		check_watchdog();
		check_expired();
		check_ready();
	}
	log4cplus_debug("prepare stopping");
	//kill_allwatchdog();
	force_kill_allwatchdog();
	check_watchdog();
	time_t stoptimer = time(NULL) + 5;
	int stopretry = 0;
	while (stopretry < 6 && get_process_count() > 0) {
		time_t now = time(NULL);
		if (stoptimer <= now) {
			stopretry++;
			stoptimer = now + 5;
			log4cplus_debug("notify all children again");
			kill_allwatchdog();
		}
		poll(pfd, 1, 1000);
		if (pfd[0].revents & POLLIN)
			notifier->input_notify();
		check_watchdog();
	}
	delete notifier;
	force_kill_allwatchdog();
	log4cplus_info("all children stopped, daemons ended");
	exit(0);
}
