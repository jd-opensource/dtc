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

extern int watchdog_stop;
extern int load_all;
extern int load_sharding;

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
	/* immediately register process exit handler to solve the issue of some processes not receiving signals when too many processes are created during startup linjinming 2014-06-14*/
	notifier = new WatchDogPipe;
	//signal(SIGCHLD, sighdlr);

	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = sighdlr;
	sigaction(SIGCHLD, &sa, NULL);
}

WatchDog::~WatchDog(void)
{
}

void close_sharding()
{
	char* p[1];
	p[0] = NULL;
	execv("../sharding/bin/stop.sh", p);

	int result = system("../sharding/bin/stop.sh");
	(void)result; // Silence unused result warning
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

	while (!watchdog_stop) {
		int timeout = expire_micro_seconds(1000, 1);
		//log4cplus_debug("befor poll, timeout:%d, %d, %d\n", timeout, pfd[0].fd, pfd[1].fd);
		int interrupted = poll(pfd, 2, timeout);
		//log4cplus_debug("after poll, watchdog_stop:%d, interrupted:%d\n", watchdog_stop, interrupted);
		update_now_time(timeout, interrupted);
		if (watchdog_stop)
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
	kill_allwatchdog();
	if(load_sharding || load_all)
		close_sharding();
	//force_kill_allwatchdog();
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
