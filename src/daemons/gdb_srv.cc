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
#include <stdlib.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>

#include "gdb.h"

static volatile int g_stop;
static void sigusr1(int signo) {}
static void sigstop(int signo) { g_stop = 1; }

void gdb_server(int debug, const char *display)
{
	signal(SIGINT, sigstop);
	signal(SIGTERM, sigstop);
	signal(SIGQUIT, sigstop);
	signal(SIGHUP, sigstop);
	signal(SIGPIPE, SIG_IGN);
	signal(SIGCHLD, SIG_DFL);
	signal(SIGWINCH, sigusr1);
	signal(SIGPIPE, SIG_IGN);
	signal(SIGCHLD, SIG_IGN);

	sigset_t sset;
	sigemptyset(&sset);
	sigaddset(&sset, SIGWINCH);
	sigprocmask(SIG_BLOCK, &sset, NULL);

	const struct timespec timeout = {5, 0};
	siginfo_t info;
	while (!g_stop) {
		if (getppid() == 1)
			break;
		int signo = sigtimedwait(&sset, &info, &timeout);
		if (signo == -1 && errno == -EAGAIN)
			continue;
		if (signo <= 0) {
			usleep(100 * 1000);
			continue;
		}
		if (info.si_code != SI_QUEUE)
			continue;

		int tid = info.si_int;
		if (debug == 0) {
			gdb_dump(tid);
		} else {
			gdb_attach(tid, display);
		}
	}
}
