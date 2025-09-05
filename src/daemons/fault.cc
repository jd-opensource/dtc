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
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <version.h>
#include <asm/unistd.h>

#include "compiler.h"
#include "fault.h"

__HIDDEN
int FaultHandler::dogpid_ = 0;

__HIDDEN
FaultHandler FaultHandler::instance_;

extern "C"
	__attribute__((__weak__)) void
	crash_hook(int signo);

extern "C"
{
	__EXPORT volatile int g_crash_continue;
};

static int g_crash_hook = 0;

__HIDDEN
void FaultHandler::handler(int signo, siginfo_t *dummy_first, void *dummy_second)
{
	signal(signo, SIG_DFL);
	if (dogpid_ > 1) {
		sigval tid;
		tid.sival_int = syscall(__NR_gettid);
		sigqueue(dogpid_, SIGWINCH, tid);
		for (int i = 0; i < 50 && !g_crash_continue; i++)
			usleep(100 * 1000);
		if ((g_crash_hook != 0) && (&crash_hook != 0))
			crash_hook(signo);
	}
}

__HIDDEN
FaultHandler::FaultHandler(void)
{
	initialize(1);
}

__HIDDEN
void FaultHandler::initialize(int protect)
{
	char *p = getenv(ENV_FAULT_LOGGER_PID);
	if (p && (dogpid_ = atoi(p)) > 1) {
		struct sigaction sa;
		memset(&sa, 0, sizeof(sa));
		sa.sa_sigaction = FaultHandler::handler;
		sa.sa_flags = SA_RESTART | SA_SIGINFO;
		sigaction(SIGSEGV, &sa, NULL);
		sigaction(SIGBUS, &sa, NULL);
		sigaction(SIGILL, &sa, NULL);
		sigaction(SIGABRT, &sa, NULL);
		sigaction(SIGFPE, &sa, NULL);
		if (protect)
			g_crash_hook = 1;
	}
}

#if __pic__ || __PIC__
extern "C" const char __invoke_dynamic_linker__[]
	__attribute__((section(".interp")))
	__HIDDEN =
#if __x86_64__
		"/lib64/ld-linux-x86-64.so.2"
#else
		"/lib/ld-linux.so.2"
#endif
	;

extern "C" __HIDDEN int _so_start(void)
{
#define BANNER "DTC FaultLogger v" DTC_VERSION_DETAIL "\n" \
			   "  - USED BY DTCD INTERNAL ONLY!!!\n"
	int unused;
	unused = write(1, BANNER, sizeof(BANNER) - 1);
	_exit(0);
}
#endif
