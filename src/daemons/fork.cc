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
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <sched.h>
#include <string.h>
#include <signal.h>

#ifndef CLONE_PARENT
#define CLONE_PARENT 0x00008000
#endif

#include "daemon_listener.h"
#include "log/log.h"

struct ForkInfo
{
	char name[16];
	int fd;
	int (*entry)(void *);
	void *args;
};

static int CloneEntry(void *arg)
{
	struct ForkInfo *info = (ForkInfo *)arg;
	send(info->fd, info->name, sizeof(info->name), 0);
	exit(info->entry(info->args));
}

int watch_dog_fork(const char *name, int (*entry)(void *), void *args)
{
	/* 4K stack is enough for CloneEntry */
	char stack[4096]; 
	struct ForkInfo info;
	char *env = getenv(ENV_WATCHDOG_SOCKET_FD);

	info.fd = env == NULL ? -1 : atoi(env);
	log4cplus_debug("daemons fork, fd: %d, name: %s, args: %p", info.fd, name, args);
	if (info.fd <= 0) {
		int pid = fork();
		if (pid == 0) 
			exit(entry(args));
		return pid;
	}

	strncpy(info.name, name, sizeof(info.name));
	info.entry = entry;
	info.args = args;

	return clone(
		/* entry */
		CloneEntry, 
		stack + sizeof(stack) - 16,
		/* flag */
		CLONE_PARENT | SIGCHLD,
		/* data */ 
		&info					
	);
};
