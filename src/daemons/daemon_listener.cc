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
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

#include "daemon_listener.h"
#include "helper.h"
#include "config/dbconfig.h"
#include "log/log.h"

WatchDogListener::WatchDogListener(WatchDog *watchdog, int sec) : owner_(watchdog), peerfd_(-1), delay_(sec){};

WatchDogListener::~WatchDogListener()
{
	if (peerfd_ > 0) {
		log4cplus_debug("daemons listener fd: %d closed", peerfd_);
		close(peerfd_);
	}
};

/* Create socket */
int WatchDogListener::attach_watch_dog()
{
	int fd[2];
	socketpair(AF_UNIX, SOCK_DGRAM, 0, fd);
	netfd = fd[0];
	peerfd_ = fd[1];

	char buf[30];
	snprintf(buf, sizeof(buf), "%d", peerfd_);
	setenv(ENV_WATCHDOG_SOCKET_FD, buf, 1);

	int no;
	setsockopt(netfd, SOL_SOCKET, SO_PASSCRED, (no = 1, &no), sizeof(int));
	fcntl(netfd, F_SETFD, FD_CLOEXEC);
	enable_input();
	owner_->set_listener(this);
	return 0;
}

void WatchDogListener::input_notify()
{
	char buf[16];
	int n;
	struct msghdr msg = {0};
	char ucbuf[CMSG_SPACE(sizeof(struct ucred))];
	struct iovec iov[1];

	iov[0].iov_base = (void *)&buf;
	iov[0].iov_len = sizeof(buf);
	msg.msg_control = ucbuf;
	msg.msg_controllen = sizeof ucbuf;
	msg.msg_iov = iov;
	msg.msg_iovlen = 1;
	msg.msg_flags = 0;

	while ((n = recvmsg(netfd, &msg, MSG_TRUNC | MSG_DONTWAIT)) > 0) {
		struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
		struct ucred *uc;
		if (msg.msg_controllen < sizeof(ucbuf) ||
			cmsg->cmsg_level != SOL_SOCKET ||
			cmsg->cmsg_type != SCM_CREDENTIALS ||
			cmsg->cmsg_len != CMSG_LEN(sizeof(struct ucred)) ||
			msg.msg_controllen < cmsg->cmsg_len)
			continue;
		uc = (struct ucred *)CMSG_DATA(cmsg);
		if (n != sizeof(buf))
			continue;

		log4cplus_debug("new daemons object: %p, %d, %d, %s", owner_, buf[0], uc->pid, buf + 1);
		WatchDogObject *obj = NULL;
		if (buf[0] == WATCHDOG_INPUT_OBJECT) {
			obj = new WatchDogObject(owner_, buf, uc->pid);
			if (obj == NULL) {
				log4cplus_error("new WatchDogObject error");
				return;
			}
		} else if (buf[0] == WATCHDOG_INPUT_HELPER) {
			StartHelperPara *para = (StartHelperPara *)(buf + 1);
			char path[32];
			if (!DbConfig::build_path(path, 32, getpid(), para->mach, para->role, para->type)) {
				log4cplus_error("build helper listen path error");
				return;
			}
			NEW(WatchDogHelper(owner_, delay_, path, para->mach, para->role, para->backlog, para->type, para->conf, para->num), obj);
			if (obj == NULL) {
				log4cplus_error("new daemons helper error");
				return;
			}
			WatchDogHelper *helper = (WatchDogHelper *)obj;
			if (helper->new_proc_fork() < 0 || helper->verify() < 0) {
				log4cplus_error("fork helper error");
				return;
			}
		} else {
			log4cplus_error("unknown daemons input type: %d, %s", buf[0], buf + 1);
			return;
		}
		obj->attach_watch_dog();
	}
}
