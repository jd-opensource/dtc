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
*/
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>

#include "system_lock.h"

int unix_socket_lock(const char *format, ...)
{
	int lockfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (lockfd >= 0) {
		va_list ap;
		struct sockaddr_un addr;
		int len;

		addr.sun_family = AF_UNIX;

		va_start(ap, format);
		vsnprintf(addr.sun_path + 1, sizeof(addr.sun_path) - 1, format,
			  ap);
		va_end(ap);

		addr.sun_path[0] = '@';
		len = SUN_LEN(&addr);
		addr.sun_path[0] = '\0';
		if (bind(lockfd, (struct sockaddr *)&addr, len) == 0)
			fcntl(lockfd, F_SETFD, FD_CLOEXEC);
		else {
			close(lockfd);
			lockfd = -1;
		}
	}
	return lockfd;
}
