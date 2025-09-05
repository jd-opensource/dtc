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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <libgen.h>
#include <sys/resource.h>

#include "../config/config.h"
#include "../log/log.h"
#include "daemon/daemon.h"
#include "mem_check.h"
#include <stat_dtc.h>
#include "thread/thread_cpu_stat.h"

void daemon_wait(void)
{
	StatCounter statfd =
		g_stat_mgr.get_stat_int_counter(SERVER_OPENNING_FD);
	statfd = 0;

	unsigned fdthreshold, fdlimit = daemon_get_fd_limit();
	if (fdlimit < 0)
		fdlimit = 1024;
	fdthreshold = (fdlimit * 80) / 100;

	sleep(10);
	thread_cpu_stat cpu_stat;
	cpu_stat.init();
	while (!stop) {
		sleep(10);
		if (stop)
			break;

		cpu_stat.do_stat();

		/* Scan process open fd handle count, if exceeds configured threshold, alert to secondary network management */
		statfd = scan_process_openning_fd();
		if ((unsigned)statfd > fdthreshold) {
			log4cplus_fatal(
				"openning fd overload[current=%d threshold=%d max=%d]",
				(unsigned)statfd, fdthreshold, fdlimit);
		}
#if MEMCHECK
		log4cplus_debug("memory allocated %lu virtual %lu",
				count_alloc_size(), count_virtual_size());
#endif
	}
}

/* Scan number of file handles opened by process */
unsigned int scan_process_openning_fd(void)
{
	unsigned int count = 0;
	char fd_dir[1024] = { 0 };
	DIR *dir;
	struct dirent *ptr;

	snprintf(fd_dir, sizeof(fd_dir), "/proc/%d/fd", getpid());

	if ((dir = opendir(fd_dir)) == NULL) {
		log4cplus_warning("open dir :%s failed, msg:%s", fd_dir,
				  strerror(errno));
		return count;
	}

	while ((ptr = readdir(dir)) != NULL) {
		if (strcasecmp(ptr->d_name, "..") != 0 &&
		    strcasecmp(ptr->d_name, ".") != 0)
			count++;
	}

	closedir(dir);
	return count;
}
