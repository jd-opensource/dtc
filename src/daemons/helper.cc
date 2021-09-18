/*
* Copyright [2021] JD.com, Inc.
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
#include <sys/un.h>
#include "socket/unix_socket.h"

#include "config/dbconfig.h"
#include "thread/thread.h"
#include "helper.h"
#include "daemon/daemon.h"
#include "log/log.h"
#include "dtc_global.h"
#include <sstream>

WatchDogHelper::WatchDogHelper(WatchDog *watchdog, int sec, const char *path,
			       int machine_conf, int role, int backlog,
			       int type, int conf, int num)
	: WatchDogDaemon(watchdog, sec)
{
	std::stringstream oss;
	oss << "helper" << machine_conf << MACHINEROLESTRING[role];
	memcpy(watchdog_object_name_, oss.str().c_str(), oss.str().length());
	path_ = path;
	backlog_ = backlog;
	type_ = type;
	conf_ = conf;
	num_ = num;
}

WatchDogHelper::~WatchDogHelper(void)
{
}

const char *HelperName[] = {
	NULL, NULL, "connector", "custom_connector", "custom_connector",
};

void WatchDogHelper::exec()
{
	struct sockaddr_un unaddr;
	int len = init_unix_socket_address(&unaddr, path_);
	printf("wuxz_debug:15\n");

	int listenfd = socket(unaddr.sun_family, SOCK_STREAM, 0);
	printf("wuxz_debug:14\n");
	bind(listenfd, (sockaddr *)&unaddr, len);
	listen(listenfd, backlog_);
	printf("wuxz_debug:13\n");
	/* relocate listenfd to stdin */
	dup2(listenfd, 0);
	close(listenfd);

	char *argv[9];
	int argc = 0;

	argv[argc++] = NULL;
	argv[argc++] = (char *)"-d";
	if (strcmp(cache_file, CACHE_CONF_NAME)) {
		argv[argc++] = (char *)"-f";
		argv[argc++] = cache_file;
	}
	if (conf_ == DBHELPER_TABLE_NEW) {
		argv[argc++] = (char *)"-t";
		char tableName[64];
		snprintf(tableName, 64, "../conf/table%d.conf", num_);
		argv[argc++] = tableName;
	} else if (conf_ == DBHELPER_TABLE_ORIGIN &&
		   strcmp(table_file, TABLE_CONF_NAME)) {
		argv[argc++] = (char *)"-t";
		argv[argc++] = table_file;
	}
	printf("watchdog_object_name_ = %s\n", watchdog_object_name_);
	argv[argc++] = watchdog_object_name_ + 6;
	argv[argc++] = (char *)"-";
	argv[argc++] = NULL;
	Thread *helperThread =
		new Thread(watchdog_object_name_, Thread::ThreadTypeProcess);
	helperThread->initialize_thread();
	argv[0] = (char *)HelperName[type_];
	printf("argv[0] = %s\n", argv[0]);
	for(int i = 0; i< 9; ++i)
	{
		printf("argv = %s\n", argv[i]);
	}
	argv[0] = "/usr/local/dtc/bin/connector";
	execv(argv[0], argv);
	log4cplus_error("helper[%s] execv error: %m", argv[0]);
}

int WatchDogHelper::verify()
{
	struct sockaddr_un unaddr;
	int len = init_unix_socket_address(&unaddr, path_);
	printf("unaddr.sun_path = %s, path_ = %s", unaddr.sun_path, path_);
	/* delay 100ms and verify socket */
	usleep(100 * 1000);
	printf("wuxz_debug : 1\n");
	int s = socket(unaddr.sun_family, SOCK_STREAM, 0);
	printf("wuxz_debug : 2\n");
	if (connect(s, (sockaddr *)&unaddr, len) < 0) {
		close(s);
		log4cplus_error("verify connect: %m");
		return -1;
	}
	close(s);
	return watchdog_object_pid_;
}
