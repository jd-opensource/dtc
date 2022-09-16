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
#include <unistd.h>

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

const char *connector_name[] = {
	NULL, NULL, "connector", "custom_connector", "custom_connector",
};

void WatchDogHelper::exec()
{
	struct sockaddr_un unaddr;
	int len = init_unix_socket_address(&unaddr, path_);

	int listenfd = socket(unaddr.sun_family, SOCK_STREAM, 0);
	bind(listenfd, (sockaddr *)&unaddr, len);
	int i_ret = listen(listenfd, backlog_);
	if (i_ret == 0) {
		log4cplus_info("backlog_:%d" , backlog_);
	}
	
	/* relocate listenfd to stdin */
	dup2(listenfd, 0);
	close(listenfd);
    log4cplus_info("exec path:%s" , path_);

	char *argv[9];
	int argc = 0;
  
	argv[argc++] = NULL;
	argv[argc++] = (char *)"-d";
	if (strcmp(daemons_cache_file, CACHE_CONF_NAME)) {
		argv[argc++] = (char *)"-f";
		argv[argc++] = daemons_cache_file;
	}
	if (conf_ == DBHELPER_TABLE_NEW) {
		argv[argc++] = (char *)"-t";// 4
		char tableName[64];
		snprintf(tableName, 64, "../conf/dtc%d.yaml", num_);
		argv[argc++] = tableName; // 5
	} else if (conf_ == DBHELPER_TABLE_ORIGIN &&
		   strcmp(daemons_table_file, TABLE_CONF_NAME)) {
		argv[argc++] = (char *)"-t";
		argv[argc++] = daemons_table_file;
	}
	argv[argc++] = watchdog_object_name_ + 6; // 6 : 0m
	argv[argc++] = (char *)"-"; // 7
	argv[argc++] = NULL; // 8
	Thread *helperThread =
		new Thread(watchdog_object_name_, Thread::ThreadTypeProcess);
	helperThread->initialize_thread();
	char filedir[260] = {0};
	char filepath[260] = {0};
	char fn[260] = {0};
	snprintf(fn, sizeof(fn), "/proc/%d/exe", getpid());
	int rv = readlink(fn, filedir, sizeof(filedir) - 1);
	if(rv > 0)
	{
		filedir[rv] = '\0';
		std::string str = filedir;
		rv = str.rfind('/');
		strcpy(filedir, str.substr(0, rv).c_str());
	}
	sprintf(filepath, "%s/%s", filedir, connector_name[type_]);
	log4cplus_info("connector path:%s", filepath);
	argv[0] = filepath;
	execv(argv[0], argv);
	log4cplus_error("helper[%s] execv error: %m", argv[0]);
}

int WatchDogHelper::verify()
{
	struct sockaddr_un unaddr;
	int len = init_unix_socket_address(&unaddr, path_);
	log4cplus_info("verify path:%s." , path_);
	/* delay 100ms and verify socket */
	//usleep(5000 * 1000);
	sleep(2);
	int s = socket(unaddr.sun_family, SOCK_STREAM, 0);
	if (connect(s, (sockaddr *)&unaddr, len) < 0) {
		close(s);
		log4cplus_error("verify connect: %m");
		return -1;
	}
	log4cplus_info("verify success.");
	close(s);
	return watchdog_object_pid_;
}
