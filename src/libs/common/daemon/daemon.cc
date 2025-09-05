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
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/resource.h>

#include "../config/config.h"
#include "../table/table_def.h"
#include "table/hotbackup_table_def.h"
#include "daemon/daemon.h"
#include "../log/log.h"
#include <pthread.h>
#include "config/dbconfig.h"
#include "version.h"
#include "table/table_def_manager.h"
#include "daemons.h"

DTCConfig *g_dtc_config = NULL;
DTCTableDefinition *g_table_def[2] = { NULL, NULL };
DbConfig *dbConfig = NULL;

volatile int stop = 0;
volatile int crash_signo = 0;
int background = 1;
const char stat_project_name[] = "daemon";
const char stat_usage_argv[] = "";

#define TABLE_CONF_NAME "../conf/dtc.yaml"
#define CACHE_CONF_NAME "../conf/dtc.yaml"

char d_cache_file[256] = CACHE_CONF_NAME;
char d_table_file[256] = TABLE_CONF_NAME;

pthread_t mainthreadid;

bool flatMode;
//Print version information
void show_version_detail()
{
	printf("%s version: %s\n", stat_project_name, version_detail);
}
//Print compilation information
void show_comp_date()
{
	printf("%s compile date: %s %s\n", stat_project_name, compdatestr,
	       comptimestr);
}

void show_usage()
{
	show_version_detail();
	show_comp_date();
	printf("Usage:\n    %s  [-d] [-h] [-v] [-V]%s\n", stat_project_name,
	       stat_usage_argv);
}

//Get initial information (input parameters, compilation info, etc.)
int load_entry_parameter(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "df:t:hvV")) != -1) {
		switch (c) {
		case 'd':
			background = 0;
			break;
		case 'f':
			strncpy(d_cache_file, optarg, sizeof(d_cache_file) - 1);
			break;
		case 't':
			strncpy(d_table_file, optarg, sizeof(d_table_file) - 1);
			break;
		case 'h':
			show_usage();
			exit(0);
		case 'v':
			show_version_detail();
			show_comp_date();
			exit(0);
		case 'V':
			show_version_detail();
			show_comp_date();
			exit(0);
		case '?':
			show_usage();
			exit(-1);
		}
	}

	//init_log("dtcd");
	log4cplus_info("%s v%s: starting....", stat_project_name, version);
	g_dtc_config = new DTCConfig;
	//load config file and copy it to ../stat
	if (g_dtc_config->load_yaml_file(d_table_file, true) == -1)
		return -1;

	if (g_dtc_config->load_yaml_file(d_cache_file, true))
		return -1;
	dbConfig = DbConfig::Load(g_dtc_config);
	if (dbConfig == NULL)
		return -1;
	g_table_def[0] = dbConfig->build_table_definition();
	if (g_table_def[0] == NULL)
		return -1;

	DTCTableDefinition *t;
	if ((t = TableDefinitionManager::instance()->load_table(d_table_file)) ==
	    NULL)
		return -1;
	TableDefinitionManager::instance()->set_cur_table_def(t, 0);

	g_table_def[1] = build_hot_backup_table();
	if (g_table_def[1] == NULL)
		return -1;

	if (!TableDefinitionManager::instance()->build_hot_backup_table_def())
		return -1;

	return 0;
}

static void sigterm_handler(int signo)
{
	stop = 1;
}
//Signal initialization, set background running process
int init_daemon()
{
	struct sigaction sa;
	sigset_t sset;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = sigterm_handler;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);
	sigaction(SIGQUIT, &sa, NULL);
	sigaction(SIGHUP, &sa, NULL);

	signal(SIGPIPE, SIG_IGN);
	signal(SIGCHLD, SIG_IGN);
	/********************************
 * sigemptyset: clear signal set
 * sigaddset: add signal to signal set
 * sigprocmask: query or set signal mask
 *********************************/
	sigemptyset(&sset);
	sigaddset(&sset, SIGTERM);
	sigaddset(&sset, SIGSEGV);
	sigaddset(&sset, SIGBUS);
	sigaddset(&sset, SIGABRT);
	sigaddset(&sset, SIGILL);
	sigaddset(&sset, SIGCHLD);
	sigaddset(&sset, SIGFPE);
	sigprocmask(SIG_UNBLOCK, &sset, &sset);

	int ret = background ? daemon(1, 1) : 0;
	mainthreadid = pthread_self();
	return ret;
}

void DaemonCrashed(int signo)
{
	stop = 1;
	crash_signo = signo;
	if (mainthreadid)
		pthread_kill(mainthreadid, SIGTERM);
}

void daemon_cleanup(void)
{
#if MEMCHECK
	DELETE(gBConfig);
	DELETE(gConfig);
	DELETE(g_table_def[0]);
	DELETE(g_table_def[1]);
	if (dbConfig)
		dbConfig->destory();
#endif
	// CrashProtect: special retval 85 means protected-crash
	if (crash_signo)
		exit(85);
}

int daemon_get_fd_limit(void)
{
	struct rlimit rlim;
	if (getrlimit(RLIMIT_NOFILE, &rlim) == -1) {
		log4cplus_info("Query FdLimit failed, errmsg[%s]",
			       strerror(errno));
		return -1;
	}

	return rlim.rlim_cur;
}

int daemon_set_fd_limit(int maxfd)
{
	struct rlimit rlim;
	if (maxfd) {
		/* raise open files */
		rlim.rlim_cur = maxfd;
		rlim.rlim_max = maxfd;
		if (setrlimit(RLIMIT_NOFILE, &rlim) == -1) {
			log4cplus_error(
				"Increase FdLimit failed, set val[%d] errmsg[%s]",
				maxfd, strerror(errno));
			return -1;
		}
	}
	return 0;
}

//core(coredump) file initialization
int init_core_dump(void)
{
	struct rlimit rlim;

	/* allow core dump  100M */
	rlim.rlim_cur = 100UL << 30;
	rlim.rlim_max = 100UL << 30;
	//Set maximum bytes for core file
	if (setrlimit(RLIMIT_CORE, &rlim) == -1) {
		//If setting fails, set soft limit to hard limit and reset
		if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
			rlim.rlim_cur = rlim.rlim_max;
			setrlimit(RLIMIT_CORE, &rlim);
		}

		log4cplus_info("EnableCoreDump size[%ld]", (long)rlim.rlim_max);
	}

	return 0;
}
