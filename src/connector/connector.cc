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

#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sched.h>
// local include files
#include "mysql_operation.h"
// common include files
#include "dtc_global.h"
#include "version.h"
#include "proc_title.h"
#include "dtcutils.h"
#include "log/log.h"
#include "config/config.h"
#include "daemon/daemon.h"
#include "listener/listener.h"
#include "socket/socket_addr.h"
#include "socket/unix_socket.h"
// core include files
#include "buffer/buffer_pond.h"
// daemons include files
#include "daemon_listener.h"

const char progname[] = "connector";

static ConnectorProcess *conn_proc;
static unsigned int proc_timeout;

int target_new_hash;
int hash_changing;

static int sync_decode(DtcJob *task, int netfd, ConnectorProcess *conn_proc)
{
	SimpleReceiver receiver(netfd);
	int code;
	do {
		code = task->do_decode(receiver);
		if (code == DecodeFatalError) {
			if (errno != 0)
				log4cplus_info("decode fatal error, fd=%d, %m",
					       netfd);
			return -1;
		}
		if (code == DecodeDataError) {
			if (task->result_code() == 0 ||
			    task->result_code() ==
				    -EC_EXTRA_SECTION_DATA) // -EC_EXTRA_SECTION_DATA   verify package
				return 0;
			log4cplus_info("decode error, fd=%d, %d", netfd,
				       task->result_code());
			return -1;
		}
		conn_proc->set_title("Receiving...");
	} while (!stop && code != DecodeDone);

	if (task->result_code() < 0) {
		log4cplus_info("register result, fd=%d, %d", netfd,
			       task->result_code());
		return -1;
	}
	return 0;
}

static int sync_send(Packet *reply, int netfd)
{
	int code;
	do {
		code = reply->Send(netfd);
		if (code == SendResultError) {
			log4cplus_info("send error, fd=%d, %m", netfd);
			return -1;
		}
	} while (!stop && code != SendResultDone);

	return 0;
}

static void alarm_handler(int signo)
{
	if (background == 0 && getppid() == 1)
		exit(0);
	alarm(10);
}

static int accept_connection(int fd)
{
	conn_proc->set_title("listener");
	signal(SIGALRM, alarm_handler);
	while (!stop) {
		alarm(10);
		int newfd;
		if ((newfd = accept(fd, NULL, 0)) >= 0) {
			alarm(0);
			return newfd;
		}
		if (newfd < 0 && errno == EINVAL) {
			if (getppid() == (pid_t)1) {
				log4cplus_error(
					"dtc parent process not exist. helper[%d] exit now.",
					getpid());
				exit(0);
			}
			usleep(10000);
		}
	}
	exit(0);
}

static void proc_timeout_handler(int signo)
{
	log4cplus_error(
		"mysql process timeout(more than %u seconds), helper[pid: %d] exit now.",
		proc_timeout, getpid());
	exit(-1);
}

#ifdef __DEBUG__
static void inline simulate_helper_delay(void)
{
	char *env = getenv("ENABLE_SIMULATE_DTC_HELPER_DELAY_SECOND");
	if (env && env[0] != 0) {
		unsigned delay_sec = atoi(env);
		if (delay_sec > 5)
			delay_sec = 5;

		log4cplus_debug("simulate dtc helper delay second[%d s]",
				delay_sec);
		sleep(delay_sec);
	}
	return;
}
#endif

struct HelperParameter {
	int netfd;
	int gid;
	int role;
};

static int helper_proc_run(struct HelperParameter *args)
{
	// close listen fd
	close(0);
	open("/dev/null", O_RDONLY);

	conn_proc->set_title("Initializing...");

	if (proc_timeout > 0)
		signal(SIGALRM, proc_timeout_handler);

	alarm(proc_timeout);
	if (conn_proc->do_init(
		    args->gid, dbConfig,
		    TableDefinitionManager::instance()->get_cur_table_def(),
		    args->role) != 0) {
		log4cplus_error("%s", "helper process init failed");
		exit(-1);
	}

	conn_proc->init_ping_timeout();
	alarm(0);

	hash_changing = g_dtc_config->get_int_val("cache", "HashChanging", 0);
	target_new_hash =
		g_dtc_config->get_int_val("cache", "TargetNewHash", 0);

	unsigned int timeout;

	while (!stop) {
		conn_proc->set_title("Waiting...");
		DtcJob *task = new DtcJob(
			TableDefinitionManager::instance()->get_cur_table_def());
		if (sync_decode(task, args->netfd, conn_proc) < 0) {
			delete task;
			break;
		}

		if (task->result_code() == 0) {
			switch (task->request_code()) {
			case DRequest::Insert:
			case DRequest::Update:
			case DRequest::Delete:
			case DRequest::Replace:
			case DRequest::ReloadConfig:
				timeout = 2 * proc_timeout;
			default:
				timeout = proc_timeout;
			}
			alarm(timeout);
#ifdef __DEBUG__
			simulate_helper_delay();
#endif
			conn_proc->do_process(task);
			alarm(0);
		}

		conn_proc->set_title("Sending...");
		Packet *reply = new Packet;
		reply->encode_result(task);

		if (sync_send(reply, args->netfd) < 0) {
			delete reply;
			delete task;
			break;
		}
		delete reply;
		delete task;
	}
	close(args->netfd);
	conn_proc->set_title("Exiting...");

	delete conn_proc;
	daemon_cleanup();
#if MEMCHECK
	log4cplus_info("%s v%s: stopped", progname, version);
	dump_non_delete();
	log4cplus_debug("memory allocated %lu virtual %lu", count_alloc_size(),
			count_virtual_size());
#endif
	exit(0);
	return 0;
}

int check_db_version(void)
{
	int ver = CDBConn::get_client_version();
	if (ver == MYSQL_VERSION_ID)
		return 0;
	log4cplus_warning(
		"MySql version mismatch: header=%d.%d.%d lib=%d.%d.%d",
		MYSQL_VERSION_ID / 10000, (MYSQL_VERSION_ID / 100) % 100,
		MYSQL_VERSION_ID % 100, ver / 10000, (ver / 100) % 100,
		ver % 100);
	return -1;
}

int check_db_table(int gid, int role)
{
	ConnectorProcess *helper = new ConnectorProcess();

	if (proc_timeout > 1) {
		helper->set_proc_timeout(proc_timeout - 1);
		signal(SIGALRM, proc_timeout_handler);
	}

	alarm(proc_timeout);
	if (helper->do_init(
		    gid, dbConfig,
		    TableDefinitionManager::instance()->get_cur_table_def(),
		    role) != 0) {
		log4cplus_error("%s", "helper process init failed");
		delete helper;
		alarm(0);
		return (-1);
	}

	if (helper->check_table() != 0) {
		delete helper;
		alarm(0);
		return (-2);
	}

	alarm(0);
	delete helper;

	return (0);
}

int main(int argc, char **argv)
{
	init_proc_title(argc, argv);
	init_log4cplus();
	if (load_entry_parameter(argc, argv) < 0)
		return -1;
	check_db_version();
	argc -= optind;
	argv += optind;

	struct HelperParameter helperArgs = { 0, 0, 0 };
	char *addr = NULL;
	if (argc > 0) {
		char *p;
		helperArgs.gid = strtol(argv[0], &p, 0);
		if (*p == '\0' || *p == MACHINEROLESTRING[0])
			helperArgs.role = 0;
		else if (*p == MACHINEROLESTRING[1])
			helperArgs.role = 1;
		else {
			log4cplus_error("Bad machine id: %s", argv[0]);
			return -1;
		}
	}
	if (argc != 2 && argc != 3) {
		show_usage();
		return -1;
	}
	int usematch = g_dtc_config->get_int_val("cache",
						 "UseMatchedAsAffectedRows", 1);

	int backlog = g_dtc_config->get_int_val("cache", "MaxListenCount", 256);

	int helperTimeout =
		g_dtc_config->get_int_val("cache", "HelperTimeout", 30);

	if (helperTimeout > 1)
		proc_timeout = helperTimeout - 1;
	else
		proc_timeout = 0;
		
	addr = argv[1];
	if (check_db_table(helperArgs.gid, helperArgs.role) != 0) {
		return -1;
	}
	int fd = -1;
	if (!strcmp(addr, "-"))
		fd = 0;
	else {
		std::string dtcid = g_dtc_config->get_config_node()["props"]["listener.port.dtc"].as<std::string>();
		if(dtcid != "none")
		{
			log4cplus_warning(
				"standalone %s need DTCID set to NONE",
				progname);
			return -1;
		}
		
		SocketAddress sockaddr;
		const char *err =
			sockaddr.set_address(addr, argc == 2 ? NULL : argv[2]);
		if (err) {
			log4cplus_warning("host %s port %s: %s", addr,
					  argc == 2 ? "NULL" : argv[2], err);
			return -1;
		}
		if (sockaddr.socket_type() != SOCK_STREAM) {
			log4cplus_warning(
				"standalone %s don't support UDP protocol",
				progname);
			return -1;
		}
		fd = socket_bind(&sockaddr, backlog);
		if (fd < 0)
			return -1;
	}

	log4cplus_debug(
		"If you want to simulate db busy,"
		"you can set \"ENABLE_SIMULATE_DTC_HELPER_DELAY_SECOND=second\" before dtc startup");
	init_daemon();
	conn_proc = new ConnectorProcess();
	if (usematch)
		conn_proc->use_matched_rows();
#if HAS_LOGAPI
	conn_proc->logapi.do_init(
		g_dtc_config->get_int_val("LogApi", "MessageId", 0),
		g_dtc_config->get_int_val("LogApi", "CallerId", 0),
		g_dtc_config->get_int_val("LogApi", "TargetId", 0),
		g_dtc_config->get_int_val("LogApi", "InterfaceId", 0));
#endif

	conn_proc->init_title(helperArgs.gid, helperArgs.role);
	if (proc_timeout > 1)
		conn_proc->set_proc_timeout(proc_timeout - 1);
	while (!stop) {
		helperArgs.netfd = accept_connection(fd);
		char buf[16];
		memset(buf, 0, 16);
		buf[0] = WATCHDOG_INPUT_OBJECT;
		snprintf(buf + 1, 15, "%s", conn_proc->get_name());
		watch_dog_fork(buf, (int (*)(void *))helper_proc_run,
			       (void *)&helperArgs);

		close(helperArgs.netfd);
	}

	if (fd > 0 && addr && addr[0] == '/')
		unlink(addr);
	return 0;
}
