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
#include <dlfcn.h>

#include <dtc_global.h>
#include <version.h>
#include <proc_title.h>
#include <log.h>
#include <config.h>
#include <dbconfig.h>
#include "comm_process.h"
#include <daemon.h>
#include <socket_addr.h>
#include <listener.h>
#include <unix_socket.h>
#include <listener.h>
#include <task_base.h>
#include <fork.cc>

const char service_file[] = "./helper-service.so";
const char create_handle_name[] = "create_process";
const char project_name[] = "custom-helper";
const char usage_argv[] = "machId addr [port]";
char cache_file[256] = CACHE_CONF_NAME;
char table_file[256] = TABLE_CONF_NAME;

static CreateHandle create_helper = NULL;
static CommHelper *helper_proc;
static unsigned int proc_timeout;

class HelperMain {
    public:
	HelperMain(CommHelper *helper) : h(helper){};

	void attach(DtcJob *job)
	{
		h->do_attach((void *)job);
	}
	void init_title(int group, int role)
	{
		h->init_title(group, role);
	}
	void set_title(const char *status)
	{
		h->set_title(status);
	}
	const char *Name()
	{
		return h->Name();
	}
	int pre_init(int gid, int r)
	{
		if (dbConfig->machineCnt <= gid) {
			log4cplus_error(
				"parse config error, machineCnt[%d] <= group_id[%d]",
				dbConfig->machineCnt, gid);
			return (-1);
		}
		h->_group_id = gid;
		h->_role = r;
		h->_dbconfig = dbConfig;
		h->_tdef = g_table_def[0];
		h->_config = g_dtc_config;
		h->_server_string = dbConfig->mach[gid].role[r].addr;
		h->logapi.init_target(h->_server_string);
		return 0;
	}

    private:
	CommHelper *h;
};

static int load_service(const char *dll_file)
{
	void *dll;

	dll = dlopen(dll_file, RTLD_NOW | RTLD_GLOBAL);
	if (dll == (void *)NULL) {
		log4cplus_error("dlopen(%s) error: %s", dll_file, dlerror());
		return -1;
	}

	create_helper = (CreateHandle)dlsym(dll, create_handle_name);
	if (create_helper == NULL) {
		log4cplus_error("function[%s] not found", create_handle_name);
		return -2;
	}

	return 0;
}

static int sync_decode(DtcJob *job, int netfd, CommHelper *helper_proc)
{
	SimpleReceiver receiver(netfd);
	int code;
	do {
		code = job->do_decode(receiver);
		if (code == DecodeFatalError) {
			if (errno != 0)
				log4cplus_info("decode fatal error, fd=%d, %m",
					       netfd);
			return -1;
		}
		if (code == DecodeDataError) {
			if (job->result_code() == 0 ||
			    job->result_code() ==
				    -EC_EXTRA_SECTION_DATA) // -EC_EXTRA_SECTION_DATA   verify package
				return 0;
			log4cplus_info("decode error, fd=%d, %d", netfd,
				       job->result_code());
			return -1;
		}
		HelperMain(helper_proc).set_title("Receiving...");
	} while (!stop && code != DecodeDone);

	if (job->result_code() < 0) {
		log4cplus_info("register result, fd=%d, %d", netfd,
			       job->result_code());
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
	HelperMain(helper_proc).set_title("listener");
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
					"dtc father process not exist. helper[%d] exit now.",
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
		"comm-helper process timeout(more than %u seconds), helper[pid: %d] exit now.",
		proc_timeout, getpid());
	exit(-1);
}

struct ConnectorInfo {
	int netfd;
	int gid;
	int role;
};

static int helper_proc_run(struct ConnectorInfo *args)
{
	// close listen fd
	close(0);
	open("/dev/null", O_RDONLY);

	HelperMain(helper_proc).set_title("Initializing...");

	if (proc_timeout > 0)
		signal(SIGALRM, proc_timeout_handler);

	alarm(proc_timeout);
	if (helper_proc->do_init() != 0) {
		log4cplus_error("%s", "helper process init failed");
		exit(-1);
	}

	alarm(0);

	unsigned int timeout;

	while (!stop) {
		HelperMain(helper_proc).set_title("Waiting...");
		DtcJob *job = new DtcJob(g_table_def[0]);
		if (sync_decode(job, args->netfd, helper_proc) < 0) {
			delete job;
			break;
		}

		if (job->result_code() == 0) {
			switch (job->request_code()) {
			case DRequest::Insert:
			case DRequest::Update:
			case DRequest::Delete:
			case DRequest::Replace:
				timeout = 2 * proc_timeout;
			default:
				timeout = proc_timeout;
			}
			HelperMain(helper_proc).attach(job);
			alarm(timeout);
			helper_proc->do_execute();
			alarm(0);
		}

		HelperMain(helper_proc).set_title("Sending...");
		Packet *reply = new Packet;
		reply->encode_result(job);

		delete job;
		if (sync_send(reply, args->netfd) < 0) {
			delete reply;
			break;
		}
		delete reply;
	}
	close(args->netfd);
	HelperMain(helper_proc).set_title("Exiting...");

	delete helper_proc;
	daemon_cleanup();
#if MEMCHECK
	log4cplus_info("%s v%s: stopped", project_name, version);
	dump_non_delete();
	log4cplus_debug("memory allocated %lu virtual %lu", count_alloc_size(),
			count_virtual_size());
#endif
	exit(0);
	return 0;
}

int main(int argc, char **argv)
{
	init_proc_title(argc, argv);
	if (load_entry_parameter(argc, argv) < 0)
		return -1;

	argc -= optind;
	argv += optind;

	struct ConnectorInfo helperArgs = { 0, 0, 0 };
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

	int backlog = g_dtc_config->get_int_val("cache", "MaxListenCount", 256);
	int helperTimeout =
		g_dtc_config->get_int_val("cache", "HelperTimeout", 30);
	if (helperTimeout > 1)
		proc_timeout = helperTimeout - 1;
	else
		proc_timeout = 0;
	addr = argv[1];

	// load dll file
	const char *file = getenv("HELPER_SERVICE_FILE");
	if (file == NULL || file[0] == 0)
		file = service_file;
	if (load_service(file) != 0)
		return -1;

	helper_proc = create_helper();
	if (helper_proc == NULL) {
		log4cplus_error("create helper error");
		return -1;
	}
	if (HelperMain(helper_proc).pre_init(helperArgs.gid, helperArgs.role) <
	    0) {
		log4cplus_error("%s", "helper prepare init failed");
		exit(-1);
	}
	if (helper_proc->global_init() != 0) {
		log4cplus_error("helper gobal-init error");
		return -1;
	}

	int fd = -1;
	if (!strcmp(addr, "-"))
		fd = 0;
	else {
		if (strcasecmp(
			    g_dtc_config->get_str_val("cache", "DTCID") ?:
				    "",
			    "none") != 0) {
			log4cplus_warning(
				"standalone %s need DTCID set to NONE",
				project_name);
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
				project_name);
			return -1;
		}
		fd = socket_bind(&sockaddr, backlog);
		if (fd < 0)
			return -1;
	}

	init_daemon();

#if HAS_LOGAPI
	helper_proc->logapi.do_init(
		gConfig->get_int_val("LogApi", "MessageId", 0),
		gConfig->get_int_val("LogApi", "CallerId", 0),
		gConfig->get_int_val("LogApi", "TargetId", 0),
		gConfig->get_int_val("LogApi", "InterfaceId", 0));
#endif

	HelperMain(helper_proc).init_title(helperArgs.gid, helperArgs.role);
	if (proc_timeout > 1)
		helper_proc->set_proc_timeout(proc_timeout - 1);
	while (!stop) {
		helperArgs.netfd = accept_connection(fd);

		watch_dog_fork(HelperMain(helper_proc).Name(),
			       (int (*)(void *))helper_proc_run,
			       (void *)&helperArgs);

		close(helperArgs.netfd);
	}

	if (fd > 0 && addr && addr[0] == '/')
		unlink(addr);
	return 0;
}
