#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sched.h>
#include <dtc_global.h>
#include <version.h>
#include <proc_title.h>
#include <log.h>
#include <config.h>
#include "mysql_operation.h"
#include "buffer_pond.h"
#include <daemon.h>
#include <listener.h>
#include <socket_addr.h>
#include <unix_socket.h>
#include "daemon_listener.h"
#include "dtcutils.h"
extern void _set_remote_log_config_(const char *addr, int port, int businessid);
const char progname[] = "mysql-helper";
char cacheFile[256] = CACHE_CONF_NAME;
char tableFile[256] = TABLE_CONF_NAME;

static CHelperProcess *helperProc;
static unsigned int procTimeout;

int targetNewHash;
int hashChanging;

static int SyncDecode(DtcJob *task, int netfd, CHelperProcess *helperProc)
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
		helperProc->SetTitle("Receiving...");
	} while (!stop && code != DecodeDone);

	if (task->result_code() < 0) {
		log4cplus_info("register result, fd=%d, %d", netfd,
			       task->result_code());
		return -1;
	}
	return 0;
}

static int SyncSend(Packet *reply, int netfd)
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

static int AcceptConnection(int fd)
{
	helperProc->SetTitle("listener");
	signal(SIGALRM, alarm_handler);
	while (!stop) {
		alarm(10);
		int newfd;
		if ((newfd = accept(fd, NULL, 0)) >= 0) {
			alarm(0);
			return newfd;
		}
		if (newfd < 0 && errno == EINVAL) {
			if (getppid() == (pid_t)1) { // �������Ѿ��˳�
				log4cplus_error(
					"ttc father process not exist. helper[%d] exit now.",
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
		procTimeout, getpid());
	exit(-1);
}

#ifdef __DEBUG__
static void inline simulate_helper_delay(void)
{
	char *env = getenv("ENABLE_SIMULATE_TTC_HELPER_DELAY_SECOND");
	if (env && env[0] != 0) {
		unsigned delay_sec = atoi(env);
		if (delay_sec > 5)
			delay_sec = 5;

		log4cplus_debug("simulate ttc helper delay second[%d s]",
				delay_sec);
		sleep(delay_sec);
	}
	return;
}
#endif

struct THelperProcParameter {
	int netfd;
	int gid;
	int role;
};

static int HelperProcRun(struct THelperProcParameter *args)
{
	// close listen fd
	close(0);
	open("/dev/null", O_RDONLY);

	helperProc->SetTitle("Initializing...");

	if (procTimeout > 0)
		signal(SIGALRM, proc_timeout_handler);

	alarm(procTimeout);
	if (helperProc->Init(
		    args->gid, dbConfig,
		    TableDefinitionManager::instance()->get_cur_table_def(),
		    args->role) != 0) {
		log4cplus_error("%s", "helper process init failed");
		exit(-1);
	}

	helperProc->InitPingTimeout();
	alarm(0);

	hashChanging = g_dtc_config->get_int_val("cache", "HashChanging", 0);
	targetNewHash = g_dtc_config->get_int_val("cache", "TargetNewHash", 0);

	unsigned int timeout;

	while (!stop) {
		helperProc->SetTitle("Waiting...");
		DtcJob *task = new DtcJob(
			TableDefinitionManager::instance()->get_cur_table_def());
		if (SyncDecode(task, args->netfd, helperProc) < 0) {
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
				timeout = 2 * procTimeout;
			default:
				timeout = procTimeout;
			}
			alarm(timeout);
#ifdef __DEBUG__
			simulate_helper_delay();
#endif
			helperProc->ProcessTask(task);
			alarm(0);
		}

		helperProc->SetTitle("Sending...");
		Packet *reply = new Packet;
		reply->encode_result(task);

		if (SyncSend(reply, args->netfd) < 0) {
			delete reply;
			delete task;
			break;
		}
		delete reply;
		delete task;
	}
	close(args->netfd);
	helperProc->SetTitle("Exiting...");

	delete helperProc;
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
	int ver = CDBConn::ClientVersion();
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
	CHelperProcess *helper = new CHelperProcess();

	if (procTimeout > 1) {
		helper->SetProcTimeout(procTimeout - 1);
		signal(SIGALRM, proc_timeout_handler);
	}

	alarm(procTimeout);
	if (helper->Init(gid, dbConfig,
			 TableDefinitionManager::instance()->get_cur_table_def(),
			 role) != 0) {
		log4cplus_error("%s", "helper process init failed");
		delete helper;
		alarm(0);
		return (-1);
	}

	if (helper->CheckTable() != 0) {
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
	if (load_entry_parameter(argc, argv) < 0)
		return -1;
	check_db_version();
	argc -= optind;
	argv += optind;

	struct THelperProcParameter helperArgs = { 0, 0, 0 };
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
		procTimeout = helperTimeout - 1;
	else
		procTimeout = 0;
	addr = argv[1];
	if (dbConfig->checkTable &&
	    check_db_table(helperArgs.gid, helperArgs.role) != 0) {
		return -1;
	}
	int fd = -1;
	if (!strcmp(addr, "-"))
		fd = 0;
	else {
		if (strcasecmp(
			    g_dtc_config->get_str_val("cache", "CacheShmKey") ?:
				    "",
			    "none") != 0) {
			log4cplus_warning(
				"standalone %s need CacheShmKey set to NONE",
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
		"you can set \"ENABLE_SIMULATE_TTC_HELPER_DELAY_SECOND=second\" before ttc startup");
	init_daemon();
	helperProc = new CHelperProcess();
	if (usematch)
		helperProc->UseMatchedRows();
#if HAS_LOGAPI
	helperProc->logapi.Init(
		g_dtc_config->get_int_val("LogApi", "MessageId", 0),
		g_dtc_config->get_int_val("LogApi", "CallerId", 0),
		g_dtc_config->get_int_val("LogApi", "TargetId", 0),
		g_dtc_config->get_int_val("LogApi", "InterfaceId", 0));
#endif

	helperProc->InitTitle(helperArgs.gid, helperArgs.role);
	if (procTimeout > 1)
		helperProc->SetProcTimeout(procTimeout - 1);
	while (!stop) {
		helperArgs.netfd = AcceptConnection(fd);
		char buf[16];
		memset(buf, 0, 16);
		buf[0] = WATCHDOG_INPUT_OBJECT;
		snprintf(buf + 1, 15, "%s", helperProc->Name());
		watch_dog_fork(buf, (int (*)(void *))HelperProcRun,
			       (void *)&helperArgs);

		close(helperArgs.netfd);
	}

	if (fd > 0 && addr && addr[0] == '/')
		unlink(addr);
	return 0;
}
