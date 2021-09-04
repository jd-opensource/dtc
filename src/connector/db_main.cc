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
#include "../daemons/daemon_listener.h"
//#include "dtcutils.h"
#include "table/table_def_manager.h"
#include <assert.h>
#include <config/config.h>
#include <daemon/daemon.h>
#include <dtc_global.h>
#include <fcntl.h>
#include <listener/listener.h>
#include <log/log.h>
#include <proc_title.h>
#include <sched.h>
#include <socket/socket_addr.h>
#include <socket/unix_socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <version.h>

#include "db_process_rocks.h"
#include "rocksdb_conn.h"
#include "rocksdb_direct_process.h"

const char progname_t[] = "rocksdb_connector";
const char usage_argv_t[] = "machId addr [port]";
char cache_file[256] = CACHE_CONF_NAME;
char table_file[256] = TABLE_CONF_NAME;

static HelperProcessBase *helper_proc;
static unsigned int proc_timeout;

static RocksDBConn *g_rocksdb_conn;
std::string g_rocksdb_path = "../rocksdb_data";
std::string g_rocks_direct_access_path = "/tmp/domain_socket/";
static RocksdbDirectProcess *g_rocksdb_direct_process;

int target_new_hash;
int hash_changing;

static int sync_decode(DtcJob *job, int netfd, HelperProcessBase *helper_proc)
{
	SimpleReceiver receiver(netfd);
	int code;
	do {
		code = job->do_decode(receiver);
		if (code == DecodeFatalError) {
			if (errno != 0)
				log4cplus_info("decode fatal error, fd=%d, %m",
					       netfd);
			log4cplus_info("decode error!!!!!");
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
		helper_proc->set_title("Receiving...");
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
	helper_proc->set_title("listener");
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
			log4cplus_info("parent process close the connection!");
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

	helper_proc->set_title("Initializing...");

	if (proc_timeout > 0)
		signal(SIGALRM, proc_timeout_handler);

	alarm(proc_timeout);
	if (helper_proc->do_init(
		    args->gid, dbConfig,
		    TableDefinitionManager::instance()->get_cur_table_def(),
		    args->role) != 0) {
		log4cplus_error("%s", "helper process init failed");
		exit(-1);
	}

	helper_proc->init_ping_timeout();
	alarm(0);

	hash_changing = g_dtc_config->get_int_val("cache", "HashChanging", 0);
	target_new_hash =
		g_dtc_config->get_int_val("cache", "TargetNewHash", 0);

	unsigned int timeout;

	while (!stop) {
		helper_proc->set_title("Waiting...");
		DtcJob *job = new DtcJob(
			TableDefinitionManager::instance()->get_cur_table_def());
		if (sync_decode(job, args->netfd, helper_proc) < 0) {
			log4cplus_info("sync decode failed!");
			delete job;
			break;
		}

		if (job->result_code() == 0) {
			switch (job->request_code()) {
			case DRequest::Insert:
			case DRequest::Update:
			case DRequest::Delete:
			case DRequest::Replace:
			case DRequest::ReloadConfig:
			case DRequest::Replicate:
			case DRequest::LocalMigrate:
				timeout = 2 * proc_timeout;
			default:
				timeout = proc_timeout;
			}
			alarm(timeout);
#ifdef __DEBUG__
			simulate_helper_delay();
#endif
			helper_proc->process_task(job);
			alarm(0);
		}

		helper_proc->set_title("Sending...");
		Packet *reply = new Packet;
		reply->encode_result(job);

		if (sync_send(reply, args->netfd) < 0) {
			delete reply;
			delete job;
			break;
			log4cplus_info("sync send failed!");
		}
		delete reply;
		delete job;
	}
	close(args->netfd);
	helper_proc->set_title("Exiting...");

	delete helper_proc;
	daemon_cleanup();
#if MEMCHECK
	log4cplus_info("%s v%s: stopped", progname_t, version);
	dump_non_delete();
	log4cplus_debug("memory allocated %lu virtual %lu", count_alloc_size(),
			count_virtual_size());
#endif
	log4cplus_info("helper exit!");

	exit(0);
	return 0;
}

static int helper_proc_run_rocks(struct ConnectorInfo args)
{
	log4cplus_info(
		"xx77xx11 test multiple thread model! threadId:%d, fd:%d",
		std::this_thread::get_id(), args.netfd);

	open("/dev/null", O_RDONLY);

	helper_proc->set_title("Initializing...");

	if (proc_timeout > 0)
		signal(SIGALRM, proc_timeout_handler);

	alarm(proc_timeout);

	// helper_proc->init_ping_timeout();
	alarm(0);

	hash_changing = g_dtc_config->get_int_val("cache", "HashChanging", 0);
	target_new_hash =
		g_dtc_config->get_int_val("cache", "TargetNewHash", 0);

	unsigned int timeout;

	while (!stop) {
		helper_proc->set_title("Waiting...");
		DtcJob *job = new DtcJob(
			TableDefinitionManager::instance()->get_cur_table_def());
		if (sync_decode(job, args.netfd, helper_proc) < 0) {
			log4cplus_info("sync decode failed!");
			delete job;
			break;
		}

		log4cplus_info("receive request, threadId:%d",
			       std::this_thread::get_id());

		if (job->result_code() == 0) {
			switch (job->request_code()) {
			case DRequest::Insert:
			case DRequest::Update:
			case DRequest::Delete:
			case DRequest::Replace:
			case DRequest::ReloadConfig:
			case DRequest::Replicate:
			case DRequest::LocalMigrate:
				timeout = 2 * proc_timeout;
			default:
				timeout = proc_timeout;
			}
			alarm(timeout);
#ifdef __DEBUG__
			simulate_helper_delay();
#endif
			helper_proc->process_task(job);
			alarm(0);
		}

		helper_proc->set_title("Sending...");
		Packet *reply = new Packet;
		reply->encode_result(job);

		if (sync_send(reply, args.netfd) < 0) {
			delete reply;
			delete job;
			break;
			log4cplus_info("sync send failed!");
		}
		delete reply;
		delete job;
	}
	close(args.netfd);
	helper_proc->set_title("Exiting...");

	daemon_cleanup();
#if MEMCHECK
	log4cplus_info("%s v%s: stopped", progname_t, version);
	dump_non_delete();
	log4cplus_debug("memory allocated %lu virtual %lu", count_alloc_size(),
			count_virtual_size());
#endif
	log4cplus_info("helper exit!");

	return 0;
}

// check version base on DB type
int check_db_version(void)
{
	// dbConfig->dstype = 1;
	switch (dbConfig->dstype) {
	case 0:
	default:
	case 2: {
		log4cplus_debug("no need to check rocksdb!");
		// checker guass db version
		break;
	}
	}

	return -1;
}

int check_db_table(int gid, int role)
{
	HelperProcessBase *helper;
	switch (dbConfig->dstype) {
	case 0:
	default:
	case 2:
		// no table concept in rocksdb, no need to check
		log4cplus_error("no need to check table in rocksdb storage!");
		return 0;
	}

	if (proc_timeout > 1) {
		helper->set_proc_timeout(proc_timeout - 1);
		signal(SIGALRM, proc_timeout_handler);
	}

	alarm(proc_timeout);
	log4cplus_debug("begin initialize gauss db");
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

int create_rocks_domain_dir()
{
	// create domain socket directory
	int ret = access(g_rocks_direct_access_path.c_str(), F_OK);
	if (ret != 0) {
		int err = errno;
		if (errno == ENOENT) {
			// create log dir
			if (mkdir(g_rocks_direct_access_path.c_str(), 0755) !=
			    0) {
				log4cplus_error(
					"create rocksdb domain socket dir failed! path:%s, errno:%d",
					g_rocks_direct_access_path.c_str(),
					errno);
				return -1;
			}
		} else {
			log4cplus_error(
				"access rocksdb domain socket dir failed!, path:%s, errno:%d",
				g_rocks_direct_access_path.c_str(), errno);
			return -1;
		}
	}

	return 0;
}

// process rocksdb direct access
int rocks_direct_access_proc()
{
	log4cplus_error("Rocksdb direct access channel open!");

	std::string socketPath = g_rocks_direct_access_path;
	std::string dtcDeployAddr =
		dbConfig->cfgObj->get_str_val("cache", "BIND_ADDR");

	SocketAddress sockAddr;
	const char *strRet = sockAddr.set_address(dtcDeployAddr.c_str());
	if (strRet) {
		log4cplus_error("parse dtc bind addr failed, errmsg:%s",
				strRet);
		return -1;
	}

	int dtcDeployPort;
	switch (sockAddr.addr->sa_family) {
	case AF_INET:
		dtcDeployPort = ntohs(sockAddr.in4->sin_port);
		break;
	case AF_INET6:
		dtcDeployPort = ntohs(sockAddr.in6->sin6_port);
		break;
	default:
		log4cplus_error("unsupport addr type! addr:%s, type:%d",
				dtcDeployAddr.c_str(),
				sockAddr.addr->sa_family);
		return -1;
	}
	assert(dtcDeployPort > 0);

	socketPath.append("rocks_direct_")
		.append(std::to_string(dtcDeployPort))
		.append(".sock");

	g_rocksdb_direct_process =
		new RocksdbDirectProcess(socketPath, helper_proc);
	if (!g_rocksdb_direct_process) {
		log4cplus_error("create RocksdbDirectProcess failed!");
		return -1;
	}

	int ret = g_rocksdb_direct_process->init();
	if (ret != 0)
		return -1;

	return g_rocksdb_direct_process->run_process();
}

int main(int argc, char **argv)
{
	init_proc_title(argc, argv);
	if (load_entry_parameter(argc, argv) < 0)
		return -1;

	check_db_version();
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
	log4cplus_error("helper listen addr:%s", addr);

	if (dbConfig->checkTable &&
	    check_db_table(helperArgs.gid, helperArgs.role) != 0) {
		return -1;
	}

	int fd = -1;
	if (!strcmp(addr, "-"))
		fd = 0;
	else {
		if (strcasecmp(g_dtc_config->get_str_val("cache", "DTCID") ?:
				       "",
			       "none") != 0) {
			log4cplus_warning(
				"standalone %s need DTCID set to NONE",
				progname_t);
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
				progname_t);
			return -1;
		}
		fd = socket_bind(&sockaddr, backlog);
		if (fd < 0)
			return -1;
	}
	log4cplus_info("helper listen fd:%d", fd);

	log4cplus_debug(
		"If you want to simulate db busy,"
		"you can set \"ENABLE_SIMULATE_DTC_HELPER_DELAY_SECOND=second\" before "
		"dtc startup");

	init_daemon();

	log4cplus_debug("dbConfig->dstype:%d", dbConfig->dstype);
	// create helper instance base on database type
	switch (dbConfig->dstype) {
	default:
	case 2: {
		// rocksdb
		log4cplus_debug("rocksdb mode entry");
		g_rocksdb_conn = RocksDBConn::instance();
		helper_proc = new RocksdbProcess(g_rocksdb_conn);
		assert(helper_proc);

		int ret = helper_proc->do_init(
			helperArgs.gid, dbConfig,
			TableDefinitionManager::instance()->get_cur_table_def(),
			helperArgs.role);
		if (ret != 0) {
			log4cplus_error("%s", "helper process init failed");
			return -1;
		}

		g_rocksdb_conn->set_key_type(TableDefinitionManager::instance()
						     ->get_cur_table_def()
						     ->key_type());
		ret = g_rocksdb_conn->Open(g_rocksdb_path);
		assert(ret == 0);

		// start direct rocksdb access channel
		ret = create_rocks_domain_dir();
		if (ret != 0)
			return -1;

		ret = rocks_direct_access_proc();
		if (ret != 0)
			return -1;

		break;
	}
	}
	log4cplus_debug("switch end");

	if (usematch)
		helper_proc->use_matched_rows();
#if HAS_LOGAPI
	helper_proc->logapi.do_init(
		gConfig->get_int_val("LogApi", "MessageId", 0),
		gConfig->get_int_val("LogApi", "CallerId", 0),
		gConfig->get_int_val("LogApi", "TargetId", 0),
		gConfig->get_int_val("LogApi", "InterfaceId", 0));
#endif

	helper_proc->init_title(helperArgs.gid, helperArgs.role);
	if (proc_timeout > 1)
		helper_proc->set_proc_timeout(proc_timeout - 1);
	while (!stop) {
		helperArgs.netfd = accept_connection(fd);
		char buf[16];
		memset(buf, 0, 16);
		buf[0] = WATCHDOG_INPUT_OBJECT;
		log4cplus_info("xx77xx11 procName:%s", helper_proc->Name());
		snprintf(buf + 1, 15, "%s", helper_proc->Name());

		log4cplus_info("fork child helper! fd:%d", helperArgs.netfd);

		if (dbConfig->dstype != 2) {
			// mysql
			log4cplus_debug("dstype 2, watch_dog_fork");
			watch_dog_fork(buf, (int (*)(void *))helper_proc_run,
				       (void *)&helperArgs);
			close(helperArgs.netfd);
		} else {
			// rocksdb use multiple thread mode
			std::thread runner(helper_proc_run_rocks, helperArgs);
			runner.detach();
		}
	}

	/* close global rocksdb connection */
	if (g_rocksdb_conn) {
		int ret = g_rocksdb_conn->Close();
		if (ret != 0) {
			log4cplus_error(
				"close rocksdb connection failed, rockspath:%s",
				g_rocksdb_path.c_str());
		}
	}

	log4cplus_info("helper main process exist!");
	if (fd > 0 && addr && addr[0] == '/')
		unlink(addr);
	return 0;
}