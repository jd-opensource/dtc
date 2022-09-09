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
*/

#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
//#include "version.h"
#include "da_log.h"
#include "da_util.h"
#include "da_string.h"
#include "da_core.h"
#include "da_buf.h"
#include "da_conf.h"
#include "da_mem_pool.h"
#include "da_signal.h"
#include "da_time.h"
#include "da_listener.h"
#include "da_stats.h"
#include <sys/utsname.h>
#include <sched.h>
#include "../rule/rule.h"

#define DA_VERSION_MAJOR	2
#define DA_VERSION_MINOR	0
#define DA_VERSION_BUILD	0

#define DA_STRING_HELPER(str)	#str
#define DA_STRING(x)			DA_STRING_HELPER(x)

#define DA_VERSION_STR \
	DA_STRING(DA_VERSION_MAJOR)"."DA_STRING(DA_VERSION_MINOR)"."\
	DA_STRING(DA_VERSION_BUILD)

#define DA_VERSION_DETAIL \
	DA_VERSION_STR" built at: "__DATE__" "__TIME__

#define DA_CONF_PATH "../conf/agent.xml"
#define DA_LOG_DIR "../log"

#define DA_LOG_DEFAULT  3
#define DA_MBUF_SIZE DEF_MBUF_SIZE
#define DA_EVENT_INTERVAL (2 * 1000) /* in msec */

extern void _set_remote_log_config_(const char *addr, int port, const char *own_addr, int own_port, int businessid, int iSwitch);

static sig_atomic_t da_reload;
static sig_atomic_t da_exiting;
static sig_atomic_t da_stop = 0;
static uint64_t da_reload_ts;

/* how many ms should we delay since we received reload signal */
#define NC_RELOAD_DELAYED 20000

static int show_help;
static int show_version;
static int test_conf;
static int daemonize;
static int describe_stats;

static struct option long_options[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' },
		{ "test-conf", no_argument, NULL,'t' },
		{ "daemonize", no_argument, NULL, 'd' },
		{ "describe-stats",no_argument, NULL, 'D' },
		{ "verbose", required_argument, NULL, 'v' },
		{ "output", required_argument, NULL, 'o' },
		{ "conf-file",required_argument, NULL, 'c' },
		{ "pid-file", required_argument, NULL, 'p' },
		{ "mbuf-size", required_argument, NULL, 'm' },
		{ "cpu-affinity", required_argument, NULL, 'a' },
		{ NULL, 0, NULL, 0 } };

static char short_options[] = "hVtdDv:o:c:p:m:a:";

static int da_daemonize(int dump_core) {
	int status;
	pid_t pid, sid;
	int fd;

	pid = fork();
	switch (pid) {
	case -1:
		log_error("fork() failed: %s", strerror(errno));
		return -1;

	case 0:
		break;

	default:
		/* parent terminates */
		_exit(0);
	}

	/* 1st child continues and becomes the session leader */

	sid = setsid();
	if (sid < 0) {
		log_error("setsid() failed: %s", strerror(errno));
		return -1;
	}

	if (signal(SIGHUP, SIG_IGN) == SIG_ERR) {
		log_error("signal(SIGHUP, SIG_IGN) failed: %s", strerror(errno));
		return -1;
	}

	pid = fork();
	switch (pid) {
	case -1:
		log_error("fork() failed: %s", strerror(errno));
		return -1;

	case 0:
		break;

	default:
		/* 1st child terminates */
		_exit(0);
	}

	/* 2nd child continues */

	/* change working directory */
	if (dump_core == 0) {
		status = chdir("/");
		if (status < 0) {
			log_error("chdir(\"/\") failed: %s", strerror(errno));
			return -1;
		}
	}

	/* clear file mode creation mask */
	umask(0);

	/* redirect stdin, stdout and stderr to "/dev/null" */

	fd = open("/dev/null", O_RDWR);
	if (fd < 0) {
		log_error("open(\"/dev/null\") failed: %s", strerror(errno));
		return -1;
	}

	status = dup2(fd, STDIN_FILENO);
	if (status < 0) {
		log_error("dup2(%d, STDIN) failed: %s", fd, strerror(errno));
		close(fd);
		return -1;
	}

	status = dup2(fd, STDOUT_FILENO);
	if (status < 0) {
		log_error("dup2(%d, STDOUT) failed: %s", fd, strerror(errno));
		close(fd);
		return -1;
	}

	status = dup2(fd, STDERR_FILENO);
	if (status < 0) {
		log_error("dup2(%d, STDERR) failed: %s", fd, strerror(errno));
		close(fd);
		return -1;
	}

	if (fd > STDERR_FILENO) {
		status = close(fd);
		if (status < 0) {
			log_error("close(%d) failed: %s", fd, strerror(errno));
			return -1;
		}
	}

	return 0;
}

static void set_default_options(struct instance *dai) {
	int status;
	dai->ctx = NULL;

	dai->log_level = DA_LOG_DEFAULT;
	dai->conf_filename = DA_CONF_PATH;
	dai->log_dir = DA_LOG_DIR;
	dai->event_max_timeout = DA_EVENT_INTERVAL;

	status = da_gethostname(dai->hostname, DA_MAXHOSTNAMELEN);
	if (status < 0) {
		log_warning("gethostname failed, ignored: %s", strerror(errno));
		da_snprintf(dai->hostname, DA_MAXHOSTNAMELEN, "unknown");
	}
	dai->hostname[DA_MAXHOSTNAMELEN - 1] = '\0';
	dai->mbuf_chunk_size = DA_MBUF_SIZE;
	dai->pid = (pid_t) -1;
	dai->pid_filename = NULL;
	dai->pidfile = 0;
	dai->argv = NULL;
	dai->stats_interval = STATS_INTERVAL;
	dai->cpumask = -1;
}

static int get_options(int argc, char **argv, struct instance *dai) {
	int c, value;

	opterr = 0;

	for (;;) {
		c = getopt_long(argc, argv, short_options, long_options, NULL);
		if (c == -1) {
			/* no more options */
			break;
		}
		switch (c) {
		case 'h':
			show_version = 1;
			show_help = 1;
			break;
		case 'V':
			show_version = 1;
			break;
		case 't':
			test_conf = 1;
			break;
		case 'd':
			daemonize = 1;
			break;
		case 'D':
			describe_stats = 1;
			show_version = 1;
			break;
		case 'v':
			value = da_atoi(optarg, strlen(optarg));
			if (value < 0) {
				write_stderr("dtcagent: option -v requires a number");
				return -1;
			}
			dai->log_level = value;
			break;
		case 'o':
			dai->log_dir = optarg;
			break;
		case 'c':
			dai->conf_filename = optarg;
			break;
		case 'e':
			value = da_atoi(optarg, strlen(optarg));
			if (value < 0) {
				write_stderr("dtcagent: option -e requires a number");
				return -1;
			}

			dai->event_max_timeout = value;
			break;
		case 'i':
			value = da_atoi(optarg, strlen(optarg));
			if (value < 0) {
				write_stderr("dtcagent: option -i requires a number");
				return -1;
			}
			dai->stats_interval = value;
			break;
		case 'p':
			dai->pid_filename = optarg;
			break;
		case 'm':
			value = da_atoi(optarg, strlen(optarg));
			if (value <= 0) {
				write_stderr("dtcagent: option -m requires a non-zero number");
				return -1;
			}

			if (value < MBUF_MIN_SIZE || value > MBUF_MAX_SIZE) {
				write_stderr("dtcagent: mbuf chunk size must be between %zu and"
						" %zu bytes", MBUF_MIN_SIZE, MBUF_MAX_SIZE);
				return -1;
			}

			dai->mbuf_chunk_size = (size_t) value;
			break;
		case 'a':
			value = da_atoi(optarg, strlen(optarg));
			if (value < 0) {
				write_stderr("dtcagent: option -a requires a non-zero number");
				return -1;
			}
			dai->cpumask = value;
			break;
		case '?':
			switch (optopt) {
			case 'o':
			case 'c':
			case 'p':
				write_stderr("dtcagent: option -%c requires a file name",
						optopt);
				break;

			case 'm':
			case 'v':
				write_stderr("dtcagent: option -%c requires a number", optopt);
				break;

			case 'a':
				write_stderr("dtcagent: option -%c requires a string", optopt);
				break;

			default:
				write_stderr("dtcagent: invalid option -- '%c'", optopt);
				break;
			}
			return -1;

		default:
			write_stderr("dtcagent: invalid option -- '%c'", optopt);
			return -1;
		}

	}
	return 0;
}

static void show_usage(void) {
	write_stderr(
			"Usage: dtcagent [-?hVdDt] [-v verbosity level] [-o output file]" CRLF
			"                  [-c conf file] [-p pid file] [-m mbuf size] [-a cpu affinity]" CRLF
			"");
	write_stderr(
			"Options:" CRLF
			"  -h, --help             		: this help" CRLF
			"  -V, --version          		: show version and exit" CRLF
			"  -t, --test-conf        		: test configuration for syntax errors and exit" CRLF
			"  -d, --daemonize        		: run as a daemon" CRLF
			"  -D, --describe-stats   : print stats description and exit");
	write_stderr(
			"  -v, --verbosity=N      		: set logging level (default: 3, min: 1, max: 7)" CRLF
			"  -o, --output=S         		: set logging dir (default: ../log/)" CRLF
			"  -c, --conf-file=S      		: set configuration file (default: ../conf/agent.xml)" CRLF
			"  -e, --event-max-timeout=S	: set epoll max timeout(ms)(default: (30*1000)ms)" CRLF
			"  -i, --stats_interval=S	    : set stats aggregator interval(ms)(default: (10*1000)ms)" CRLF
			"  -p, --pid-file=S       		: set pid file (default: off)" CRLF
			"  -m, --mbuf-size=N      		: set size of mbuf chunk in bytes (default: 16384 bytes)" CRLF
			"  -a, --bind-cpu-mask=S        : set processor bind cpu(default: no bind)" CRLF
			"");

}

static bool da_test_conf(struct instance *dai) {
	struct conf *cf;

	cf = conf_create(dai->conf_filename);
	if (cf == NULL) {
		write_stderr("dtcagent: configuration file '%s' syntax is invalid",
				dai->conf_filename);
		return false;
	}

	conf_destroy(cf);

	write_stderr("dtcagent: configuration file '%s' syntax is ok",
			dai->conf_filename);
	return true;
}

static void da_log_init(struct instance *dai) {
	_init_log_("agent", dai->log_dir);
	_set_log_level_(dai->log_level);
	_set_remote_log_fd_();
}

static int da_create_pidfile(struct instance *dai) {
	char pid[DA_UINTMAX_MAXLEN];
	int fd, pid_len;
	ssize_t n;

	fd = open(dai->pid_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		log_error("opening pid file '%s' failed: %s", dai->pid_filename,
				strerror(errno));
		return -1;
	}
	dai->pidfile = 1;

	pid_len = da_snprintf(pid, DA_UINTMAX_MAXLEN, "%d", dai->pid);

	n = da_write(fd, pid, pid_len);
	if (n < 0) {
		log_error("write to pid file '%s' failed: %s", dai->pid_filename,
				strerror(errno));
		return -1;
	}

	close(fd);

	return 0;
}

void dump(struct sig_handler *sh) {
	/* dump memory usage then free everything possible */
	dump_pools();
	pool_gc();
}

void reload(struct sig_handler *sh) {
	if (da_reload || da_exiting || da_stop) {
		log_alert("reloading or shut down is in processing ,please wait");
		return;
	}
	da_reload = 1;
	log_alert("reloading signal been received,please wait");
	return;
}

void stop(struct sig_handler *sh)
{
	if(da_reload || da_exiting || da_stop)
	{
		log_alert("reloading or shut down is in processing ,please wait");
		return;
	}
	da_stop = 1;
	log_alert("stoping is in processing,please wait");
	return;
}

static void da_print_run(struct instance *dai) {
	int status;
	struct utsname name;

	status = uname(&name);
	if (status < 0) {
		log_alert("dtcagent-%s started on pid %d", DA_VERSION_DETAIL, dai->pid);
	} else {
		log_alert("dtcagent-%s built for %s %s %s started on pid %d",
				DA_VERSION_DETAIL, name.sysname, name.release, name.machine,
				dai->pid);
	}
	return;
}

static void da_print_done(void) {
	log_alert("dtcagent-%s stoped!", DA_VERSION_STR);
}

static int da_set_sched_affinity(int run_cupmask)
{
	cpu_set_t set;
	
	CPU_ZERO(&set);
	CPU_SET(run_cupmask,&set);	

	int ret = sched_setaffinity(0,sizeof(cpu_set_t),&set);
	if(ret == -1)
	{
		log_error("set cpu affinity failed");
	}
	return ret;
}

static int da_pre_run(struct instance *dai) {
	int status;
	struct sig_handler *sh;
	char dtckey[50] = {0};
	//init system time
	tv_update_date(-1, -1);
	//init log
	da_log_init(dai);

	log_info("DTC AGENT init.");

#if 0
	if(re_load_table_key(dtckey) < 0)
	{
		log_error("load dtc define error.");
		return -1;
	}
#endif

	if (daemonize) {
		status = da_daemonize(1);
		if (status != 0) {
			return -1;
		}
	}
	if(dai->cpumask != -1)
		da_set_sched_affinity(dai->cpumask);
	//init signal process pool and queue
	status = signal_init();
	if (status < 0) {
		log_error("init sinal pool error");
		return status;
	}
	//register signal process function SIGQUIT----> dump memory pool
	sh = signal_register_fct(SIGQUIT, dump, SIGQUIT);
	if (sh == NULL) {
		log_error("set catch SIGQUIT fail!");
		return -1;
	}
	sh = signal_register_fct(SIGUSR1, reload, SIGUSR1);
	if (sh == NULL) {
		log_error("set catch SIGQUIT fail!");
		return -1;
	}
	sh = signal_register_fct(SIGTERM, stop, SIGTERM);
	if (sh == NULL) {
		log_error("set catch SIGTERM fail!");
		return -1;
	}
	//register SIGPIPE\SIGUSR2 without function
	sh = signal_register_fct(SIGPIPE, NULL, 0);
	sh = signal_register_fct(SIGUSR2, NULL, 0);
	sh = signal_register_fct(SIGINT, NULL, 0);
	sh = signal_register_fct(SIGHUP, NULL, 0);
	sh = signal_register_fct(SIGTTOU, NULL, 0);
	sh = signal_register_fct(SIGTTIN, NULL, 0);
	sh = signal_register_fct(SIGCHLD, NULL, 0);

	dai->pid = getpid();

	if (dai->pid_filename) {
		status = da_create_pidfile(dai);
		if (status != 0) {
			return status;
		}
	}

	da_print_run(dai);
	return 0;
}

static void da_remove_pidfile(struct instance *dai) {
	int status;

	status = unlink(dai->pid_filename);
	if (status < 0) {
		log_error("unlink of pid file '%s' failed, ignored: %s",
				dai->pid_filename, strerror(errno));
	}
}

static void da_post_run(struct instance *dai) {
	if (dai->pidfile) {
		da_remove_pidfile(dai);
	}

	deinit_signals();
	tv_update_date(0, 1);
	da_print_done();
}

static void da_run(struct instance *dai) {
	int status;
	struct context *ctx;
	struct conn* c = NULL;

	ctx = core_start(dai);
	if (ctx == NULL) {
		return;
	}
	core_cleanup_inherited_socket();
	_set_log_switch_(dai->ctx->cf->stCL.log_switch);

	/*
	  own_addr 取本地IP
	  own_port, bid 只取server_pool的第一个
	  _set_remote_log_config_放在此处初始化会导致在此之前的log不会被远程发送
	*/
	if(array_n(&(dai->ctx->pool)) > 0)
	{
		struct server_pool *sp = (struct server_pool *)array_top(&(dai->ctx->pool));
		const char *pPort = strchr((char *)sp->addrstr.data, ':');
		if(NULL != pPort)
		{
			int iPort = da_atoi(pPort + 1, strlen(pPort + 1));
			char szBid[9] = {0};
			strncpy(szBid, (char *)sp->accesskey.data, 8);
			szBid[8] = '\0';
			int iBid = 0;
			sscanf(szBid, "%d", &iBid);
			_set_remote_log_config_((char *)dai->ctx->cf->stCL.remote_log_ip.data, dai->ctx->cf->stCL.remote_log_port, dai->ctx->cf->localip, iPort, iBid, dai->ctx->cf->stCL.remote_log_switch);
		}
	}
	else
		return ;

	tv_update_date(0, 1);

	/* run rabbit run */
	while (!da_stop) {
		status = core_loop(dai->ctx);
		if (status != 0) {
			break;
		}
		//reload start
		if (da_reload) {
			core_setinst_status(RELOADING);
			log_alert("reload start..........");
			da_reload = 0;
			da_reload_ts = now_ms;
			//fork fail,do nothing
			status = core_exec_new_binary(dai);
			if (status != 0) {
				core_setinst_status(NORMAL);
				log_error("reload fail,please check error log...");
			} else {
				/* for parent: close listen fd here */
				listener_deinit(dai->ctx);
				core_setinst_status(EXITING);
				/* do not remove pid file when reload */
				dai->pidfile = 0;
				da_exiting = 1;
			}
		}
		//reload exit
		if (da_exiting) {
			log_alert("waiting for client close all connections, will shut down soon");
			if (now_ms - da_reload_ts > NC_RELOAD_DELAYED) {
				log_alert("process exit for reload,reload success");
				break;
			}
		}
	}
	core_setinst_status(EXITED);
	core_stop(dai->ctx);
}

int main(int argc, char **argv) {

	int status;
	struct instance dai;

	set_default_options(&dai);

	dai.argv = argv;

	status = get_options(argc, argv, &dai);
	if (status != 0) {
		show_usage();
		exit(1);
	}
	if (show_version) {
		printf("This is dtcagent-%s" CRLF, DA_VERSION_STR);
		if (show_help) {
			show_usage();
		}

		if (describe_stats) {
			stats_describe();
		}
		exit(0);
	}
	if (test_conf) {
		if (!da_test_conf(&dai)) {
			exit(1);
		}
		exit(0);
	}

	status = da_pre_run(&dai);
	if (status < 0) {
		da_post_run(&dai);
		exit(1);
	}

	da_run(&dai);
	da_post_run(&dai);

	return EXIT_SUCCESS;
}
