#include <stdio.h>
#include <getopt.h>
#include <signal.h>

#include "log.h"
#include "sharding_entry.h"
#include "fulldata_entry.h"
#include "main_entry.h"
#include "cold_wipe_entry.h"
#include "core_entry.h"
#include "agent_entry.h"
#include "proc_title.h"

extern char cache_file[256];
extern char table_file[256];

#define DA_VERSION_MAJOR	1
#define DA_VERSION_MINOR	0
#define DA_VERSION_BUILD	1

#define DA_STRING_HELPER(str)	#str
#define DA_STRING(x)			DA_STRING_HELPER(x)

#define DA_VERSION_STR \
	DA_STRING(DA_VERSION_MAJOR)"."DA_STRING(DA_VERSION_MINOR)"."\
	DA_STRING(DA_VERSION_BUILD)

static int show_help;
static int show_version;
static int load_datalife;
static int load_agent;
static int load_fulldata;
static int load_core;
int recovery_mode;
int load_sharding;
int load_all;

volatile int watchdog_stop = 0;

static struct option long_options[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'v' },
		{ "data-lifecycle", no_argument, NULL,'l' },
		{ "agent", no_argument, NULL,'a' },
		{ "async-connector", no_argument, NULL,'y' },
		{ "sharding", no_argument, NULL,'s' },
		{ "recovery", no_argument, NULL,'r' },
		{ "core", no_argument, NULL,'c' },
		{ NULL, 0, NULL, 0 } };

static char short_options[] = "hvlaycsr";


static int get_options(int argc, char **argv) {
	int c, value;

	opterr = 0;
	if(argc == 1)
		load_all = 1;

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
		case 'v':
			show_version = 1;
			break;
		case 'l':
			load_datalife = 1;
			break;
		case 'a':
			load_agent = 1;
			break;
		case 'y':
			load_fulldata = 1;
			break;
		case 's':
			load_sharding = 1;
			break;		
		case 'r':
			recovery_mode = 1;
			break;					
		case 'c':
			load_core = 1;
			break;				
		default:
			break;
		}

	}
	return 0;
}


static void show_usage(void) {
	printf("Usage: dtc -[hvlaycsr], default load all modules.\n");
	printf("Options:\n"); 
	printf("  -h, --help             		: this help\n");
	printf("  -v, --version          		: show version and exit\n");
	printf("  -a, --agent        			: load agent module\n");
	printf("  -c, --core        			: load dtc core module\n");
	printf("  -l, --data-lifecycle			: load data-lifecycle module\n");
	printf("  -y, --async-connector			: load async-connector module\n");
	printf("  -s, --sharding      			: load sharding module\n");
	printf("  -r, --recovery mode  			: auto restart when crashed\n");

}

int start_full_data(WatchDog* wdog, int delay)
{
	// start full-data connector
	FullDataEntry *fulldata_connector = new FullDataEntry(wdog, delay);
	if (fulldata_connector == NULL) {
		log4cplus_error(
			"create FullDataEntry object failed, msg: %m");
		return -1;
	}

	if (fulldata_connector->new_proc_fork() < 0)
	{
		return -1;
	}

	return 0;
}

int start_sharding(WatchDog* wdog, int delay)
{
	// start full-data connector
	ShardingEntry *sharding = new ShardingEntry(wdog, delay);
	if (sharding == NULL) {
		log4cplus_error(
			"create ShardingEntry object failed, msg: %m");
		return -1;
	}

	if (sharding->new_proc_fork() < 0)
	{
		return -1;
	}

	return 0;
}

int start_data_lifecycle(WatchDog* wdog, int delay)
{
	// start cold-data earse process.
	DataLifeCycleEntry *colddata_connector = new DataLifeCycleEntry(wdog, delay);
	if (colddata_connector == NULL) {
		log4cplus_error(
			"create DataLifeCycleEntry object failed, msg: %m");
		return -1;
	}
	if (colddata_connector->new_proc_fork() < 0)
		return -1;

	return 0;
}

int start_core(WatchDog* wdog, int delay)
{
	// start dtcd core main process.
	CoreEntry *core_entry = new CoreEntry(wdog, delay);
	if (core_entry == NULL) {
		log4cplus_error(
			"create CoreEntry object failed, msg: %m");
		return -1;
	}
	if (core_entry->new_proc_fork() < 0)
		return -1;
	
	return 0;
}

int start_agent(WatchDog* wdog, int delay)
{
	// start agent main process.
	AgentEntry *agent_entry = new AgentEntry(wdog, delay);
	if (agent_entry == NULL) {
		log4cplus_error(
			"create AgentEntry object failed, msg: %m");
		return -1;
	}
	if (agent_entry->new_proc_fork() < 0)
		return -1;
	
	return 0;
}

static void sigterm_handler(int signo)
{
	watchdog_stop = 1;
}

int init_watchdog()
{
	struct sigaction sa;
	sigset_t sset;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = sigterm_handler;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);
	sigaction(SIGQUIT, &sa, NULL);
	sigaction(SIGHUP, &sa, NULL);

	sigemptyset(&sset);
	sigaddset(&sset, SIGTERM);
	sigaddset(&sset, SIGSEGV);
	sigaddset(&sset, SIGBUS);
	sigaddset(&sset, SIGABRT);
	sigaddset(&sset, SIGILL);
	//sigaddset(&sset, SIGCHLD);
	sigaddset(&sset, SIGFPE);
	sigprocmask(SIG_UNBLOCK, &sset, &sset);

	return 0;
}


int main(int argc, char* argv[])
{
	int status;
	WatchDog* wdog = new WatchDog;
	int delay = 5;

	init_log4cplus();
	init_proc_title(argc, argv);

	signal(SIGCHLD, SIG_DFL);

	status = get_options(argc, argv);
	if (status != 0) {
		show_usage();
		exit(1);
	}
	if (show_version) {
		printf("This is dtc watchdog -%s\n", DA_VERSION_STR);
		if (show_help) {
			show_usage();
		}
		exit(0);
	}

	if (load_datalife || load_all) {
		if(start_data_lifecycle(wdog, delay) < 0)
			log4cplus_error("start data_lifecycle failed.");
	}

	if (load_fulldata || load_all) {
		if(start_full_data(wdog, delay) < 0)
			log4cplus_error("start full-data failed.");
	}

	if (load_sharding || load_all) {
		if(start_sharding(wdog, delay) < 0)
			log4cplus_error("start sharding failed.");
	}

	if (load_core || load_all) {
		if(start_core(wdog, delay) < 0)
			log4cplus_error("start core failed.");
		sleep(5);
	}

	if (load_agent || load_all) {
		if(start_agent(wdog, delay) < 0)
			log4cplus_error("start full-data failed.");
	}

	if(init_watchdog() < 0)
	{
		log4cplus_error("init daemon error");
		return -1;
	}

	wdog->run_loop();

	return 0;
}