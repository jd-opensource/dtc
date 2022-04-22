#include "agent_listen_pkg.h"
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include "task_request.h"
#include "config.h"
#include "poll_thread.h"
#include "poll_thread_group.h"
#include "transaction_group.h"
#include "log.h"
#include "pipetask.h"
#include "memcheck.h"
#include "agent_process.h"
#include "net_server.h"
#include "dbconfig.h"
#include "context.h"
#include "waitqueue.h"
#include "transaction_group.h"

Context context;

volatile int crash_signo = 0;
int background = 1;

#define PATH_MAX 1024

const char progname[] = "dbproxy";
const char version[] = ".";
const char usage_argv[] = "";
std::string g_cur_dir;

dbconfig g_dbconfig;

static CAgentListenPkg *agentListener;
static CAgentProcess   *agentProcess;
CNetServerProcess *netserverProcess;

static PollerBase* workerThread;

CTransactionGroup* transactionGroup;

static int Startup_Thread()
{
	
	workerThread = new PollerBase("worker");
	if (workerThread->InitializeThread () == -1)
		return -1;

	netserverProcess = new CNetServerProcess(workerThread);
	agentProcess = new CAgentProcess(workerThread);
	agentListener = new CAgentListenPkg();

	agentProcess->BindDispatcher(netserverProcess);	
	if(agentListener->Bind(g_dbconfig.GetStringValue("ListenAddr").c_str(), agentProcess) < 0)
		return -1;
		
	workerThread->RunningThread();
	agentListener->Run();
	return 0;
}

int GetIdxVal(const char *key,
	const char * const * array, int nDefault)
{
	if(key == NULL)
		return nDefault;

	if(isdigit(key[0]))
	{
		return nDefault;
	}

	for(int i=0; array[i]; i++) {
		if(!strcasecmp(key, array[i]))
			return i;
	}
	return -1;
}

int configInit(void)
{
	char cur_path[PATH_MAX] = {0};
	int rslt = readlink("/proc/self/exe", cur_path, PATH_MAX);
    if (rslt < 0 || rslt >= PATH_MAX)
    {
		log_error("init current dir error");
        return -1;
    }
	g_cur_dir = cur_path;
	g_cur_dir = g_cur_dir.substr(0, g_cur_dir.find_last_of("/"));
	log_info("current directory: %s", g_cur_dir.c_str());

	if(g_dbconfig.InitSystemConfig() == false)
	{
		log_error("InitSystemConfig error");
		return -1;
	}

	std::string loglevel = g_dbconfig.GetStringValue("LogLevel");
	stat_set_log_level_(GetIdxVal(loglevel.c_str(),
			((const char * const []){
				"emerg",
				"alert",
				"crit",
				"error",
				"warning",
				"notice",
				"info",
				"debug",
				NULL }), 7));
				
	if(init_map_table_conf() != 0)
	{
		log_error("init map table conf error");
		return -1;
	}

	return 0;
}

static int DaemonStart()
{
	int ret = background ? daemon (1, 0) : 0;

	return ret;
}

int ProxyStart()
{
	map<string,TableInfo>::iterator iter;
	std::string second_dir = g_cur_dir.substr(0, g_cur_dir.find_last_of("/"));
	std::string port = second_dir.substr(second_dir.find_last_of("/")+1);
	std::string appName = "dbproxy_";
	appName += port;
	appName += "_unix";
	for(iter=g_table_set.begin(); iter!=g_table_set.end(); iter++)
	{
		char tablename[128] = {0};
		char tablepath[256] = {0};
		strcpy(tablename, iter->first.c_str());
		strcpy(tablepath, iter->second.table_path.c_str());
		char *const argv[] = {(char*)appName.c_str(), (char*)"start", tablename, tablepath, NULL};
		if(fork() == 0)
		{
			execv(appName.c_str(), argv);		
		}
	}

	return 0;
}

int InitCompensate()
{
	return 0;
}

void dbproxy_create_pid() {
	ofstream pid_file;
	pid_file.open(context.pid_file.c_str(), ios::out | ios::trunc);
	if (pid_file.is_open()) {
		pid_file << getpid();
		pid_file.close();
	} else {
		log_error("open pid file error. file:%s, errno:%d, errmsg:%s.",
			context.pid_file.c_str(), errno, strerror(errno));
	}
}

void dbproxy_delete_pid() {
	unlink(context.pid_file.c_str());
}

void catch_signal(int32_t signal) {
	switch (signal) {
	case SIGTERM:
		context.stop_flag = true;
		log_error("catch a stop signal.");
		break;
	default:
		break;
	}
}

void dbproxy_run_pre()
{
	if (SIG_ERR == signal(SIGTERM, catch_signal))
		log_info("set stop signal handler error. errno:%d,errmsg:%s.", errno, strerror(errno));
	if (SIG_ERR == signal(SIGINT, catch_signal))
		log_info("set stop signal handler error. errno:%d,errmsg:%s.", errno, strerror(errno));
	signal(SIGPIPE, SIG_IGN);

	dbproxy_create_pid();
}

int start_transaction_thread()
{
	const int thread_num  = g_dbconfig.GetIntValue("TransThreadNum", 10);
	log_debug("transaction thread count:%d", thread_num);

	transactionGroup = new CTransactionGroup(thread_num);
	transactionGroup->Initialize();
	transactionGroup->RunningThread();

	return 0;
}

int main(int argc, char* argv[])
{
	int ret = 0;

	//stat_init_log_("ttcd","../log");
	stat_init_log_("ttcd","/export/Logs");
	log_info("%s v%s: starting....", progname, version);

    if(configInit() == -1)
		return -1;

	if(ProxyStart() < 0)
		return -1;

    if(DaemonStart () < 0)
    	return -1;

	if(InitCompensate() < 0)
		return -1;

	dbproxy_run_pre();

	start_transaction_thread();

    Startup_Thread();
    log_info("%s v%s: running...", progname, version);
    while(!context.stop_flag){
    	sleep(10);
    }

    log_info("%s v%s: stoppping...", progname, version);
	
	if(workerThread)
	{
		workerThread->interrupt();
	}
	
	DELETE(agentListener);
	DELETE(agentProcess);
	DELETE(netserverProcess);
	DELETE(workerThread);

    log_info("%s v%s: stopped", progname, version);

	dbproxy_delete_pid();

    return ret;
}

/* ends here */
