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
#include "cm_load.h"
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

ConfigHelper  g_config;

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
	if(agentListener->Bind(g_config.GetStringValue("ListenAddr").c_str(), agentProcess) < 0)
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

int init_config(void)
{
	//load cold and hot db config.
	if(g_config.load_dtc_config() == false)
	{
		log4cplus_error("load db config error");
		return -1;
	}

	return 0;
}

static int DaemonStart()
{
	int ret = background ? daemon (1, 0) : 0;

	return ret;
}

void dbproxy_create_pid() {
	ofstream pid_file;
	pid_file.open(DEF_PID_FILE, ios::out | ios::trunc);
	if (pid_file.is_open()) {
		pid_file << getpid();
		pid_file.close();
	} else {
		log4cplus_error("open pid file error. file:%s, errno:%d, errmsg:%s.",
			DEF_PID_FILE, errno, strerror(errno));
	}
}

void dbproxy_delete_pid() {
	unlink(DEF_PID_FILE);
}

void catch_signal(int32_t signal) {
	switch (signal) {
	case SIGTERM:
		context.stop_flag = true;
		log4cplus_error("catch a stop signal.");
		break;
	default:
		break;
	}
}

void dbproxy_run_pre()
{
	if (SIG_ERR == signal(SIGTERM, catch_signal))
		log4cplus_info("set stop signal handler error. errno:%d,errmsg:%s.", errno, strerror(errno));
	if (SIG_ERR == signal(SIGINT, catch_signal))
		log4cplus_info("set stop signal handler error. errno:%d,errmsg:%s.", errno, strerror(errno));
	signal(SIGPIPE, SIG_IGN);

	dbproxy_create_pid();
}

int start_db_thread_group(DBHost* dbconfig)
{
	const int thread_num  = g_config.GetIntValue("TransThreadNum", 10);
	log4cplus_debug("transaction thread count:%d", thread_num);

	transactionGroup = new CTransactionGroup(thread_num);
	if(transactionGroup->Initialize(dbconfig))
	{
		log4cplus_error("init thread group failed");
		return -1;
	}

	transactionGroup->RunningThread();

	return 0;
}

int main(int argc, char* argv[])
{
	int ret = 0;

	init_log4cplus();
	log4cplus_info("%s v%s: starting....", progname, version);

    if(init_config() == -1)
		return -1;

    if(DaemonStart () < 0)
    	return -1;

	dbproxy_run_pre();

	if(start_db_thread_group(&g_config.full_instance))
	{
		log4cplus_error("start fulldb thread group faield, exit.");
		return -1;
	}

	if(start_db_thread_group(&g_config.hot_instance))
	{
		log4cplus_error("start hotdb thread group faield, exit.");
		return -1;
	}

    Startup_Thread();
    log4cplus_info("%s v%s: running...", progname, version);
    while(!context.stop_flag){
    	sleep(10);
    }

    log4cplus_info("%s v%s: stoppping...", progname, version);
	
	if(workerThread)
	{
		workerThread->interrupt();
	}
	
	DELETE(agentListener);
	DELETE(agentProcess);
	DELETE(netserverProcess);
	DELETE(workerThread);

    log4cplus_info("%s v%s: stopped", progname, version);

	dbproxy_delete_pid();

    return ret;
}

/* ends here */
