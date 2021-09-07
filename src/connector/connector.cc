#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sched.h>

#include <ttc_global.h>
#include <version.h>
#include <proctitle.h>
#include <log.h>
#include <config.h>
#include "DBProcess.h"
#include "cache_pool.h"
#include <daemon.h>
#include <listener.h>
#include <sockaddr.h>
#include <unix_socket.h>
#include <watchdog_listener.h>
#include "dtcutils.h"
extern void _set_remote_log_config_(const char *addr, int port, int businessid);
const char progname[] = "mysql-helper";
const char usage_argv[] = "machId addr [port]";
char cacheFile[256] = CACHE_CONF_NAME;
char tableFile[256] = TABLE_CONF_NAME;

static CHelperProcess *helperProc;
static unsigned int procTimeout;

int targetNewHash;
int hashChanging;

static int SyncDecode(CTask *task, int netfd, CHelperProcess *helperProc) {
	CSimpleReceiver receiver(netfd);
	int code;
	do {
		code = task->Decode(receiver);
		if(code==DecodeFatalError) {
		    if(errno != 0)
			log_notice ("decode fatal error, fd=%d, %m", netfd);
		    return -1;
		}
		if(code==DecodeDataError) {
		    if(task->ResultCode() == 0 || task->ResultCode() == -EC_EXTRA_SECTION_DATA) // -EC_EXTRA_SECTION_DATA   verify package 
			    return 0;
		    log_notice ("decode error, fd=%d, %d", netfd, task->ResultCode());
		    return -1;
		}
		helperProc->SetTitle("Receiving...");
	} while (!stop && code != DecodeDone);

	if(task->ResultCode() < 0) {
		log_notice ("register result, fd=%d, %d", netfd, task->ResultCode());
		return -1;
	}
	return 0;
}

static int SyncSend(CPacket *reply, int netfd) {
	int code;
	do {
		code = reply->Send(netfd);
		if(code==SendResultError)
		{
		    log_notice ("send error, fd=%d, %m", netfd);
		    return -1;
		}
	} while (!stop && code != SendResultDone);

	return 0;
}


static void alarm_handler(int signo) {
	if(background==0 && getppid()==1)
		exit(0);
	alarm(10);
}

static int AcceptConnection(int fd) {
	helperProc->SetTitle("listener");
	signal(SIGALRM, alarm_handler);
	while(!stop) {
		alarm(10);
		int newfd;
		if((newfd = accept(fd, NULL, 0)) >= 0) {
			alarm(0);
			return newfd;
		}
		if(newfd < 0 && errno == EINVAL){
			if(getppid() == (pid_t)1){ // 父进程已经退出
				log_error ("ttc father process not exist. helper[%d] exit now.", getpid());
				exit(0);
			}
			usleep(10000);
		}
	}
	exit(0);
}

static void proc_timeout_handler(int signo) {
	log_error ("mysql process timeout(more than %u seconds), helper[pid: %d] exit now.", procTimeout, getpid());
	exit(-1);
}


#ifdef  __DEBUG__
static void inline simulate_helper_delay(void)
{
	char *env = getenv("ENABLE_SIMULATE_TTC_HELPER_DELAY_SECOND");
	if(env && env[0] != 0) 
	{
		unsigned delay_sec = atoi(env);
		if(delay_sec > 5)
			delay_sec = 5;

		log_debug("simulate ttc helper delay second[%d s]", delay_sec);
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

static int HelperProcRun (struct THelperProcParameter *args)
{
	// close listen fd
	close(0);
	open("/dev/null", O_RDONLY);

	helperProc->SetTitle("Initializing...");

	if(procTimeout > 0)
		signal(SIGALRM, proc_timeout_handler);
	
	alarm(procTimeout);
	if (helperProc->Init(args->gid, dbConfig, CTableDefinitionManager::Instance()->GetCurTableDef(), args->role) != 0)
	{
		log_error ("%s", "helper process init failed");
		exit(-1);
	} 

	helperProc->InitPingTimeout();
	alarm(0);

	
	
	_set_remote_log_config_( gConfig->GetStrVal("cache", "RemoteLogAddr"),
   	                                      gConfig->GetIntVal("cache", "RemoteLogPort", 0),  
   	                                      dtc::utils::GetBID() );
	
	_set_remote_log_fd_();

	hashChanging = gConfig->GetIntVal("cache", "HashChanging", 0);
	targetNewHash = gConfig->GetIntVal("cache", "TargetNewHash", 0);

	unsigned int timeout;
	
	while(!stop) {
		helperProc->SetTitle("Waiting...");
		CTask *task = new CTask(CTableDefinitionManager::Instance()->GetCurTableDef());
		if(SyncDecode(task, args->netfd, helperProc) < 0) {
			delete task;
			break;
		}

		if(task->ResultCode() == 0){
			switch(task->RequestCode()){
			case DRequest::Insert:
			case DRequest::Update:
			case DRequest::Delete:
			case DRequest::Replace:
			case DRequest::ReloadConfig:
				timeout = 2*procTimeout;
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
		CPacket *reply = new CPacket;
		reply->EncodeResult(task);
		
		if(SyncSend(reply, args->netfd) <0) {
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
	DaemonCleanup();
#if MEMCHECK
	log_info("%s v%s: stopped", progname, version);
	dump_non_delete();
	log_debug("memory allocated %lu virtual %lu", count_alloc_size(), count_virtual_size());
#endif
	exit(0);
	return 0;
}

int check_db_version(void) {
	int ver = CDBConn::ClientVersion();
	if(ver == MYSQL_VERSION_ID) return 0;
	log_warning("MySql version mismatch: header=%d.%d.%d lib=%d.%d.%d",
		MYSQL_VERSION_ID/10000, (MYSQL_VERSION_ID/100)%100, MYSQL_VERSION_ID % 100,
		ver/10000, (ver/100)%100, ver%100);
	return -1;
}

int check_db_table(int gid, int role)
{
	CHelperProcess* helper = new CHelperProcess ();
	
	if(procTimeout > 1){
		helper->SetProcTimeout(procTimeout-1);
		signal(SIGALRM, proc_timeout_handler);
	}
	
	alarm(procTimeout);
	if (helper->Init(gid, dbConfig, CTableDefinitionManager::Instance()->GetCurTableDef(), role) != 0)
	{
		log_error ("%s", "helper process init failed");
		delete helper;
		alarm(0);
		return(-1);
	} 

	if(helper->CheckTable()!=0){
		delete helper;
		alarm(0);
		return(-2);
	}
	
	alarm(0);
	delete helper;
	
	return(0);
}

int main(int argc, char **argv)
{
	init_proc_title(argc, argv);
	if(TTC_DaemonInit (argc, argv) < 0)
		return -1;

	check_db_version();
	argc -= optind;
	argv += optind;

	struct THelperProcParameter helperArgs = { 0, 0, 0 };
	char *addr = NULL;

	if(argc > 0)
	{
		char *p;
		helperArgs.gid = strtol(argv[0], &p, 0);
		if(*p=='\0' || *p==MACHINEROLESTRING[0])
			helperArgs.role = 0;
		else if(*p==MACHINEROLESTRING[1])
			helperArgs.role = 1;
		else {
			log_error("Bad machine id: %s", argv[0]);
			return -1;
		}
	}

	if(argc != 2 && argc != 3)
	{
		ShowUsage();
		return -1;
	}

	int usematch = gConfig->GetIntVal("cache", "UseMatchedAsAffectedRows", 1);
	int backlog = gConfig->GetIntVal("cache", "MaxListenCount", 256);
	int helperTimeout = gConfig->GetIntVal("cache", "HelperTimeout", 30);
	if(helperTimeout > 1)
		procTimeout = helperTimeout -1;
	else
		procTimeout = 0;
	addr = argv[1];

	if(dbConfig->checkTable && check_db_table(helperArgs.gid, helperArgs.role)!=0){
		return -1;
	}
	
	int fd = -1;
	if(!strcmp(addr, "-"))
		fd = 0;
	else {
		if(strcasecmp(gConfig->GetStrVal("cache", "CacheShmKey")?:"", "none") != 0)
		{
			log_warning("standalone %s need CacheShmKey set to NONE", progname);
			return -1;
		}

		CSocketAddress sockaddr;
		const char *err = sockaddr.SetAddress(addr, argc==2 ? NULL : argv[2]);
		if(err) {
			log_warning("host %s port %s: %s", addr, argc==2 ? "NULL" : argv[2], err);
			return -1;
		}
		if(sockaddr.SocketType() != SOCK_STREAM) {
			log_warning("standalone %s don't support UDP protocol", progname);
			return -1;
		}
		fd = SockBind(&sockaddr, backlog);
		if(fd < 0)
			return -1;
	}

	log_debug("If you want to simulate db busy,"
			"you can set \"ENABLE_SIMULATE_TTC_HELPER_DELAY_SECOND=second\" before ttc startup");

	DaemonStart();

	helperProc = new CHelperProcess ();
	if(usematch)
		helperProc->UseMatchedRows();
#if HAS_LOGAPI
	helperProc->logapi.Init(
			gConfig->GetIntVal("LogApi", "MessageId", 0),
			gConfig->GetIntVal("LogApi", "CallerId", 0),
			gConfig->GetIntVal("LogApi", "TargetId", 0),
			gConfig->GetIntVal("LogApi", "InterfaceId", 0)
	);
#endif

	helperProc->InitTitle(helperArgs.gid, helperArgs.role);
	if(procTimeout > 1)
		helperProc->SetProcTimeout(procTimeout-1);
	while(!stop) {
		helperArgs.netfd = AcceptConnection(fd);
		char buf[16];
		memset(buf, 0, 16);
		buf[0] = WATCHDOG_INPUT_OBJECT;
		snprintf(buf + 1, 15, "%s", helperProc->Name());
		WatchDogFork(buf, (int(*)(void*))HelperProcRun, (void *)&helperArgs);

		close(helperArgs.netfd);
	}

	if(fd > 0 && addr && addr[0]=='/')
		unlink(addr);
	return 0;
}

