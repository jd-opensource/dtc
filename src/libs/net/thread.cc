/* : set sw=8 ai fdm=marker fmr={,} :*/
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sched.h>

#include "thread.h"
#include "memcheck.h"
#include "log.h"
#include <sched.h>

__attribute__((__weak__))
volatile extern int stop;
CThread::thread_map_t CThread::_thread_map;
pthread_mutex_t CThread::_thread_map_lock = PTHREAD_MUTEX_INITIALIZER;


CThread * CThread::FindThreadByName(const char *name)
{
	CThread::LOCK_THREAD_MAP();
	thread_map_t::const_iterator i = _thread_map.find(name);
	CThread *ret = i == _thread_map.end() ? NULL : i->second;
	CThread::UNLOCK_THREAD_MAP();
	return ret;
}

__attribute__((__weak__))
void DaemonCrashed(int);

#if HAS_TLS 
static __thread CThread *currentThread __TLS_MODEL;

extern "C" void CrashHook(int signo) {
	if(currentThread != NULL)
		currentThread->CrashHook(signo);
}
#endif

void *CThread::Entry (void *thread)
{
	CThread *my = (CThread *)thread;
	my->PrepareInternal();
	void *ret = my->Process();
	my->Cleanup();
	return ret;
}

CThread::CThread (const char *name, int type)
{
	tid = 0;
	pid = 0;
	tasktype = type;
	taskname = STRDUP(name);
	stacksize = 0;
	cpumask = 0;

	stopped = 0;
	stopPtr = &stopped;

	
	pthread_mutex_init (&runlock, NULL);

	CThread::LOCK_THREAD_MAP();
	_thread_map[taskname] = this;
	CThread::UNLOCK_THREAD_MAP();
}

CThread::~CThread ()
{
	pthread_mutex_destroy (&runlock);
	FREE_IF(taskname);
}

void CThread::SetStackSize(int size)
{
	if(size < 0)	// default size
		size = 0;
	else if(size > 64<<20) // max 64M
		size = 64<<20;

	// round to 64K
	if((size & 0xFFFF) != 0)
		size = (size & ~0xFFFF) + 0x10000;
	stacksize = size;
}

CAutoConfig *CThread::g_autoconf = NULL;

void CThread::AutoConfigStackSize(void) {
	unsigned long long size = g_autoconf->GetSizeVal("ThreadStackSize", taskname, stacksize, 'M');

	if(size > (1<<30))
		size = 1<<30;

	SetStackSize((int)size);
	log4cplus_debug("autoconf thread %s ThreadStackSize %d", taskname, stacksize);
}

void CThread::AutoConfigCPUMask(void) {
#ifdef CPU_ZERO
	const char *val = g_autoconf->GetStrVal("ThreadCPUMask", taskname);
	if(val != NULL) {
		cpumask = strtoll(val, NULL, 16);
	}
	log4cplus_debug("autoconf thread %s ThreadCPUMask %llx", taskname, (unsigned long long)cpumask);
#endif
}

void CThread::AutoConfig(void) {
	if(g_autoconf) {
		switch(tasktype) {
			case ThreadTypeSync:
			case ThreadTypeAsync:
				AutoConfigStackSize();
				break;
			default:
				break;
		}
		AutoConfigCPUMask();
		//AutoConfigRemoteLog();
	}
}

int CThread::InitializeThread (void)
{
	log4cplus_info("###deubg init thread");
	int ret = Initialize();
	if(ret < 0)
		return -1;
	switch(tasktype) {
		case 0: // main
			tid = pthread_self ();
			AutoConfig();
#ifdef CPU_ZERO
			if(cpumask != 0) {
				uint64_t temp[2] = {cpumask, 0};
				sched_setaffinity(pid, sizeof(temp), (cpu_set_t *)&temp);
			}
#endif
			break;

		case -1: // world class
			tid = 0;
			break;

		case 1: // async
		case 2: // sync
			AutoConfig();
			pthread_mutex_lock (&runlock);
			pthread_attr_t attr;
			pthread_attr_init(&attr);
			if(stacksize) {
				pthread_attr_setstacksize(&attr, stacksize);
			}
			pthread_create (&tid, &attr, Entry, this);
			break;
	}
	return 0;
}

void CThread::RunningThread ()
{
	log4cplus_info("###deubg runningthread");
	switch(tasktype) {
		case -1:
			// error
			return;
		case 0:
			stopPtr = &stop;
			PrepareInternal();
			Process();
			Cleanup();
			break;
		case 1:
		case 2:

			pthread_mutex_unlock (&runlock);
			break;
	}
}

void CThread::PrepareInternal (void)
{
	pid = gettid();
	log4cplus_info("###deubg internal");
	log4cplus_info("thread %s[%d] started", taskname, pid);
	sigset_t sset;
	sigemptyset(&sset);
	sigaddset(&sset, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sset, &sset);
#ifdef CPU_ZERO
	if(cpumask != 0) {
		uint64_t temp[2] = {cpumask, 0};
		sched_setaffinity(pid, sizeof(temp), (cpu_set_t *)&temp);
	}
#endif
	Prepare();
	pthread_mutex_lock (&runlock);

#if HAS_TLS
	currentThread = this;
#endif
}

void CThread::interrupt (void)
{
    if(this==NULL || stopped || !tid)
	return;

    int ret;
    stopped = 1;
    
    if (tasktype > 0)
    {
        pthread_kill (tid, SIGINT);
		pthread_mutex_unlock (&runlock);

        if ((ret=pthread_join(tid, 0)) != 0)
        {
            log4cplus_warning("Thread [%s] join failed %d.", Name(), ret);
        } else {
	    log4cplus_info("Thread [%s] stopped.", Name());
	}
    }
}

int  CThread::Initialize (void)
{
	return 0;
}

void CThread::Prepare (void)
{
}

void * CThread::Process (void)
{
	while(!Stopping())
		pause();
	return NULL;
}

void CThread::Cleanup (void)
{
}

void CThread::CrashHook (int signo)
{
	// mark mine is stopped
	this->stopped =  1;
	// global stopping
	if(&DaemonCrashed != 0) {
		DaemonCrashed(signo);
	}
	log4cplus_error("Ouch......, I crashed, hang and stopping server");
	pthread_exit(NULL);
	while(1) pause();
}

CThread CThread::TheWorldThread("world", CThread::ThreadTypeWorld);

