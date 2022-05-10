#ifndef __GENERICTHREAD_H__
#define __GENERICTHREAD_H__

#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <linux/unistd.h>

#include "timestamp.h"
#include "poller.h"
#include "timerlist.h"
#include "config.h"

class CThread
{
public:
	enum {
		ThreadTypeWorld = -1,
		ThreadTypeProcess = 0,
		ThreadTypeAsync = 1,
		ThreadTypeSync = 2
	};
public:
	static CAutoConfig *g_autoconf;
	static void SetAutoConfigInstance(CAutoConfig *ac) { g_autoconf = ac; };
public:
	CThread (const char *name, int type=0);
	virtual ~CThread ();

	int InitializeThread ();
	void RunningThread ();
	const char *Name(void) { return taskname; }
	int Pid(void) const { return stopped ? 0 : pid; }
	void SetStackSize(int);
	int Stopping(void) { return *stopPtr; }
	virtual void interrupt (void);
	virtual void CrashHook(int);

	CThread *TheWorld(void) { return &TheWorldThread; };

protected:
	char *taskname;
	pthread_t tid;
	pthread_mutex_t runlock;
	int stopped;
	volatile int *stopPtr;
	int tasktype; // 0-->main, 1--> epoll, 2-->sync, -1, world
	int stacksize;
	uint64_t cpumask;
	int pid;

protected:
	virtual int Initialize(void);
	virtual void Prepare(void);
	virtual void *Process(void);
	virtual void Cleanup(void);

protected:
	struct cmp
	{
		bool operator()(const char * const &a, const char * const &b) const
		{ return strcmp(a, b) < 0; }
	};
	typedef std::map<const char *, CThread *, cmp> thread_map_t;
	static thread_map_t _thread_map;
	static pthread_mutex_t _thread_map_lock;

	static void LOCK_THREAD_MAP() { pthread_mutex_lock(&_thread_map_lock); }
	static void UNLOCK_THREAD_MAP() { pthread_mutex_unlock(&_thread_map_lock); }
public:
	static CThread * FindThreadByName(const char *name);

private:
	static CThread TheWorldThread;
	static void *Entry (void *thread);
	void PrepareInternal(void);
	void AutoConfig(void);
	void AutoConfigStackSize(void);
	void AutoConfigCPUMask(void);
};

#endif
