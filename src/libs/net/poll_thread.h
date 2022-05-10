#ifndef __POLLTHREAD_H__
#define __POLLTHREAD_H__

#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <linux/unistd.h>
#include <map>

#include "timestamp.h"
#include "poller.h"
#include "timerlist.h"
#include "thread.h"

class PollerBase : public CThread, public CPollerUnit, public CTimerUnit, public CReadyUnit
{
public:
	PollerBase (const char *name);
	virtual ~PollerBase ();

protected:
	int pollTimeout;
	virtual void *Process(void);
	virtual int Initialize(void);

};

#endif
