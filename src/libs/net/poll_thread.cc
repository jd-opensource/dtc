/* : set sw=8 ai fdm=marker fmr={,} :*/
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

#include "poll_thread.h"
#include "memcheck.h"
#include "poller.h"
#include "myepoll.h"
#include "log.h"
#include <sched.h>
#include "timemanager.h"

volatile extern int stop;

PollerBase::PollerBase (const char *name) :
	CThread(name, CThread::ThreadTypeAsync), CPollerUnit(1000), pollTimeout(2000)
{
}

PollerBase::~PollerBase ()
{
}

int PollerBase::Initialize(void)
{
	if(CThread::g_autoconf != NULL) {
		int mp0 = GetMaxPollers();
		int mp1;
		mp1 = g_autoconf->GetIntVal("MaxIncomingPollers", Name(), mp0);
		log_debug("autoconf thread %s MaxIncomingPollers %d", taskname, mp1);
		if(mp1 > mp0) {
			SetMaxPollers(mp1);
		}
	}
	if(InitializePollerUnit() < 0)
		return -1;
	return 0;
}

void * PollerBase::Process (void)
{
	log_info("###deubg pollthread process");
	while (!Stopping())
	{
		// if previous event loop has no events,
		// don't allow zero epoll wait time
		int timeout = ExpireMicroSeconds(pollTimeout, nrEvents==0);		
		int interrupted = WaitPollerEvents(timeout);		
		UpdateNowTime(timeout, interrupted);
		TimeManager::Instance().AdjustDay();
		if (Stopping())
			break;

		ProcessPollerEvents();
		CheckExpired(GetNowTime());
		CTimerUnit::CheckReady();
		CReadyUnit::CheckReady(GetNowTime());
		DelayApplyEvents();
	}
	return 0;
}

