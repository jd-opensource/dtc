
#include <stdint.h>
#include "timerlist.h"
#include "log.h"
#if TIMESTAMP_PRECISION < 1000
#error TIMESTAMP_PRECISION must >= 1000
#endif

#ifndef likely
#if __GCC_MAJOR >= 3
#define likely(x)	__builtin_expect(!!(x), 1)
#define unlikely(x)	__builtin_expect(!!(x), 0)
#else
#define likely(x)	(x)
#define unlikely(x)	(x)
#endif
#endif

CTimerObject::~CTimerObject(void) {
}

void CTimerObject::TimerNotify(void) {
	log_debug("CTimerObject::TimerNotify start");
	delete this;
}

void CTimerObject::AttachTimer(class CTimerList *lst) {
	if(lst->timeout > 0)
		objexp = lst->GetTimeUnitNowTime() + lst->timeout * (TIMESTAMP_PRECISION/1000);
	ListMoveTail(lst->tlist);
}

int CTimerList::CheckExpired(int64_t now) {
	int n = 0;
	if(now==0) {
		now = GetTimeUnitNowTime();
	}
	while(!tlist.ListEmpty()) {
		CTimerObject *tobj = tlist.NextOwner();	
		if(tobj->objexp > now) break;
		tobj->ListDel();
		tobj->TimerNotify();
		n++;
	}
	return n;
}

int64_t CTimerList::GetTimeUnitNowTime()
{
	if (NULL == timerUnitOwner)
	{
		return GET_TIMESTAMP();
	}
	return timerUnitOwner->GetNowTime();
}

void CTimerUnit::UpdateNowTime(int max_wait, int interrupted)
{
	int64_t adjustTime = 0;
	int64_t deadLineTime = 0;
	const int MAX_DELAY_MS = 1000000;/*前向拨动最多量*/
	m_SystemTime = GET_TIMESTAMP();
	
	if ( unlikely(max_wait < 0) )
	{
		m_TimeOffSet = 0;		
		m_NowTime = m_SystemTime;
		return;
	}
	
	adjustTime = m_SystemTime + m_TimeOffSet;	
	/*时间被向后拨动了*/
	if (adjustTime < m_NowTime)
	{
		 adjustTime = interrupted ? (m_NowTime) : (m_NowTime + max_wait *  (TIMESTAMP_PRECISION/1000));
		 m_TimeOffSet = adjustTime - m_SystemTime;
		 m_NowTime = adjustTime;		
		 return;
	}
	
	deadLineTime = m_NowTime + max_wait *  (TIMESTAMP_PRECISION/1000) + MAX_DELAY_MS;	
	if (likely(adjustTime < deadLineTime) )
	{
		m_NowTime = adjustTime;	
		return;
	}
	/*时间被向前拨动了*/
	else
	{
		adjustTime = interrupted ? (m_NowTime) : (m_NowTime + max_wait  *  (TIMESTAMP_PRECISION/1000));
		m_TimeOffSet = adjustTime - m_SystemTime;
		m_NowTime = adjustTime;	
		return;
	}
}

CTimerList *CTimerUnit::GetTimerListByMSeconds(int to) {
	CTimerList *tl;

	for(tl = next; tl; tl=tl->next) {
		if(tl->timeout == to)
			return tl;
	}
	tl = new CTimerList(to, this);
	tl->next = next;
	next = tl;
	return tl;
}

CTimerUnit::CTimerUnit(void) : pending(0), next(NULL),m_TimeOffSet(0)
{
	m_SystemTime = GET_TIMESTAMP();
	m_NowTime = m_SystemTime;
};

CTimerUnit::~CTimerUnit(void) {
	while(next) {
		CTimerList *tl = next;
		next = tl->next;
		delete tl;
	}
};

int CTimerUnit::ExpireMicroSeconds(int msec, int msec0) {
	int64_t exp;	
	CTimerList *tl;
	int64_t timestamp = GetNowTime() ;
	exp = timestamp + msec*(TIMESTAMP_PRECISION/1000);
	
	for(tl = next; tl; tl=tl->next) {
		if(tl->tlist.ListEmpty())
			continue;
		CTimerObject *o = tl->tlist.NextOwner();
		
		if(o->objexp < exp)
			exp = o->objexp;
	}	
	exp -= timestamp;
	if(exp <= 0)
		return 0;
	msec = exp / (TIMESTAMP_PRECISION/1000);
	return msec >= msec0 ? msec : msec0;
}

int CTimerUnit::CheckReady(void) {
	int n = 0;
	while(!pending.tlist.ListEmpty()) {
		CTimerObject *tobj = pending.tlist.NextOwner();
		tobj->ListDel();
		tobj->TimerNotify();
		n++;
	}
	return n;
}

int CTimerUnit::CheckExpired(int64_t now) {
	if(now==0)
		now = GetNowTime();

	int n = CheckReady();;
	CTimerList *tl;
	for(tl = next; tl; tl=tl->next) {
		n += tl->CheckExpired(now);
	}
	return n;
}

void CReadyObject::ReadyNotify(uint64_t now)
{
        delete this;
}

int CReadyUnit::CheckReady(uint64_t now)
{
    int n = 0;
    CListObject<CReadyObject> * tmp;

    tmp = pending;
    pending = processing;
    processing = tmp;

    while(!processing->ListEmpty())
    {
	CReadyObject * robj = processing->NextOwner();
	robj->ListDel();
	robj->ReadyNotify(now);
	n++;
    }
    return n;
}
