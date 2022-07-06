#ifndef __TIMERLIST_H__
#define __TIMERLIST_H__

#include "list.h"
#include "timestamp.h"

class CTimerObject;
class CTimerUnit;

class CTimerList {
private:
	CListObject<CTimerObject> tlist;
	int timeout;
	CTimerList *next;
	CTimerUnit * timerUnitOwner;
public:
	friend class CTimerUnit;
	friend class CTimerObject;
	CTimerList(int t) : timeout(t), next(NULL) 
	{ }
	CTimerList(int t, CTimerUnit* timerUnitOwner ) : timeout(t), next(NULL),timerUnitOwner(timerUnitOwner) 
	{ 
	}
	int64_t GetTimeUnitNowTime();
	~CTimerList(void) { tlist.FreeList(); }
	int CheckExpired(int64_t now=0);
};

class CTimerUnit {
private:
	CTimerList pending;
	CTimerList *next;

	
	int64_t m_SystemTime;/*系统时间*/
	int64_t m_NowTime;/*应用层时间*/
	int64_t m_TimeOffSet;/*时间拨动后的修正量*/
	
	
public:
	friend class CTimerObject;
	CTimerUnit(void);
	~CTimerUnit(void);
	int64_t GetNowTime()
	{
		return m_NowTime;
	}
	void UpdateNowTime(int max_wait = 0, int interrupted = 0);
	CTimerList *GetTimerListByMSeconds(int);
	CTimerList *GetTimerList(int t) {return GetTimerListByMSeconds(t*1000);}
	int ExpireMicroSeconds(int,int=0); // arg: max/min msec
	int CheckExpired(int64_t now=0);
	int CheckReady(void);
};

class CTimerObject: private CListObject<CTimerObject> {
private:
	int64_t objexp;

public:
	friend class CTimerList;
	friend class CTimerUnit;
	CTimerObject() { }
	virtual ~CTimerObject(void);
	virtual void TimerNotify(void);
	void DisableTimer(void) { ResetList(); }
	void AttachTimer(class CTimerList *o);	
	void AttachReadyTimer(class CTimerUnit *o) { ListMoveTail(o->pending.tlist); }
};

template<class T>
class CTimerMember: public CTimerObject
{
private:
	T *owner;
	virtual void TimerNotify(void) { owner->TimerNotify(); }
public:
	CTimerMember(T *o) : owner(o) {}
	virtual ~CTimerMember() {}
};

class CReadyObject;
class CReadyUnit
{
    private:
	CListObject<CReadyObject> pending1;
	CListObject<CReadyObject> pending2;
	CListObject<CReadyObject> * pending;
	CListObject<CReadyObject> * processing;

    public:
	friend class CReadyObject;
	CReadyUnit() { pending = &pending1; processing = &pending2;}
	virtual ~CReadyUnit() {}

	int CheckReady(uint64_t now);
};

class CReadyObject : private CListObject<CReadyObject>
{   
    public:
	friend class CReadyUnit;
	CReadyObject() {}
	virtual ~CReadyObject() {}

	virtual void ReadyNotify(uint64_t now);
	void AttachReady(CReadyUnit * o) { ListMoveTail(o->pending); }
	void DisableReady() { ResetList(); }
};

#endif
