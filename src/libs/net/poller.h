#ifndef __POLLER_H__
#define __POLLER_H__

#include <arpa/inet.h>
#include <sys/poll.h>
#include "myepoll.h"
#include "list.h"

#define EPOLL_DATA_SLOT(x)	((x)->data.u64 & 0xFFFFFFFF)
#define EPOLL_DATA_SEQ(x)	((x)->data.u64 >> 32)

class CPollerUnit;
class CPollerObject;

struct CEpollSlot {
	CPollerObject *poller;
	uint32_t seq;
	uint32_t freeList;
};

struct CEventSlot
{
    CPollerObject * poller;
};

class CPollerObject {
public:
	CPollerObject (CPollerUnit *o=NULL, int fd=0) :
		ownerUnit(o),
		netfd(fd),
		newEvents(0),
		oldEvents(0),
		epslot(0)
	{
	}

	virtual ~CPollerObject ();

	virtual void InputNotify (void);
	virtual void OutputNotify (void);
	virtual void HangupNotify (void);
	
	void EnableInput(void) {
		newEvents |= EPOLLIN;
	}
	void EnableOutput(void) {
		newEvents |= EPOLLOUT;
	}
	void DisableInput(void) {
		newEvents &= ~EPOLLIN;
	}
	void DisableOutput(void) {
		newEvents &= ~EPOLLOUT;
	}

	void EnableInput(bool i) {
		if(i)
			newEvents |= EPOLLIN;
		else
			newEvents &= ~EPOLLIN;
	}
	void EnableOutput(bool o) {
		if(o)
			newEvents |= EPOLLOUT;
		else
			newEvents &= ~EPOLLOUT;
	}

	int AttachPoller (CPollerUnit *thread=NULL);
	int DetachPoller (void);
	int ApplyEvents ();
	int DelayApplyEvents();
	void ClearDelayApplyEvents() { eventSlot = NULL; }
	int CheckLinkStatus(void);

	void InitPollFd(struct pollfd *);

	friend class CPollerUnit;

protected:
	CPollerUnit *ownerUnit;
	int netfd;
	int newEvents;
	int oldEvents;
	int epslot;
	struct CEventSlot * eventSlot;
};

class CPollerUnit {
public:
	friend class CPollerObject;
	CPollerUnit(int mp);
	//淇敼鍩虹被涓鸿櫄鍑芥暟锛屾敮鎸佸鎬佽浆鎹�by linjinming 2014/5/16
	virtual ~CPollerUnit();

	int SetMaxPollers(int mp);
	int GetMaxPollers(void) const { return maxPollers; }
	int InitializePollerUnit(void);
	int WaitPollerEvents(int);
	void ProcessPollerEvents(void);
	int GetFD(void) { return epfd; }
	int DelayApplyEvents();

private:
	int VerifyEvents(struct epoll_event *);
	int Epctl (int op, int fd, struct epoll_event *events);
	struct CEpollSlot *GetSlot (int n) { return &pollerTable[n];}
	const struct CEpollSlot *GetSlot (int n) const { return &pollerTable[n];}

	void FreeEpollSlot (int n);
	int AllocEpollSlot (void);
	struct CEventSlot * AddDelayEventPoller(CPollerObject *p)
	{
	    if(eventCnt == totalEventSlot)
		return NULL;
	    eventSlot[eventCnt++].poller = p;
	    return &eventSlot[eventCnt - 1];
	}

private:
	struct CEpollSlot *pollerTable;
	struct epoll_event *ep_events;
	int epfd;
	int eeSize;
	int freeSlotList;	
	int maxPollers;
	int usedPollers;
	/* FIXME: maybe too small */
	static int totalEventSlot;
	struct CEventSlot eventSlot[40960];
	int eventCnt;

protected:
	int nrEvents;
};

#endif
