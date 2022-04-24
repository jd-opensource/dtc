#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>
#include "memcheck.h"

#include "poll_thread.h"
#include "poller.h"
#include "log.h"

CPollerObject::~CPollerObject ()
{
	if (ownerUnit && epslot)
		ownerUnit->FreeEpollSlot (epslot);

	if (netfd > 0) {
		log4cplus_debug("%d fd been closed!",netfd);
		close (netfd);
		netfd = 0;
	}
	if(eventSlot) 
	{       
	    eventSlot->poller = NULL;
	    eventSlot = NULL;
	} 
}

int CPollerObject::AttachPoller (CPollerUnit *unit)
{
	if(unit) {
		if( ownerUnit==NULL)
			ownerUnit = unit;
		else
			return -1;
	}
	if(netfd < 0)
		return -1;

	if(epslot <= 0) {
		if (!(epslot = ownerUnit->AllocEpollSlot ()))
			return -1;
		struct CEpollSlot *slot = ownerUnit->GetSlot(epslot);
		slot->poller = this;

		int flag = fcntl (netfd, F_GETFL);
		fcntl (netfd, F_SETFL, O_NONBLOCK | flag);
		struct epoll_event ev;
		memset(&ev,0x0,sizeof(ev));
		ev.events = newEvents;
		slot->seq++;
		ev.data.u64 = ((unsigned long long)slot->seq << 32) + epslot;
		if (ownerUnit->Epctl (EPOLL_CTL_ADD, netfd, &ev) == 0)
			oldEvents = newEvents;
		else {
            ownerUnit->FreeEpollSlot(epslot);
			log4cplus_warning("Epctl: %m");
			return -1;
		}
		return 0;

	}
	return ApplyEvents ();
}

int CPollerObject::DetachPoller() {
	if(epslot) {
		struct epoll_event ev;
		memset(&ev,0x0,sizeof(ev));
		if (ownerUnit->Epctl (EPOLL_CTL_DEL, netfd, &ev) == 0)
			oldEvents = newEvents;
		else {
			log4cplus_warning("Epctl: %m");
			return -1;
		}
		ownerUnit->FreeEpollSlot(epslot);
		epslot = 0;
	}
	return 0;
}

int CPollerObject::ApplyEvents ()
{
	if (epslot <= 0 || oldEvents == newEvents)
		return 0;
	
	struct epoll_event ev;
	memset(&ev,0x0,sizeof(ev));

	ev.events = newEvents;
	struct CEpollSlot *slot = ownerUnit->GetSlot(epslot);
	slot->seq++;
	ev.data.u64 = ((unsigned long long)slot->seq << 32) + epslot;
	if (ownerUnit->Epctl (EPOLL_CTL_MOD, netfd, &ev) == 0)
		oldEvents = newEvents;
	else {
		log4cplus_warning("Epctl: %m");
		return -1;
	}

	return 0;
}

int CPollerObject::DelayApplyEvents ()
{               
    if (epslot <= 0 || oldEvents == newEvents)
	return 0;

    if(eventSlot)
	return 0;

    eventSlot = ownerUnit->AddDelayEventPoller(this);
    if(eventSlot == NULL)
    {
	log4cplus_error("max events!!!!!!");
	struct epoll_event ev;

	ev.events = newEvents;
	struct CEpollSlot *slot = ownerUnit->GetSlot(epslot);
	slot->seq++;
	ev.data.u64 = ((unsigned long long)slot->seq << 32) + epslot;
	if (ownerUnit->Epctl (EPOLL_CTL_MOD, netfd, &ev) == 0)
	    oldEvents = newEvents;
	else {
	    log4cplus_warning("Epctl: %m");
	    return -1;
	}
    }

    return 0;
}

int CPollerObject::CheckLinkStatus(void)
{
	char msg[1] = {0};
	int err = 0;

	err = recv(netfd, msg, sizeof(msg), MSG_DONTWAIT|MSG_PEEK);

	/* client already close connection. */
	if(err == 0 || (err < 0 && errno != EAGAIN))
		return -1;
	return 0;
}

void CPollerObject::InitPollFd(struct pollfd *pfd)
{
	pfd->fd = netfd;
	pfd->events = newEvents;
	pfd->revents = 0;
}

void CPollerObject::InputNotify(void) {
	EnableInput(false);
}

void CPollerObject::OutputNotify(void) {
	EnableOutput(false);
}

int CPollerUnit::totalEventSlot = 40960;

void CPollerObject::HangupNotify(void) {
	delete this;
}

CPollerUnit::CPollerUnit(int mp)
{
	maxPollers = mp;
	
	eeSize = maxPollers > 1024 ? 1024 : maxPollers;
	epfd = -1;
	ep_events = NULL;
	pollerTable = NULL;
	freeSlotList = 0;
	usedPollers = 0;
	//not initailize eventCnt variable may crash, fix crash bug by linjinming 2014-05-18
	eventCnt = 0;
}

CPollerUnit::~CPollerUnit() {
	// skip first one
	for (int i = 1; i < maxPollers; i++) 
	{
		if (pollerTable[i].freeList)
			continue;
		//delete pollerTable[i].poller;
	}

	FREE_CLEAR(pollerTable);

	if (epfd != -1)
	{
		close (epfd);
		epfd = -1;
	}

	FREE_CLEAR(ep_events);
}

int CPollerUnit::SetMaxPollers(int mp)
{
	if(epfd >= 0)
		return -1;
	maxPollers = mp;
	return 0;
}

int CPollerUnit::InitializePollerUnit(void)
{
	pollerTable = (struct CEpollSlot *)CALLOC(maxPollers, sizeof (*pollerTable));

	if (!pollerTable)
	{
		log4cplus_error("calloc failed, num=%d, %m", maxPollers);
		return -1;
	}

	// already zero-ed
	for (int i = 1; i < maxPollers - 1; i++)
	{
		pollerTable[i].freeList = i+1;
	}

	pollerTable[maxPollers - 1].freeList = 0;
	freeSlotList = 1;

	ep_events = (struct epoll_event *)CALLOC(eeSize, sizeof (struct epoll_event));

	if (!ep_events)
	{
		log4cplus_error("malloc failed, %m");
		return -1;
	}

	if ((epfd = epoll_create (maxPollers)) == -1)
	{
		log4cplus_warning("epoll_create failed, %m");
		return -1;
	}
	fcntl(epfd, F_SETFD, FD_CLOEXEC);
	return 0;
}

inline int CPollerUnit::VerifyEvents (struct epoll_event *ev)
{
	int idx = EPOLL_DATA_SLOT (ev);

	if ((idx >= maxPollers) || (EPOLL_DATA_SEQ (ev) != pollerTable[idx].seq))
	{
		return -1;
	}

	if(pollerTable[idx].poller == NULL || pollerTable[idx].freeList != 0)
	{
		log4cplus_info("receive invalid epoll event. idx=%d seq=%d poller=%p freelist=%d event=%x",
				idx, (int)EPOLL_DATA_SEQ(ev), pollerTable[idx].poller, 
				pollerTable[idx].freeList, ev->events);
		return -1;
	}
	return 0;
}

void CPollerUnit::FreeEpollSlot (int n)
{
	if(n <= 0) return; 
	pollerTable[n].freeList = freeSlotList;
	freeSlotList = n;
	usedPollers--;
	pollerTable[n].seq++;
	pollerTable[n].poller = NULL;
}

int CPollerUnit::AllocEpollSlot ()
{
	if (0 == freeSlotList) 
	{
        log4cplus_error("no free epoll slot, usedPollers = %d", usedPollers);
        return -1;
	}
	
	int n = freeSlotList;

	usedPollers++;
	freeSlotList = pollerTable[n].freeList;
	pollerTable[n].freeList = 0;

	return n;
}

int CPollerUnit::Epctl (int op, int fd, struct epoll_event *events)
{
	if (epoll_ctl (epfd,  op, fd, events) == -1)
    {
		log4cplus_warning("epoll_ctl error, epfd=%d, fd=%d", epfd, fd);

		return -1;
	}

	return 0;
}

int CPollerUnit::WaitPollerEvents(int timeout) {
	nrEvents = epoll_wait (epfd, ep_events, eeSize, timeout);
	return nrEvents;
}

void CPollerUnit::ProcessPollerEvents(void) {
	for (int i = 0; i < nrEvents; i++)
	{
		if(VerifyEvents (ep_events+i) == -1)
		{
			log4cplus_info("VerifyEvents failed, ep_events[%d].data.u64 = %llu", i, (unsigned long long)ep_events[i].data.u64);
			continue;
		}

		CEpollSlot *s = &pollerTable[EPOLL_DATA_SLOT(ep_events+i)];
		CPollerObject *p = s->poller;

		p->newEvents = p->oldEvents;
		if(ep_events[i].events & (EPOLLHUP | EPOLLERR))
		{
			p->HangupNotify();
			continue;
		}

		if(ep_events[i].events & EPOLLIN)
			p->InputNotify();

		s = &pollerTable[EPOLL_DATA_SLOT(ep_events+i)];
		if(s->poller==p && ep_events[i].events & EPOLLOUT)
			p->OutputNotify();

		s = &pollerTable[EPOLL_DATA_SLOT(ep_events+i)];
		if(s->poller==p)
		    p->DelayApplyEvents();
	}
}

int CPollerUnit::DelayApplyEvents()
{
    for(int i = 0; i < eventCnt; i++)
    {
	CPollerObject * p = eventSlot[i].poller;
	if(p)
	{
	    p->ApplyEvents();
	    eventSlot[i].poller = NULL;
	    p->ClearDelayApplyEvents();
	}
    }
    eventCnt = 0;
    return 0;
}
