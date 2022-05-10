#ifndef __H_TTC_REQUEST_TEMP_H__
#define __H_TTC_REQUEST_TEMP_H__

#include <endian.h>
#include <stdlib.h>

#include "lqueue.h"
#include "log.h"
#include "timerlist.h"

template<typename T> class CTaskPipe;

class PollerBase;

template<typename T>
class CTaskOneWayDispatcher {
public:
	PollerBase *owner;
public:
	inline CTaskOneWayDispatcher(PollerBase *p=NULL) : owner(p) {}
	virtual inline ~CTaskOneWayDispatcher(){}
	virtual void TaskNotify(T) = 0;

	inline PollerBase *OwnerThread(void) { return owner; }
	inline void AttachThread(PollerBase *thread){ if(owner==NULL) owner = thread; }
};

template<typename T>
class CTaskDispatcher : public CTaskOneWayDispatcher<T *> {
public:
	inline CTaskDispatcher(PollerBase *p=NULL) : CTaskOneWayDispatcher<T *>(p) {}
	virtual inline ~CTaskDispatcher(){}
};

template<typename T>
class CReplyDispatcher {
public:
	inline CReplyDispatcher() {}
	virtual inline ~CReplyDispatcher() {}
	virtual void ReplyNotify(T *) = 0;
};

template<typename T>
class CRequestOutput: private CTimerObject {
private:
	PollerBase *owner;
	CTaskDispatcher<T> *proc;
	CLinkQueue<T *> queue;
	typename CLinkQueue<T *>::allocator allocator;
protected:
	int queueDisabled;

private:
	inline virtual void TimerNotify(void)
	{
	    T *p;
	    while((p=queue.Pop()) != NULL)
		TaskNotify(p);
	}
public:
	inline CRequestOutput(PollerBase *o) :
		owner(o),
		proc(NULL),
		queue(&allocator),
		queueDisabled(0)
		//reply(NULL)
		{}
	inline ~CRequestOutput() {}
	inline PollerBase *OwnerThread(void) { return owner; }
	inline void BindDispatcher(CTaskDispatcher<T> *out)
	{
	    if(owner == out->OwnerThread()) {
		proc = out;
	    } else {
		CTaskPipe<T> *tp = new CTaskPipe<T>();
		tp->BindDispatcher(this, out);
	    }
	}
	inline void TaskNotify(T *p)
	{
	    proc->TaskNotify(p);
	}
	inline void IndirectNotify(T *p)
	{
	    if(queueDisabled)
		TaskNotify(p);
	    else {
		queue.Push(p);
		AttachReadyTimer(owner);
	    }
	}
	inline CTaskDispatcher<T> *GetDispatcher(void) { return proc; }
	inline void DisableQueue(void) { queueDisabled = 1; }
};

template<typename T, int maxcount=10>
class CTaskReplyList {
public:
	CReplyDispatcher<T> *proc[maxcount];
	int count;
public:
	inline CTaskReplyList(void) {count=0;};
	inline virtual ~CTaskReplyList(void) {};
	inline void Clean()
	{
	    memset(proc, 0, sizeof(CReplyDispatcher<T> *) * maxcount);
	    count = 0;
	}
	inline void CopyReplyPath(CTaskReplyList<T, 10> * org)
	{
	    memcpy(proc, org->proc, sizeof(CReplyDispatcher<T> *) * maxcount);
	    count = org->count;
	}
	inline int Push(CReplyDispatcher<T> *p)
	{
		if(count>=maxcount) return -1;
		proc[count++] = p;
		return 0;
	}
	inline CReplyDispatcher<T> *Pop(void)
	{
		return count==0 ? NULL : proc[--count];
	}
	inline void Dump(void) {
		for(int i=0; i<count; i++)
			log4cplus_debug("replyproc%d: %p", i, proc[i]);
	}
	inline void PushReplyDispatcher(CReplyDispatcher<T> *proc) {
		if(proc==NULL)
			static_cast<T *>(this)->Panic("PushReplyDispatcher: dispatcher is NULL, check your code");
		else if(Push(proc) != 0)
			static_cast<T *>(this)->Panic("PushReplyDispatcher: push queue failed, possible memory exhausted");
	}
	inline void ReplyNotify(void) {
		CReplyDispatcher<T> *proc = Pop();
		if(proc==NULL)
			static_cast<T *>(this)->Panic("ReplyNotify: no more dispatcher, possible double reply");
		else
			proc->ReplyNotify(static_cast<T *>(this));
	}
	void Panic(const char *msg);
};

template<typename T, int maxcount>
void CTaskReplyList<T, maxcount>::Panic(const char *msg) {
	log4cplus_error("Internal Error Encountered: %s", msg);
}

#endif
