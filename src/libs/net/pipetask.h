#ifndef __H_TTC_PIPETASK_TEMP_H__
#define __H_TTC_PIPETASK_TEMP_H__

#include "log.h"
#include "pipequeue.h"
#include "compiler.h"

template<typename T> class CTaskDispatcher;
template<typename T> class CReplyDispatcher;
template<typename T> class CRequestOutput;

template<typename T>
class CTaskIncomingPipe: public CPipeQueue<T, CTaskIncomingPipe<T> >
{
public:
	CTaskIncomingPipe(void){}
	virtual ~CTaskIncomingPipe(){}
	inline void TaskNotify(T p) {
		proc->TaskNotify(p);
	}

public:
	CTaskOneWayDispatcher<T> *proc;
};

template<typename T>
class CTaskReturnPipe: public CPipeQueue<T *, CTaskReturnPipe<T> >
{
public:
	CTaskReturnPipe(){};
	virtual ~CTaskReturnPipe(){};
	inline void TaskNotify(T *p) {
		p->ReplyNotify();
	}
};

template<typename T>
class CTaskPipe:
	public CTaskDispatcher<T>,
	public CReplyDispatcher<T>
{
private:
	static CLinkQueue<CTaskPipe<T> *> pipelist;

public:
	inline CTaskPipe() { pipelist.Push(this); }
	~CTaskPipe() { }

	virtual void TaskNotify(T *p) {
		p->PushReplyDispatcher(this);
		incQueue.Push(p);
	}
	virtual void ReplyNotify(T *p) {
		retQueue.Push(p);
	}
	inline int BindDispatcher(CRequestOutput<T> *fr, CTaskDispatcher<T> *to)
	{
#if 0
		log_debug("Bind taskpipe from thread %s to thread %s",
				fr->OwnerThread()->Name(), to->OwnerThread()->Name());
#endif
		CTaskDispatcher<T>::owner = fr->OwnerThread();

		incQueue.AttachPoller(fr->OwnerThread(), to->OwnerThread());
		retQueue.AttachPoller(to->OwnerThread(), fr->OwnerThread());

		fr->BindDispatcher(this);
		incQueue.proc = to;
		fr->DisableQueue();
		return 0;
	}

	static inline void DestroyAll(void)
	{
		CTaskPipe *p;
		while((p = pipelist.Pop()) != NULL)
		{
			delete p;
		}
	}

private:
	CTaskIncomingPipe<T *> incQueue;
	CTaskReturnPipe<T> retQueue;
};

template<typename T> CLinkQueue<CTaskPipe<T> *> CTaskPipe<T>::pipelist;

#endif
