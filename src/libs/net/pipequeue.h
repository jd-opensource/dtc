#include <unistd.h>
#include "memcheck.h"
#include "poll_thread.h"

// typename T must be simple data type with 1,2,4,8,16 bytes
template<typename T, typename C>
class CPipeQueueReader: CPollerObject
{
private:
	T buf[32];
	int i; // first pending
	int n; // last pending

private:
	virtual void HangupNotify(void) { }
	virtual void InputNotify(void)
	{
		T p;
		int n=0;
		while(++n<=64 && (p=Pop()) != NULL)
			static_cast<C *>(this)->TaskNotify(p);
	}
	inline T Pop(void)
	{
		if(i>=n)
		{
			i=0;
			n = read(netfd, buf, sizeof(buf));
			if(n<=0)
				return NULL;
			n /= sizeof(T);
		}
		return buf[i++];
	}
public:
	inline CPipeQueueReader(): i(0), n(0){}
	virtual ~CPipeQueueReader(){}
	inline int AttachPoller (CPollerUnit *thread, int fd)
	{
		netfd = fd;
		EnableInput();
		return CPollerObject::AttachPoller (thread);
	}
};

template<class T>
class CPipeQueueWriter: CPollerObject, CTimerObject
{
private:
	PollerBase *owner;
	T *buf;
	int i; // first unsent
	int n; // last unsent
	int m; // buf size

private:
	virtual void HangupNotify(void) { }
	virtual void OutputNotify(void)
	{
		if(n>0)
		{
			// i always less than n
			int r = write(netfd, buf+i, (n-i)*sizeof(T));
			if(r > 0) {
				i += r/sizeof(T);
				if(i>=n) i = n = 0;
			}
		}
		if(n==0) {
			DisableOutput();
			DisableTimer();
		} else
			EnableOutput();
		DelayApplyEvents();
	}
	virtual void TimerNotify(void) {
		OutputNotify();
	}
public:
	inline CPipeQueueWriter() : i(0), n(0), m(16) { buf = (T *)MALLOC(m*sizeof(T)); }
	virtual ~CPipeQueueWriter(){ FREE_IF(buf); }
	inline int AttachPoller (PollerBase *thread, int fd)
	{
		owner = thread;
		netfd = fd;
		return CPollerObject::AttachPoller(thread);
	}
	inline int Push(T p)
	{
		if(n >= m)
		{
			if(i>0)
			{
				memmove(buf, buf+i, (n-i)*sizeof(T));
				n -= i;
			}
			else
			{
				T *newbuf = (T *)realloc(buf, 2*m*sizeof(T));
				if(newbuf==NULL)
					return -1;
				buf = newbuf;
				m *= 2;
			}
			i=0;
		}
		buf[n++] = p;

		// force flush every 16 pending task
		//if(n-i>=16 && n%16==0)
		if(n>0)
		{
			OutputNotify();
		}
		// some task pending, trigger ready timer
		/*if(n>0)
		{
			AttachReadyTimer(owner);
		}*/
		return 0;
	}
};


template<typename T, typename C>
class CPipeQueue: public CPipeQueueReader<T, C>
{
private:
	CPipeQueueWriter<T> writer;
public:
	inline CPipeQueue(){ }
	inline ~CPipeQueue(){}
	inline int AttachPoller(PollerBase *fr, PollerBase *to, int fd[2])
	{
		CPipeQueueReader<T, C>::AttachPoller(to, fd[0]);
		writer.AttachPoller(fr, fd[1]);
		return 0;
	}
	inline int AttachPoller(PollerBase *fr, PollerBase *to)
	{
		int fd[2];
		int ret = pipe(fd);
        if(ret != 0)
            return ret;

		CPipeQueueReader<T, C>::AttachPoller(to, fd[0]);
		writer.AttachPoller(fr, fd[1]);
		return 0;
	}
	inline int Push(T p) { return writer.Push(p); }
};
