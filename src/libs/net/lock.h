#ifndef __TTC_LOCK_H__
#define __TTC_LOCK_H__

#include <pthread.h>
#include "namespace.h"

TTC_BEGIN_NAMESPACE

class CMutex 
{
    friend class condition;

public:
    inline CMutex (void) 
    {
        ::pthread_mutex_init (&_mutex, 0);
    }

    inline void lock (void) 
    {
        ::pthread_mutex_lock (&_mutex);
    }

    inline void unlock (void) 
    {
        ::pthread_mutex_unlock (&_mutex);
    }

    inline ~CMutex (void) 
    {
        ::pthread_mutex_destroy (&_mutex);
    }

private:
    CMutex (const CMutex& m);
    CMutex& operator= (const CMutex &m);

private:
    pthread_mutex_t _mutex;
};

/**
 * *    definition of ScopedLock;
 * **/
class CScopedLock 
{
    friend class condition;

public:
    inline CScopedLock (CMutex& mutex) : _mutex (mutex) 
    {
        _mutex.lock ();
    }

    inline ~CScopedLock (void) 
    {
        _mutex.unlock ();
    }

private:
    CMutex& _mutex;
};

TTC_END_NAMESPACE

#endif //__TTC_LOCK_H__
