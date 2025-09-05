/*
* Copyright JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef __DTC_LOCK_H__
#define __DTC_LOCK_H__

#include <pthread.h>
#include "namespace.h"

DTC_BEGIN_NAMESPACE

class Mutex {
	friend class condition;

    public:
	inline Mutex(void)
	{
		::pthread_mutex_init(&_mutex, 0);
	}

	inline void lock(void)
	{
		::pthread_mutex_lock(&_mutex);
	}

	inline void unlock(void)
	{
		::pthread_mutex_unlock(&_mutex);
	}

	inline ~Mutex(void)
	{
		::pthread_mutex_destroy(&_mutex);
	}

    private:
	Mutex(const Mutex &m);
	Mutex &operator=(const Mutex &m);

    private:
	pthread_mutex_t _mutex;
};

/**
 * *    definition of ScopedLock;
 * **/
class ScopedLock {
	friend class condition;

    public:
	inline ScopedLock(Mutex &mutex) : _mutex(mutex)
	{
		_mutex.lock();
	}

	inline ~ScopedLock(void)
	{
		_mutex.unlock();
	}

    private:
	Mutex &_mutex;
};

DTC_END_NAMESPACE

#endif //__DTC_LOCK_H__
