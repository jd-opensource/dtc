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
#ifndef __DTC_CONDITION_H__
#define __DTC_CONDITION_H__

#include "algorithm/non_copyable.h"

class condition : private noncopyable {
    public:
	condition(void)
	{
		pthread_mutex_init(&m_lock, NULL);
		pthread_cond_init(&m_cond, NULL);
	}

	~condition(void)
	{
		pthread_mutex_destroy(&m_lock);
		pthread_cond_destroy(&m_cond);
	}

	void notify_one(void)
	{
		pthread_cond_signal(&m_cond);
	}
	void notify_all(void)
	{
		pthread_cond_broadcast(&m_cond);
	}

	void wait(void)
	{
		pthread_cond_wait(&m_cond, &m_lock);
	}

	void lock(void)
	{
		pthread_mutex_lock(&m_lock);
	}

	void unlock(void)
	{
		pthread_mutex_unlock(&m_lock);
	}

    private:
	pthread_cond_t m_cond;
	pthread_mutex_t m_lock;
};

class copedEnterCritical {
    public:
	copedEnterCritical(condition &c) : m_cond(c)
	{
		m_cond.lock();
	}

	~copedEnterCritical(void)
	{
		m_cond.unlock();
	}

    private:
	condition &m_cond;
};

class copedLeaveCritical {
    public:
	copedLeaveCritical(condition &c) : m_cond(c)
	{
		m_cond.unlock();
	}

	~copedLeaveCritical(void)
	{
		m_cond.lock();
	}

    private:
	condition &m_cond;
};

#endif //__DTC_CONDITION_H__
