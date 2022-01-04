/*
* Copyright [2021] JD.com, Inc.
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
#ifndef __SINGLETON_H__
#define __SINGLETON_H__
#include "lock/lock.h"
#include "namespace.h"

DTC_BEGIN_NAMESPACE

template <class T> struct CreateUsingNew {
	static T *Create(void)
	{
		return new T;
	}

	static void destory(T *p)
	{
		delete p;
	}
};

template <class T, template <class> class CreationPolicy = CreateUsingNew>
class Singleton {
    public:
	static T *instance(void);
	static void destory(void);

    private:
	Singleton(void);
	Singleton(const Singleton &);
	Singleton &operator=(const Singleton &);

    private:
	static T *_instance;
	static Mutex _mutex;
};

DTC_END_NAMESPACE

DTC_USING_NAMESPACE

//implement
template <class T, template <class> class CreationPolicy>
Mutex Singleton<T, CreationPolicy>::_mutex;

template <class T, template <class> class CreationPolicy>
T *Singleton<T, CreationPolicy>::_instance = 0;

template <class T, template <class> class CreationPolicy>
T *Singleton<T, CreationPolicy>::instance(void)
{
	if (0 == _instance) {
		ScopedLock guard(_mutex);

		if (0 == _instance) {
			_instance = CreationPolicy<T>::Create();
		}
	}

	return _instance;
}

template <class T, template <class> class CreationPolicy>
void Singleton<T, CreationPolicy>::destory(void)
{
	if (0 != _instance) {
		ScopedLock guard(_mutex);
		if (0 != _instance) {
			CreationPolicy<T>::destory(_instance);
			_instance = 0;
		}
	}

	return;
}

#endif //__SINGLETON_H__
