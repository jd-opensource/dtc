#ifndef __SINGLETON_H__
#define __SINGLETON_H__
#include "lock.h"
#include "namespace.h"

TTC_BEGIN_NAMESPACE

template <class T> struct CreateUsingNew
{
    static T* Create (void)
    {
        return new T;
    }

    static void Destroy(T* p)
    {
        delete p;
    }
};

template <class T, template <class> class CreationPolicy = CreateUsingNew>
class CSingleton
{
public:
    static T* Instance (void);
    static void Destroy (void);

private:
    CSingleton (void);
    CSingleton (const CSingleton&);
    CSingleton& operator= (const CSingleton&);

private: 
    static T*       _instance;
    static CMutex   _mutex;
};

TTC_END_NAMESPACE


TTC_USING_NAMESPACE

//implement
template <class T, template <class> class CreationPolicy> 
CMutex CSingleton<T, CreationPolicy>::_mutex;

template <class T, template <class> class CreationPolicy> 
T* CSingleton<T, CreationPolicy>::_instance = 0;


template <class T, template <class> class CreationPolicy> 
T* CSingleton<T, CreationPolicy>::Instance (void)
{
    if (0 == _instance)
    {
        CScopedLock guard(_mutex);

        if (0 == _instance)
        {
            _instance = CreationPolicy<T>::Create ();
        }
    }

    return _instance;
}

/* BUGFIX by ada*/
#if 0
template <class T, template <class> class CreationPolicy> 
void CSingleton<T, CreationPolicy>::Destroy (void)
{
	return CreationPolicy<T>::Destroy (_instance);
}
#endif

template <class T, template <class> class CreationPolicy> 
void CSingleton<T, CreationPolicy>::Destroy (void)
{
	if(0 != _instance)
	{
		CScopedLock guard(_mutex);
		if(0 != _instance)
		{
			CreationPolicy<T>::Destroy (_instance);
			_instance = 0;
		}
	}

	return;
}

#endif //__SINGLETON_H__
