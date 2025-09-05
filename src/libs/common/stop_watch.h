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
#ifndef __STOPWATCH_TIMER_H__
#define __STOPWATCH_TIMER_H__

#include <sys/time.h>

class stopwatch_unit_t {
    public:
	class sec {
	    public:
		static inline unsigned int gettime(void)
		{
			return time(NULL);
		};
	};

	class msec {
	    public:
		static inline unsigned int gettime(void)
		{
			struct timeval tv;
			gettimeofday(&tv, NULL);
			return tv.tv_sec * 1000 + tv.tv_usec / 1000;
		};
	};

	class usec {
	    public:
		static inline unsigned int gettime(void)
		{
			struct timeval tv;
			gettimeofday(&tv, NULL);
			return tv.tv_sec * 1000000 + tv.tv_usec;
		};
	};
};

template <typename T> class stopwatch_t {
    private:
	unsigned int _value;

    public:
	inline stopwatch_t(void)
	{
		_value = 0;
	}
	inline stopwatch_t(bool started)
	{
		_value = started ? T::gettime() : 0;
	}
	inline stopwatch_t(unsigned int v)
	{
		_value = v;
	}
	inline stopwatch_t(const stopwatch_t &v)
	{
		_value = v._value;
	}
	inline ~stopwatch_t(void)
	{
	}

	/* start timer now */
	inline void start(void)
	{
		_value = T::gettime();
	}
	/* start from timestamp:v */
	inline void init(unsigned int v)
	{
		_value = v;
	}
	inline stopwatch_t &operator=(unsigned int v)
	{
		_value = v;
		return *this;
	}
	/* stop watch */
	inline void stop(void)
	{
		_value = T::gettime() - _value;
	}
	/* get counter _value */
	inline unsigned int get(void)
	{
		return _value;
	}
	inline operator unsigned int(void) const
	{
		return _value;
	}
	inline unsigned int live(void) const
	{
		return T::gettime() - _value;
	}
};

typedef stopwatch_t<stopwatch_unit_t::sec> stopwatch_sec_t;
typedef stopwatch_t<stopwatch_unit_t::msec> stopwatch_msec_t;
typedef stopwatch_t<stopwatch_unit_t::usec> stopwatch_usec_t;
#endif
