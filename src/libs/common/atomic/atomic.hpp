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
#include "atomic.h"

struct AtomicU32 {
    private:
	typedef uint32_t V;
	atomic_t count;

    public:
	~AtomicU32(void)
	{
	}
	AtomicU32(V v = 0)
	{
		set(v);
	}

	inline V get(void) const
	{
		return atomic_read((atomic_t *)&count);
	}
	inline V set(V v)
	{
		atomic_set(&count, v);
		return v;
	}
	inline V add(V v)
	{
		return atomic_add_return(v, &count);
	}
	inline V sub(V v)
	{
		return atomic_sub_return(v, &count);
	}
	inline V clear(void)
	{
		return atomic_clear(&count);
	}
	inline V inc(void)
	{
		return add(1);
	}
	inline V dec(void)
	{
		return sub(1);
	}
	inline operator V(void) const
	{
		return get();
	}
	inline V operator=(V v)
	{
		return set(v);
	}
	inline V operator+=(V v)
	{
		return add(v);
	}
	inline V operator-=(V v)
	{
		return sub(v);
	}
	inline V operator++(void)
	{
		return inc();
	}
	inline V operator--(void)
	{
		return dec();
	}
	inline V operator++(int)
	{
		return inc() - 1;
	}
	inline V operator--(int)
	{
		return dec() + 1;
	}
};

struct AtomicS32 {
    private:
	typedef int32_t V;
	atomic_t count;

    public:
	~AtomicS32(void)
	{
	}
	AtomicS32(V v = 0)
	{
		set(v);
	}

	inline V get(void) const
	{
		return atomic_read((atomic_t *)&count);
	}
	inline V set(V v)
	{
		atomic_set(&count, v);
		return v;
	}
	inline V add(V v)
	{
		return atomic_add_return(v, &count);
	}
	inline V sub(V v)
	{
		return atomic_sub_return(v, &count);
	}
	inline V clear(void)
	{
		return atomic_clear(&count);
	}
	inline V inc(void)
	{
		return add(1);
	}
	inline V dec(void)
	{
		return sub(1);
	}
	inline operator V(void) const
	{
		return get();
	}
	inline V operator=(V v)
	{
		return set(v);
	}
	inline V operator+=(V v)
	{
		return add(v);
	}
	inline V operator-=(V v)
	{
		return sub(v);
	}
	inline V operator++(void)
	{
		return inc();
	}
	inline V operator--(void)
	{
		return dec();
	}
	inline V operator++(int)
	{
		return inc() - 1;
	}
	inline V operator--(int)
	{
		return dec() + 1;
	}
};

#if HAS_ATOMIC8
struct AtomicS64 {
    private:
	typedef int64_t V;
	atomic8_t count;

    public:
	~AtomicS64(void)
	{
	}
	AtomicS64(V v = 0)
	{
		set(v);
	}

	inline V get(void) const
	{
		return atomic8_read((atomic8_t *)&count);
	}
	inline V set(V v)
	{
		atomic8_set(&count, v);
		return v;
	}
	inline V add(V v)
	{
		return atomic8_add_return(v, &count);
	}
	inline V sub(V v)
	{
		return atomic8_sub_return(v, &count);
	}
	inline V clear(void)
	{
		return atomic8_clear(&count);
	}
	inline V inc(void)
	{
		return add(1);
	}
	inline V dec(void)
	{
		return sub(1);
	}
	inline operator V(void) const
	{
		return get();
	}
	inline V operator=(V v)
	{
		return set(v);
	}
	inline V operator+=(V v)
	{
		return add(v);
	}
	inline V operator-=(V v)
	{
		return sub(v);
	}
	inline V operator++(void)
	{
		return inc();
	}
	inline V operator--(void)
	{
		return dec();
	}
	inline V operator++(int)
	{
		return inc() - 1;
	}
	inline V operator--(int)
	{
		return dec() + 1;
	}
};

#endif
