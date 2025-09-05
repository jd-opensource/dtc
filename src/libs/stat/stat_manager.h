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
* 
*/
#ifndef _STAT_MANAGER_H_
#define _STAT_MANAGER_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <map>
#include <vector>

#include "stat_info.h"
#include "atomic/atomic.h"

struct StatLock {
    public:
	class P {
	    private:
		StatLock *ptr;

	    public:
		P(StatLock *p)
		{
			ptr = p;
			pthread_mutex_lock(&ptr->lock);
		}
		~P()
		{
			pthread_mutex_unlock(&ptr->lock);
		}
	};

    private:
	friend class P;
	pthread_mutex_t lock;

    public:
	~StatLock()
	{
	}
	StatLock()
	{
		pthread_mutex_init(&lock, 0);
	}
};

#if HAS_ATOMIC8
struct StatItem {
    private:
	typedef int64_t V;
	static int64_t stat_item_dummy;
	atomic8_t *ptr;

    public:
	~StatItem(void)
	{
	}
	StatItem(void)
	{
		ptr = (atomic8_t *)&stat_item_dummy;
	}
	StatItem(volatile V *p) : ptr((atomic8_t *)p)
	{
	}
	StatItem(const StatItem &f) : ptr(f.ptr)
	{
	}

	inline V get(void) const
	{
		return atomic8_read(ptr);
	}
	inline V set(V v)
	{
		atomic8_set(ptr, v);
		return v;
	}
	inline V add(V v)
	{
		return atomic8_add_return(v, ptr);
	}
	inline V sub(V v)
	{
		return atomic8_sub_return(v, ptr);
	}
	inline V clear(void)
	{
		return atomic8_clear(ptr);
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
typedef StatItem StatCounter;
typedef StatItem StatCounter;

#else
struct StatCounter {
    private:
	typedef uint32_t V;
	static uint32_t stat_item_u32_dummy;
	atomic_t *ptr;

    public:
	~StatCounter(void)
	{
	}
	StatCounter(void)
	{
		ptr = (atomic_t *)&stat_item_u32_dummy;
	}
	StatCounter(volatile V *p) : ptr((atomic_t *)p)
	{
	}
	StatCounter(const StatCounter &f) : ptr(f.ptr)
	{
	}

	inline V get(void) const
	{
		return atomic_read(ptr);
	}
	inline V set(V v)
	{
		atomic_set(ptr, v);
		return v;
	}
	inline V add(V v)
	{
		return atomic_add_return(v, ptr);
	}
	inline V sub(V v)
	{
		return atomic_sub_return(v, ptr);
	}
	inline V clear(void)
	{
		return atomic_clear(ptr);
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
struct StatCounter {
    private:
	typedef int32_t V;
	static int32_t stat_item_s32_dummy;
	atomic_t *ptr;

    public:
	~StatCounter(void)
	{
	}
	StatCounter(void)
	{
		ptr = (atomic_t *)&stat_item_s32_dummy;
	}
	StatCounter(volatile V *p) : ptr((atomic_t *)p)
	{
	}
	StatCounter(const StatCounter &f) : ptr(f.ptr)
	{
	}

	inline V get(void) const
	{
		return atomic_read(ptr);
	}
	inline V set(V v)
	{
		atomic_set(ptr, v);
		return v;
	}
	inline V add(V v)
	{
		return atomic_add_return(v, ptr);
	}
	inline V sub(V v)
	{
		return atomic_sub_return(v, ptr);
	}
	inline V clear(void)
	{
		return atomic_clear(ptr);
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

struct StatItemObject : private StatLock {
    private:
	typedef int64_t V;
	volatile V *ptr;

	StatItemObject(const StatItemObject &);

    public:
	~StatItemObject(void)
	{
	}
	StatItemObject(void)
	{
	}
	StatItemObject(volatile V *p) : ptr(p)
	{
	}

	inline V get(void)
	{
		P a(this);
		return *ptr;
	}
	inline V set(V v)
	{
		P a(this);
		*ptr = v;
		return *ptr;
	}
	inline V add(V v)
	{
		P a(this);
		*ptr += v;
		return *ptr;
	}
	inline V sub(V v)
	{
		P a(this);
		*ptr -= v;
		return *ptr;
	}
	inline V clear(void)
	{
		P a(this);
		V v = *ptr;
		*ptr = 0;
		return v;
	}
	inline V inc(void)
	{
		return add(1);
	}
	inline V dec(void)
	{
		return sub(1);
	}
	inline operator V(void)
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

struct StatItem {
    private:
	typedef int64_t V;
	StatItemObject *ptr;
	static StatItemObject stat_item_Object_dummy;

    public:
	~StatItem(void)
	{
	}
	StatItem(void)
	{
		ptr = &stat_item_dummy;
	}
	StatItem(StatItemObject *p) : ptr(p)
	{
	}
	StatItem(const StatItem &f) : ptr(f.ptr)
	{
	}

	inline V get(void)
	{
		return ptr->get();
	}
	inline V set(V v)
	{
		return ptr->set(v);
	}
	inline V add(V v)
	{
		return ptr->add(v);
	}
	inline V sub(V v)
	{
		return ptr->sub(v);
	}
	inline V clear(void)
	{
		return ptr->clear();
	}
	inline V inc(void)
	{
		return add(1);
	}
	inline V dec(void)
	{
		return sub(1);
	}
	inline operator V(void)
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

struct StatSampleObject : private StatLock {
    private:
	const DTCStatInfo *dtc_stat_info;
	int64_t *stat_sample_object_count;
	StatSampleObject(const StatSampleObject &);
	static int64_t dummy_count[2];
	static const DTCStatInfo stat_dummy_info;

    public:
	~StatSampleObject(void)
	{
	}
	StatSampleObject(void)
		: dtc_stat_info(&stat_dummy_info),
		  stat_sample_object_count(dummy_count)
	{
	}
	StatSampleObject(const DTCStatInfo *i, int64_t *c)
		: dtc_stat_info(i), stat_sample_object_count(c)
	{
	}

	int64_t count(unsigned int n = 0);
	int64_t sum(void);
	int64_t average(int64_t o);
	void push(int64_t v);
	void output(int64_t *v);
};

struct StatSample {
    private:
	StatSampleObject *stat_sample_object_ptr;
	static StatSampleObject stat_sample_object_dummy;

    public:
	~StatSample(void)
	{
	}
	StatSample(void)
	{
		stat_sample_object_ptr = &stat_sample_object_dummy;
	}
	StatSample(StatSampleObject *p) : stat_sample_object_ptr(p)
	{
	}
	StatSample(const StatSample &f)
		: stat_sample_object_ptr(f.stat_sample_object_ptr)
	{
	}

	int64_t count(unsigned int n = 0)
	{
		return stat_sample_object_ptr->count(n);
	}
	int64_t sum(void)
	{
		return stat_sample_object_ptr->sum();
	}
	int64_t average(int64_t o)
	{
		return stat_sample_object_ptr->average(o);
	}
	void push(int64_t v)
	{
		return stat_sample_object_ptr->push(v);
	}
	void operator<<(int64_t v)
	{
		return stat_sample_object_ptr->push(v);
	}
};

class StatManager : protected StatLock {
    public:
	// types
	struct StatExpression {
		unsigned int offset_zero;
		unsigned int off_one;
		int value;
		int64_t (*call)(StatExpression *stat_expr_info,
				const char *map);
	};

    protected:
	// types
	struct StatInfo {
	    private:
		friend class StatManager;

#if !HAS_ATOMIC8
		StatItemObject *vobj;
		int ltype;
#endif
		StatManager *stat_manager_owner;
		DTCStatInfo *stat_info;
		StatSampleObject *stat_sample_object;
		StatExpression *stat_expression;

		StatInfo()
			:
#if !HAS_ATOMIC8
			  vobj(0), ltype(0),
#endif
			  stat_manager_owner(0), stat_info(0),
			  stat_sample_object(0), stat_expression(0)
		{
		}
		~StatInfo()
		{
#if !HAS_ATOMIC8
			if (vobj)
				delete vobj;
#endif
			if (stat_sample_object)
				delete stat_sample_object;
			if (stat_expression)
				delete[] stat_expression;
		}

#if !HAS_ATOMIC8
		int istype(int t)
		{
			return ltype == 0 || ltype == t;
		}
#endif
	    public:
		const DTCStatInfo &info(void)
		{
			return *stat_info;
		}
		inline unsigned id(void) const
		{
			return stat_info->id;
		}
		inline unsigned int type(void) const
		{
			return stat_info->type;
		}
		inline unsigned int unit(void) const
		{
			return stat_info->unit;
		}
		inline int is_sample(void) const
		{
			return stat_info->is_sample();
		}
		inline int is_counter(void) const
		{
			return stat_info->is_counter();
		}
		inline int is_value(void) const
		{
			return stat_info->is_value();
		}
		inline int is_const(void) const
		{
			return stat_info->is_const();
		}
		inline int is_expr(void) const
		{
			return stat_info->is_expr();
		}
		inline unsigned offset(void) const
		{
			return stat_info->offset;
		}
		inline unsigned count(void) const
		{
			return stat_info->before_count;
		}
		inline const char *name(void) const
		{
			return stat_info->name;
		}
	};

	static void *get_map_file_info(const char *file_path, int size);

    protected:
	// members
	int socket_lock_fd_;
	dev_t socket_lock_device_;
	ino_t socket_lock_ino_;

	char stat_error_message_[128];
	DTCStatHeader *header_;
	unsigned int index_size_;
	char *map_[7];
	const unsigned int get_map_size(void) const
	{
		return sizeof(map_) / sizeof(*map_);
	} //=======================Constants===============
	int64_t &at(unsigned int cat, unsigned offset, unsigned int n = 0)
	{
		return ((int64_t *)(map_[cat] + offset))[n];
	}
	int64_t &at_cur(unsigned offset, unsigned int n = 0)
	{
		return at(SC_CUR, offset, n);
	}
	int64_t &at_10s(unsigned offset, unsigned int n = 0)
	{
		return at(SC_10S, offset, n);
	}
	int64_t &at_10m(unsigned offset, unsigned int n = 0)
	{
		return at(SC_10M, offset, n);
	}
	int64_t &at_all(unsigned offset, unsigned int n = 0)
	{
		return at(SC_ALL, offset, n);
	}
	int64_t &atc_10s(unsigned offset, unsigned int n = 0)
	{
		return at(SCC_10S, offset, n);
	}
	int64_t &atc_10m(unsigned offset, unsigned int n = 0)
	{
		return at(SCC_10M, offset, n);
	}
	int64_t &atc_all(unsigned offset, unsigned int n = 0)
	{
		return at(SCC_ALL, offset, n);
	}

	std::map<unsigned int, StatInfo *> id_map_;
	std::vector<StatInfo *> stat_info_expression_;
	StatInfo *stat_info_;
	unsigned int stat_num_info_;

    private:
	StatManager(const StatManager &);

    public:
	// public method
	StatManager(void);
	~StatManager(void);

	const char *get_error_message(void) const
	{
		return stat_error_message_;
	}
	int init_stat_info(const char *, const char *, int isc = 0);
	static int create_stat_index(const char *name, const char *indexfile,
				     const DTCStatDefinition *stat_definition,
				     char *stat_error_message,
				     int stat_error__message_length);

	StatItem get_stat_iterm(unsigned int id);
	StatItem get_interval_10s_stat_iterm(unsigned int id);
	// get 10s static value , add by tomchen
	int64_t get_interval_10s_stat_value(unsigned int id);
#if HAS_ATOMIC8
	inline StatCounter get_stat_int_counter(unsigned int id)
	{
		return get_stat_iterm(id);
	}
	inline StatCounter get_stat_string_counter(unsigned int id)
	{
		return get_stat_iterm(id);
	}
#else
	StatCounter get_stat_int_counter(unsigned int id);
	StatCounter get_stat_string_counter(unsigned int id);
#endif
	StatSample get_sample(unsigned int id);

	int set_count_base(unsigned int id, const int64_t *v, int c);
	int get_count_base(unsigned int id, int64_t *v);

	int get_socket_lock_fd(const char *type);
	int unlock(void);
	void run_job_once(void);
	void clear(void);

    protected:
	StatExpression *init_stat_expression(unsigned int count, int64_t *arg);
	int64_t calculate_stat_expression(const char *map, unsigned int count,
					  StatExpression *stat_expression);
};

#endif
