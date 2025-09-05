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
#ifndef __CH_VALUE_H_
#define __CH_VALUE_H_

#include <stdint.h>
#include <string.h>

struct DTCBinary {
	// member
	int len;
	char *ptr;

	// API
	inline int is_empty(void) const
	{
		return len <= 0;
	};
	inline uint8_t unsigned_value(int n = 0) const
	{
		return *(uint8_t *)(ptr + n);
	}
	inline int8_t signed_value(int n = 0) const
	{
		return *(uint8_t *)(ptr + n);
	}
	inline void next_value(int n = 1)
	{
		ptr += n;
		len -= n;
	}
	inline uint8_t unsigned_value_next(void)
	{
		len--;
		return *(uint8_t *)ptr++;
	}
	inline int8_t signed_value_next(void)
	{
		len--;
		return *(int8_t *)ptr++;
	}

	inline int operator!() const
	{
		return len <= 0;
	}
	inline int operator*() const
	{
		return *(uint8_t *)ptr;
	}
	inline uint8_t operator[](int n) const
	{
		return *(uint8_t *)(ptr + n);
	}
	inline DTCBinary &operator+=(int n)
	{
		ptr += n;
		len -= n;
		return *this;
	}
	inline DTCBinary &operator++()
	{
		ptr++;
		len--;
		return *this;
	}
	inline DTCBinary operator++(int)
	{
		DTCBinary r = { len, ptr };
		ptr++;
		len--;
		return r;
	}

	inline int operator<(int n) const
	{
		return len < n;
	}
	inline int operator<=(int n) const
	{
		return len <= n;
	}
	inline int operator>(int n) const
	{
		return len > n;
	}
	inline int operator>=(int n) const
	{
		return len >= n;
	}

	/* set ptr and len */
	inline DTCBinary &Set(const char *v)
	{
		ptr = (char *)v;
		len = v ? strlen(v) : 0;
		return *this;
	}
	inline DTCBinary &Set(const char *v, int l)
	{
		ptr = (char *)v;
		len = l;
		return *this;
	}
	inline DTCBinary &SetZ(const char *v)
	{
		ptr = (char *)v;
		len = v ? strlen(v) + 1 : 0;
		return *this;
	}
	inline DTCBinary &operator=(const char *v)
	{
		return Set(v);
	}

	/* set and copy */
	inline static DTCBinary Make(const char *v, int l)
	{
		DTCBinary a;
		a.Set(v, l);
		return a;
	}
	inline static DTCBinary Make(const char *v)
	{
		DTCBinary a;
		a.Set(v);
		return a;
	}
	inline static DTCBinary MakeZ(const char *v)
	{
		DTCBinary a;
		a.SetZ(v);
		return a;
	}
};

struct Array : public DTCBinary {
	Array(int l, char *p)
	{
		len = l;
		ptr = p;
	}
	Array(const DTCBinary &bin)
	{
		len = bin.len;
		ptr = bin.ptr;
	}

#define ADD_V(TYPE)                                                            \
	void Add(TYPE val)                                                     \
	{                                                                      \
		*(TYPE *)(ptr + len) = val;                                    \
		len += sizeof(TYPE);                                           \
	}
	ADD_V(int8_t);
	ADD_V(uint8_t);
	ADD_V(int32_t);
	ADD_V(uint32_t);
	ADD_V(int64_t);
	ADD_V(uint64_t);
#undef ADD_V

	void Add(const char *val)
	{
		uint32_t slen = strlen(val);
		*(uint32_t *)(ptr + len) = slen + 1;
		memcpy(ptr + len + sizeof(uint32_t), val, slen + 1);
		len += sizeof(uint32_t) + slen + 1;
	}

	void Add(const char *val, int l)
	{
		*(uint32_t *)(ptr + len) = l;
		memcpy(ptr + len + sizeof(uint32_t), val, l);
		len += sizeof(uint32_t) + l;
	}

#define GET_V(TYPE)                                                            \
	int Get(TYPE &val, int size = 0)                                       \
	{                                                                      \
		if ((unsigned int)len < sizeof(TYPE))                          \
			return (-1);                                           \
		val = *(TYPE *)ptr;                                            \
		ptr += sizeof(TYPE);                                           \
		len -= sizeof(TYPE);                                           \
		return (0);                                                    \
	}

	GET_V(int8_t);
	GET_V(uint8_t);
	GET_V(int32_t);
	GET_V(uint32_t);
	GET_V(int64_t);
	GET_V(uint64_t);
#undef GET_V

	int Get(DTCBinary &bin)
	{
		if ((unsigned int)len < sizeof(uint32_t))
			return (-1);
		bin.len = *(uint32_t *)ptr;
		bin.ptr = ptr + sizeof(uint32_t);
		if ((unsigned int)len < sizeof(uint32_t) + bin.len)
			return (-2);
		ptr += sizeof(uint32_t) + bin.len;
		len -= sizeof(uint32_t) + bin.len;
		return (0);
	}
};

typedef union DTCValue {
	// member
	int64_t s64;
	uint64_t u64;
	double flt;
	struct DTCBinary bin;
	struct DTCBinary str;

	// API
	DTCValue()
	{
	}
#if !__x86_64__
	DTCValue(long v) : s64(v)
	{
	}
	DTCValue(unsigned long v) : u64(v)
	{
	}
#else
	DTCValue(long long v) : s64(v)
	{
	}
	DTCValue(unsigned long long v) : u64(v)
	{
	}
#endif
	DTCValue(int32_t v) : s64(v)
	{
	}
	DTCValue(uint32_t v) : u64(v)
	{
	}
	DTCValue(int64_t v) : s64(v)
	{
	}
	DTCValue(uint64_t v) : u64(v)
	{
	}
	DTCValue(float v) : flt(v)
	{
	}
	DTCValue(double v) : flt(v)
	{
	}
	DTCValue(const char *v)
	{
		str = DTCBinary::Make(v);
	}
	DTCValue(const char *v, int l)
	{
		str = DTCBinary::Make(v, l);
	}
	~DTCValue()
	{
	}
#if !__x86_64__
	static DTCValue Make(const long v)
	{
		DTCValue a(v);
		return a;
	}
	static DTCValue Make(const unsigned long v)
	{
		DTCValue a(v);
		return a;
	}
#else
	static DTCValue Make(const long long v)
	{
		DTCValue a(v);
		return a;
	}
	static DTCValue Make(const unsigned long long v)
	{
		DTCValue a(v);
		return a;
	}
#endif
	static DTCValue Make(const int32_t v)
	{
		DTCValue a(v);
		return a;
	}
	static DTCValue Make(const uint32_t v)
	{
		DTCValue a(v);
		return a;
	}
	static DTCValue Make(const int64_t v)
	{
		DTCValue a(v);
		return a;
	}
	static DTCValue Make(const uint64_t v)
	{
		DTCValue a(v);
		return a;
	}
	static DTCValue Make(const float v)
	{
		DTCValue a(v);
		return a;
	}
	static DTCValue Make(const double v)
	{
		DTCValue a(v);
		return a;
	}
	static const DTCValue Make(const char *v)
	{
		const DTCValue a(v);
		return a;
	}
	static const DTCValue Make(const char *v, int l)
	{
		const DTCValue a(v, l);
		return a;
	}
#if !__x86_64__
	DTCValue &Set(const long v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &Set(const unsigned long v)
	{
		u64 = v;
		return *this;
	}
#else
	DTCValue &Set(const long long v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &Set(const unsigned long long v)
	{
		u64 = v;
		return *this;
	}
#endif
	DTCValue &Set(const int32_t v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &Set(const uint32_t v)
	{
		u64 = v;
		return *this;
	}
	DTCValue &Set(const int64_t v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &Set(const uint64_t v)
	{
		u64 = v;
		return *this;
	}
	DTCValue &Set(const float v)
	{
		flt = v;
		return *this;
	}
	DTCValue &Set(const double v)
	{
		flt = v;
		return *this;
	}
	DTCValue &Set(const char *v)
	{
		str.Set(v);
		return *this;
	}
	DTCValue &SetZ(const char *v)
	{
		str.SetZ(v);
		return *this;
	}
	DTCValue &Set(const char *v, int l)
	{
		str.Set(v, l);
		return *this;
	}

#if !__x86_64__
	DTCValue &operator=(const long v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &operator=(const unsigned long v)
	{
		u64 = v;
		return *this;
	}
#else
	DTCValue &operator=(const long long v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &operator=(const unsigned long long v)
	{
		u64 = v;
		return *this;
	}
#endif
	DTCValue &operator=(const int32_t v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &operator=(const uint32_t v)
	{
		u64 = v;
		return *this;
	}
	DTCValue &operator=(const int64_t v)
	{
		s64 = v;
		return *this;
	}
	DTCValue &operator=(const uint64_t v)
	{
		u64 = v;
		return *this;
	}
	DTCValue &operator=(const float v)
	{
		flt = v;
		return *this;
	}
	DTCValue &operator=(const double v)
	{
		flt = v;
		return *this;
	}
	DTCValue &operator=(const char *v)
	{
		str.Set(v);
		return *this;
	}
} DTCValue;

extern "C" {
int mystrcmp(const char *s1, const char *s2, int l);
static inline char INTERNAL_TO_LOWER(char c)
{
	extern unsigned char internal_tolower_table_[];
	return internal_tolower_table_[(unsigned char)c];
}
};
static inline int is_string_z(const DTCBinary &a)
{
	return a.len > 0 && a.ptr[a.len - 1] == '\0';
} // NULL ENDED
static inline int is_string_z(const DTCValue &a)
{
	return is_string_z(a.str);
}

static inline int string_equal(const DTCBinary &a, const char *b)
{
	return !mystrcmp(a.ptr, b, a.len);
}
static inline int string_equal(const char *a, const DTCBinary &b)
{
	return string_equal(b, a);
}
static inline int string_equal(const DTCValue &a, const char *b)
{
	return string_equal(a.str, b);
}
static inline int string_equal(const char *a, const DTCValue &b)
{
	return string_equal(a, b.str);
}

static inline int string_equal(const DTCBinary &a, const DTCBinary &b)
{
	return a.len == b.len && !mystrcmp(a.ptr, b.ptr, a.len);
}

static inline int string_greater(const DTCBinary &a, const DTCBinary &b)
{
	return a.len == b.len && mystrcmp(a.ptr, b.ptr, a.len) > 0;
}

static inline int string_greater_equal(const DTCBinary &a, const DTCBinary &b)
{
	return a.len == b.len && mystrcmp(a.ptr, b.ptr, a.len) >= 0 ;
}

static inline int string_less(const DTCBinary &a, const DTCBinary &b)
{
	return a.len == b.len && mystrcmp(a.ptr, b.ptr, a.len) < 0;
}

static inline int string_less_equal(const DTCBinary &a, const DTCBinary &b)
{
	return a.len == b.len && mystrcmp(a.ptr, b.ptr, a.len) <= 0;
}

static inline int string_equal(const DTCValue &a, const DTCBinary &b)
{
	return string_equal(a.str, b);
}
static inline int string_equal(const DTCBinary &a, const DTCValue &b)
{
	return string_equal(a, b.str);
}
static inline int string_equal(const DTCValue &a, const DTCValue &b)
{
	return string_equal(a.str, b.str);
}

static inline int string_greater(const DTCValue &a, const DTCValue &b)
{
	return string_greater(a.str, b.str);
}

static inline int string_greater_equal(const DTCValue &a, const DTCValue &b)
{
	return string_greater_equal(a.str, b.str);
}

static inline int string_less(const DTCValue &a, const DTCValue &b)
{
	return string_less(a.str, b.str);
}

static inline int string_less_equal(const DTCValue &a, const DTCValue &b)
{
	return string_less_equal(a.str, b.str);
}

static inline int binary_equal(const DTCValue &a, const DTCValue &b)
{
	return a.bin.len == b.bin.len &&
	       !memcmp(a.bin.ptr, b.bin.ptr, a.bin.len);
}

#include <sys/select.h>

static inline int FIELD_ISSET(int id, const uint8_t *mask)
{
	return FD_ISSET(id, (const fd_set *)mask);
}

static inline void FIELD_SET(int id, uint8_t *mask)
{
	FD_SET(id, (fd_set *)mask);
}
static inline void FIELD_CLR(int id, uint8_t *mask)
{
	FD_CLR(id, (fd_set *)mask);
}

#define FIELD_ZERO(x) memset(x, 0, sizeof(x))

struct BufferChain {
	inline void Count(int &n, int &l)
	{
		n = l = 0;
		BufferChain *bc = this;
		while (bc) {
			n++;
			l += bc->usedBytes;
			bc = bc->nextBuffer;
		}
	}
	/* reset all data's used bytes, total bytes not changed */
	inline void ChainClean()
	{
		BufferChain *bc = this;
		while (bc) {
			bc->usedBytes = 0;
			bc = bc->nextBuffer;
		}
	}
	inline void Clean()
	{
		nextBuffer = NULL;
		usedBytes = 0;
	}
	BufferChain *nextBuffer;
	int totalBytes;
	int usedBytes;
	char data[0];
};

#endif
