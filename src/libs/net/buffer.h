#ifndef __BUFFER_H
#define __BUFFER_H

#include "memcheck.h"
#include <string.h>
#include <stdarg.h>

extern void CreateBuff(int inlen, int & len, char ** buff);

class buffer
{
private:
	char *buf;
	unsigned int bufSize;
	unsigned int dataSize;
	unsigned int needSize;

public:
	buffer(void) { buf = NULL; bufSize=dataSize=needSize=0; }
	~buffer(void) { FREE_IF(buf); bufSize=dataSize=needSize=0; }
	inline int expand(int size) {
		needSize = dataSize + size + 1;
		if(needSize > bufSize)
		{
			int sz = needSize + (needSize>>3);
			sz = (sz|0xf)+0x10; // align to 16 bytes
			if(REALLOC(buf, sz)==NULL)
				return -1;
			bufSize = sz;
		}
		return 0;
	}
	inline void trunc(int n)
	{
		if(n >= (long long)dataSize) return;
		if(n >= 0) { buf[dataSize = n] = 0; return; }
		n += dataSize;
		if(n < 0) return;
		buf[dataSize = n] = 0;
	}
	inline int resize(int n)
	{
		if(n > (long long)dataSize && expand(n - dataSize) < 0)
			return -1;
		buf[dataSize = n] = 0;
		return 0;
	}
	inline void clear(void) { trunc(0); }
	inline void release(void) { trunc(0); FREE_CLEAR(buf); bufSize=0; needSize=0; }
	inline char &operator[](int n) const { return buf[n]; }
	inline char at(int n) const {
		if(n > (long long)dataSize) return 0;
		if(n >= 0) return buf[n];
		n += dataSize;
		if(n < 0) return 0;
		return buf[n];
	}
	inline char *c_str(void) { return buf; }
	inline const char *c_str(void) const { return buf; }
	inline char *cursor(void) { return buf + dataSize; }
	inline const char *cursor(void) const { return buf + dataSize; }
	inline unsigned int size(void) const { return dataSize; }
	inline unsigned int needed(void) const { return needSize; }
	inline unsigned int remain(void) const { return bufSize - dataSize; }
	inline unsigned int margin(void) const { return needSize < bufSize ? 0 : needSize - bufSize; }
	inline int append(const char *data, int len) {
		if(expand(len) < 0) return -1;
		memcpy(buf+dataSize, data, len);
		dataSize += len;
		buf[dataSize] = 0;
		return 0;
	}
	inline int append(const char *data) {
		return append(data, strlen(data));
	}
	inline int append(const buffer &data) {
		return append(data.c_str(), data.size());
	}
	inline int append(const buffer *data) {
		return append(data->c_str(), data->size());
	}

#define __TEMPLATE_FUNC__(t) \
	inline int append(t v) { \
		if(expand(sizeof(t)) < 0) return -1; \
		*(t *)(buf + dataSize) = v; \
		dataSize += sizeof(t); \
		buf[dataSize] = 0; \
		return 0; \
	}
	__TEMPLATE_FUNC__(char);
	__TEMPLATE_FUNC__(signed char);
	__TEMPLATE_FUNC__(unsigned char);
	__TEMPLATE_FUNC__(short);
	__TEMPLATE_FUNC__(unsigned short);
	__TEMPLATE_FUNC__(int);
	__TEMPLATE_FUNC__(unsigned int);
	__TEMPLATE_FUNC__(long);
	__TEMPLATE_FUNC__(unsigned long);
	__TEMPLATE_FUNC__(long long);
	__TEMPLATE_FUNC__(unsigned long long);
	__TEMPLATE_FUNC__(float);
	__TEMPLATE_FUNC__(double);
#undef __TEMPLATE_FUNC__

	// unsafe
	inline buffer & add(const char *data, int len) {
		memcpy(buf+dataSize, data, len);
		dataSize += len;
		return *this;
	}
	inline buffer & operator<<(const char *data) {
		return add(data, strlen(data));
	}
	inline buffer & operator<<(const buffer &data) {
		return add(data.c_str(), data.size());
	}
	inline buffer & operator<<(const buffer *data) {
		return add(data->c_str(), data->size());
	}
	
#define __TEMPLATE_FUNC__(t) \
	inline buffer & operator<< (t v) { \
		*(t *)(buf + dataSize) = v; \
		dataSize += sizeof(t); \
		return *this; \
	}
	__TEMPLATE_FUNC__(char);
	__TEMPLATE_FUNC__(signed char);
	__TEMPLATE_FUNC__(unsigned char);
	__TEMPLATE_FUNC__(short);
	__TEMPLATE_FUNC__(unsigned short);
	__TEMPLATE_FUNC__(int);
	__TEMPLATE_FUNC__(unsigned int);
	__TEMPLATE_FUNC__(long);
	__TEMPLATE_FUNC__(unsigned long);
	__TEMPLATE_FUNC__(long long);
	__TEMPLATE_FUNC__(unsigned long long);
	__TEMPLATE_FUNC__(float);
	__TEMPLATE_FUNC__(double);
#undef __TEMPLATE_FUNC__
	int bprintf(const char *format,...);
	int vbprintf(const char *format, va_list ap);
};

#endif

