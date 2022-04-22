#include <malloc.h>
#include <string.h>

#if MEMCHECK
#include <map>
#include "lock.h"
#include "log.h"

#define HASHBASE 4097

static CMutex lock;

struct location_t {
	const char *ret;
	int ln;
};

struct location_cmp
{
	bool operator()(const location_t &a, const location_t &b) const
	{ return a.ret < b.ret || (a.ret==b.ret && a.ln < b.ln) ? true : false; }
};

struct count_t
{
	unsigned long count;
	unsigned long size;
	void add(unsigned long s) { count++; size+=s; }
};

struct slot_t
{
	struct slot_t *next;
	void *ptr;
	size_t size;
	struct location_t loc;
};

static struct slot_t *freelist;
static struct slot_t *usedlist[HASHBASE];
static int bypass = 1;
static unsigned long totalsize;
static unsigned long maxsize;

void add_ptr(void *ptr, size_t size, const char *ret, int ln)
{
	struct slot_t *s = freelist;
	if(s) freelist = s->next;
	else s = (struct slot_t *)malloc(sizeof(struct slot_t));

	unsigned n = ((unsigned long)ptr) % HASHBASE;
	s->ptr = ptr;
	s->size = size;
	s->loc.ret = ret;
	s->loc.ln = ln;
	s->next = usedlist[n];
	usedlist[n] = s;
	totalsize += size;
	if(size >= 1<<20)
		log_debug("large memory allocated size=%ld", (long)size);
	if(totalsize > maxsize)
		maxsize = totalsize;
}

void del_ptr(void *ptr)
{
	unsigned n = ((unsigned long)ptr) % HASHBASE;
	struct slot_t **ps = &usedlist[n];
	while(*ps)
	{
		if((*ps)->ptr == ptr)
		{
			struct slot_t *s = *ps;
			totalsize -= s->size;
			*ps = s->next;
			s->next = freelist;
			freelist = s;
			return;
		}
		ps = &(*ps)->next;
	}
}

void enable_memchecker(void)
{
	bypass = 0;
}

void dump_non_delete(void)
{
	bypass = 1;

	while(freelist)
	{
		struct slot_t *s = freelist;
		freelist = freelist->next;
		free(s);
	}

	std::map<location_t, count_t, location_cmp> m;
	for(unsigned int n=0; n<HASHBASE; n++)
	{
		while(usedlist[n])
		{
			struct slot_t *s = usedlist[n];
			usedlist[n] = s->next;
			m[s->loc].add(s->size);
			free(s);
		}
	}

	for(std::map<location_t, count_t, location_cmp>::iterator i = m.begin(); i!=m.end(); i++)
	{
		const location_t &loc = i->first;
		const count_t &val = i->second;
		if(loc.ln==0)
			log_info("remain %ld %ld ret@%p", val.count, val.size, loc.ret);
		else
			log_info("remain %ld %ld %s(%d)", val.count, val.size, loc.ret, loc.ln);
	}
	log_info("Maximum Memory allocated: %lu\n", maxsize);
}

void report_mallinfo(void)
{
	struct mallinfo mi = mallinfo();
	log_debug("mallinfo.   arena: %d\n", mi.arena);
	log_debug("mallinfo. ordblks: %d\n", mi.ordblks);
	log_debug("mallinfo.  smblks: %d\n", mi.smblks);
	log_debug("mallinfo.   hblks: %d\n", mi.hblks);
	log_debug("mallinfo. usmblks: %d\n", mi.usmblks);
	log_debug("mallinfo. fsmblks: %d\n", mi.fsmblks);
	log_debug("mallinfo.uordblks: %d\n", mi.uordblks);
	log_debug("mallinfo.fordblks: %d\n", mi.fordblks);
	log_debug("mallinfo.keepcost: %d\n", mi.keepcost);
}

unsigned long count_virtual_size(void)
{
	FILE *fp = fopen("/proc/self/maps", "r");
	char buf[256];
	buf[sizeof(buf)-1] = '\0';
	unsigned long total = 0;
	while(fgets(buf, sizeof(buf)-1, fp) != NULL)
	{
		unsigned long start, end;
		char r,w,x,p;
		if(sscanf(buf, "%lx-%lx %c%c%c%c", &start, &end,&r,&w,&x,&p)!=6) continue;
		if(r=='-' && w=='-' && x=='-') continue;
		if(r!='r') continue;
		if(p=='s') continue;
		total += end-start;
		
	}
	fclose(fp);
	return total;
}

unsigned long count_alloc_size(void)
{
	return totalsize;
}


void *operator new(size_t size)
{
	const char *ret = (const char *)__builtin_return_address(0);
	CScopedLock a(lock);

	void *p = malloc(size);
	if(bypass==0)
	{
		if(p==NULL)
			log_error("ret@%p: operator new(%ld) failed", ret, (long)size);
		else
			add_ptr(p, size, ret, 0);
	}
	return p;
}

void operator delete(void *p)
{
	CScopedLock a(lock);

	if(bypass==0)
	{
		if(p)
			del_ptr(p);
	}
	return free(p);
}

void *operator new[](size_t size)
{
	const char *ret = (const char *)__builtin_return_address(0);
	CScopedLock a(lock);

	void *p = malloc(size);
	if(bypass==0)
	{
		if(p==NULL)
			log_error("ret@%p: operator new(%ld) failed", ret, (long)size);
		else
			add_ptr(p, size, ret, 0);
	}
	return p;
}

void operator delete[](void *p)
{
	CScopedLock a(lock);

	if(bypass==0)
	{
		if(p)
			del_ptr(p);
	}
	return free(p);
}

extern "C"
void *malloc_debug(size_t size, const char *fn, int ln)
{
	CScopedLock a(lock);

	void *p = malloc(size);
	if(bypass==0)
	{
		if(p==NULL)
			log_error("%s(%d): malloc(%ld) failed", fn, ln, (long)size);
		else
			add_ptr(p, size, fn, ln);
	}
	return p;
}

extern "C"
void *calloc_debug(size_t size, size_t nmem, const char *fn, int ln)
{
	CScopedLock a(lock);

	void *p = calloc(size, nmem);
	if(bypass==0)
	{
		if(p==NULL)
			log_error("%s(%d): calloc(%ld, %ld) failed", fn, ln, (long)size, (long)nmem);
		else
			add_ptr(p, size*nmem, fn, ln);
	}
	return p;
}

extern "C"
void *realloc_debug(void *o, size_t size, const char *fn, int ln)
{
	CScopedLock a(lock);

	void *p = realloc(o, size);
	if(bypass==0)
	{
		if(p==NULL)
			log_error("%s(%d): realloc(%p, %ld) failed", fn, ln, o, (long)size);
		else {
			del_ptr(o);
			add_ptr(p, size, fn, ln);
		}
	}
	return p;
}

extern "C"
char *strdup_debug(const char *o, const char *fn, int ln)
{
	CScopedLock a(lock);

	char *p = strdup(o);
	if(bypass==0)
	{
		long size = strlen(o)+1;
		if(p==NULL)
			log_error("%s(%d): strdup(%ld) failed", fn, ln, size);
		else
			add_ptr(p, size, fn, ln);
	}
	return p;
}

extern "C"
void free_debug(void *p, const char *fn, int lnp)
{
	CScopedLock a(lock);

	if(bypass==0)
	{
		if(p)
			del_ptr(p);
	}
	return free(p);
}

#else
void *operator new(size_t size)
{
	return calloc(1, size);
}
void operator delete(void *p)
{
	return free(p);
}
void dump_non_delete(void)
{
}
#endif
