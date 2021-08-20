/*
 * da_mem.c
 *
 *  Created on: 2014年11月30日
 *      Author: Jiansong
 *
 *  利用双向尾队列链接内存,经过测试发现为队列的效率略高于双向循环队列
 */

#include "da_mem_pool.h"
#include "da_log.h"
#include "da_util.h"

char mem_poison_byte = 0;

static struct pool_circqh pools = CIRCLEQ_HEAD_INITIALIZER(pools);

struct pool_head *create_pool(char *name, unsigned int size, unsigned int flags) {

	ASSERT(name !=NULL);
	ASSERT(size > 0);
	ASSERT(flags==0 || flags==MEM_F_SHARED);

	struct pool_head *pool;
	struct pool_head *entry;
	struct pool_head *start;
	unsigned int align;

	//16字节对齐
	align = 16;
	size = (size + align - 1) & -align;

	start = NULL;
	pool = NULL;

	CIRCLEQ_FOREACH(entry,&pools,pool_circqe)
	{
		if (entry->size == size) {
			if (flags & entry->flags & MEM_F_SHARED) {
				pool = entry;
				log_debug("Sharing %s with %s\n", name, pool->name);
				break;
			}
		} else if (entry->size > size) {
			log_debug("no suitable pool for %s", name);
			start = entry;
			break;
		}
	}

	if (!pool) {
		pool = calloc(1, sizeof(*pool));
		if (!pool) {
			log_error("allocate %s error,no more memory!", name);
			return NULL;
		}
		if (name)
			da_strlcpy(pool->name, name, sizeof(pool->name));
		pool->size = size;
		pool->flags = flags;
		if(start==NULL)
		{
			CIRCLEQ_INSERT_HEAD(&pools, pool, pool_circqe);
		}
		else
		{
			CIRCLEQ_INSERT_AFTER(&pools, start, pool, pool_circqe);
		}
	}
	pool->users++;
	return pool;
}

void *pool_refill_alloc(struct pool_head *pool) {
	ASSERT(pool !=NULL);

	void *ret;
	if (pool->limit && (pool->allocated >= pool->limit)) {
		return NULL;
	}
	ret = calloc(1, pool->size);
	if (!ret) {
		pool_gc();
		ret = calloc(1, pool->size);
		if (!ret)
			return NULL;
	}
	if (mem_poison_byte)
		memset(ret, mem_poison_byte, pool->size);
	pool->allocated++;
	pool->used++;
	return ret;
}

void pool_flush(struct pool_head *pool) {
	void *temp, *next;
	if (!pool)
		return;

	next = pool->free_list;
	while (next) {
		temp = next;
		next = *(void **) temp;
		pool->allocated--;
		free(temp);
	}
	pool->free_list = next;
}

void pool_gc() {
	static int recurse;
	struct pool_head *entry;

	//预防重复调用
	if (recurse++)
		goto out;

	CIRCLEQ_FOREACH(entry,&pools,pool_circqe)
	{
		void *temp, *next;

		next = entry->free_list;
		while (next && entry->allocated > entry->minavail
				&& entry->allocated > entry->used) {
			temp = next;
			next = *(void **) temp;
			entry->allocated--;
			free(temp);
		}
		entry->free_list = next;
	}
	out: recurse--;
}

void *pool_destroy(struct pool_head *pool) {
	if (pool) {
		pool_flush(pool);
		if (pool->used)
			return pool;
		pool->users--;
		if (!pool->users) {
			CIRCLEQ_REMOVE(&pools, pool, pool_circqe);
			free(pool);
		}
	}
	return NULL;
}

void dump_pools(void) {
	struct pool_head *entry;
	unsigned long allocated, used;
	int nbpools;

	allocated = used = nbpools = 0;
	log_error("Dumping pools usage. Use SIGQUIT to flush them.");
	CIRCLEQ_FOREACH(entry, &pools, pool_circqe) {
			log_error("  - Pool %s (%d bytes) : %d allocated (%u bytes), %d used, %d users%s\n",
				 entry->name, entry->size, entry->allocated,
				 entry->size * entry->allocated, entry->used,
				 entry->users, (entry->flags & MEM_F_SHARED) ? " [SHARED]" : "");

			allocated += entry->allocated * entry->size;
			used += entry->used * entry->size;
			nbpools++;
	}
	log_error("Total: %d pools, %lu bytes allocated, %lu used.\n",
			 nbpools, allocated, used);
	return;
}
