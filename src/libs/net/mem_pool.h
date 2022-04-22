/*
*  sc_mem_pool.h
*
*  Created on: 2019.8.15
*  Author: liwujun
*/
#ifndef MEMPOOL_H_H_
#define MEMPOOL_H_H_

#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include "log.h"

typedef struct
{
	void **free_list;
	unsigned int size;
	char name[12];
}pool_head;

class MemPool
{
private:
	pool_head pool_block;
public:
	MemPool(){}
	~MemPool() {
		PoolDestroy();
	}

	void SetMemInfo(const char*name, unsigned int size) {
		memset(&pool_block, 0 ,sizeof(pool_block));
		memcpy(pool_block.name, name, sizeof(pool_block.name));
		pool_block.size = size;
		pool_block.free_list = NULL;
	}

	pool_head& GetPoolInfo(){
		return pool_block;
	}

	void* PoolRefillAlloc() {
		void *ret;
		ret = calloc(1, pool_block.size);
		if (ret == NULL) {
			log_error("allocate %s error,no more memory!", pool_block.name);
		}
		return ret;
	}

	void PoolDestroy() {
		void *temp, *next;
		if (!pool_block.free_list)
			return;

		next = pool_block.free_list;
		while (next) {
			temp = next;
			next = *(void **) temp;
			free(temp);
		}
		pool_block.free_list = (void**)next;
	}

	void* PoolAlloc(){
		void *ptr;
	    if ((ptr = pool_block.free_list) == NULL)
	        ptr = PoolRefillAlloc();
	    else {
	        pool_block.free_list = (void**)(*(pool_block.free_list));
	    }
	    return ptr;
	}

	void PoolFree(void* ptr) {
		if (ptr != NULL) {
        	*(void **)(ptr) = (void *)(pool_block.free_list);
        	pool_block.free_list = (void **)(ptr);
    	}
	}
};

#endif