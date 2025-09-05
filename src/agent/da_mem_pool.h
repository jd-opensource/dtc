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

#ifndef DA_MEM_POOL_H_
#define DA_MEM_POOL_H_

#include "da_queue.h"
#include <stdio.h>
#include <stdlib.h>

#define MEM_F_SHARED 0x01

struct pool_head {
  void **free_list;
  CIRCLEQ_ENTRY(pool_head) pool_circqe;
  unsigned int used;
  unsigned int allocated;
  unsigned int limit;
  unsigned int minavail;
  unsigned int size;
  unsigned int flags;
  unsigned int users;
  char name[12];
};

CIRCLEQ_HEAD(pool_circqh, pool_head);
extern char mem_poison_byte;

/*
 *	init pools
 */
void init_pools();

/* Try to find an existing shared pool with the same characteristics and
 * returns it, otherwise creates this one. NULL is returned if no memory
 * is available for a new creation.
 */
struct pool_head *create_pool(char *name, unsigned int size,
                              unsigned int flags);

/* Allocate a new entry for pool <pool>, and return it for immediate use.
 * NULL is returned if no memory is available for a new creation.
 */
void *pool_refill_alloc(struct pool_head *pool);

/*
 * This function frees whatever can be freed in pool <pool>.
 */
void pool_flush(struct pool_head *pool);

/*
 * This function frees whatever can be freed in all pools, but respecting
 * the minimum thresholds imposed by owners.
 */
void pool_gc();

/*
 * This function destroys a pool by freeing it completely, unless it's still
 * in use. This should be called only under extreme circumstances. It always
 * returns NULL if the resulting pool is empty, easing the clearing of the old
 * pointer, otherwise it returns the pool.
 * .
 */
void *pool_destroy(struct pool_head *pool);

/*
 * dump pools to log
 */
void dump_pools(void);

/*
 * Returns a pointer to type <type> taken from the
 * pool <pool_type> or dynamically allocated. In the
 * first case, <pool_type> is updated to point to the
 * next element in the list.
 */
#define pool_alloc(pool)                                                       \
  ({                                                                           \
    void *__p;                                                                 \
    if ((__p = (pool)->free_list) == NULL)                                     \
      __p = pool_refill_alloc(pool);                                           \
    else {                                                                     \
      (pool)->free_list = *(void **)(pool)->free_list;                         \
      (pool)->used++;                                                          \
    }                                                                          \
    __p;                                                                       \
  })

/*
 * Puts a memory area back to the corresponding pool.
 * Items are chained directly through a pointer that
 * is written in the beginning of the memory area, so
 * there's no need for any carrier cell. This implies
 * that each memory area is at least as big as one
 * pointer. Just like with the libc's free(), nothing
 * is done if <ptr> is NULL.
 */
#define pool_free(pool, ptr)                                                   \
  ({                                                                           \
    if (likely((ptr) != NULL)) {                                               \
      *(void **)(ptr) = (void *)(pool)->free_list;                             \
      (pool)->free_list = (void *)(ptr);                                       \
      (pool)->used--;                                                          \
    }                                                                          \
  })

#endif /* DA_MEM_H_ */
