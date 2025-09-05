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

#ifndef __DTC_GLOBAL_H
#define __DTC_GLOBAL_H

#include <stdint.h>
#include <stdarg.h>
#include "namespace.h"
#include "mem/pt_malloc.h"

DTC_BEGIN_NAMESPACE

/* Shared memory operation definitions */
#define M_HANDLE(ptr) PtMalloc::instance()->get_handle(ptr)
#define M_POINTER(type, v) PtMalloc::instance()->Pointer<type>(v)
#define M_MALLOC(size) PtMalloc::instance()->Malloc(size)
#define M_CALLOC(size) PtMalloc::instance()->Calloc(size)
#define M_REALLOC(v, size) PtMalloc::instance()->ReAlloc(v, size)
#define M_FREE(v) PtMalloc::instance()->Free(v)
#define M_ERROR() PtMalloc::instance()->get_err_msg()

/* Node search functions */
#define I_SEARCH(id) NodeIndex::instance()->do_search(id)
#define I_INSERT(node) NodeIndex::instance()->do_insert(node)
/*#define I_DELETE(node)		NodeIndex::instance()->Delete(node) */

/* memory handle*/
#define MEM_HANDLE_T ALLOC_HANDLE_T

/*Node ID*/
#define NODE_ID_T uint32_t
#define INVALID_NODE_ID ((NODE_ID_T)(-1))
#define SYS_MIN_NODE_ID ((NODE_ID_T)(0))
#define SYS_DIRTY_NODE_INDEX 0
#define SYS_CLEAN_NODE_INDEX 1
#define SYS_EMPTY_NODE_INDEX 2
#define SYS_DIRTY_HEAD_ID (SYS_MIN_NODE_ID + SYS_DIRTY_NODE_INDEX)
#define SYS_CLEAN_HEAD_ID (SYS_MIN_NODE_ID + SYS_CLEAN_NODE_INDEX)
#define SYS_EMPTY_HEAD_ID (SYS_MIN_NODE_ID + SYS_EMPTY_NODE_INDEX)

/* Node time list */
#define LRU_PREV (0)
#define LRU_NEXT (1)

/* features */
#define MIN_FEATURES 32

/*Hash ID*/
#define HASH_ID_T uint32_t

/* Node Group */
#define NODE_GROUP_INCLUDE_NODES 256

/* output u64 format */
#if __WORDSIZE == 64
#define UINT64FMT "%lu"
#else
#define UINT64FMT "%llu"
#endif

#if __WORDSIZE == 64
#define INT64FMT "%ld"
#else
#define INT64FMT "%lld"
#endif

enum { MULTIPLE_THREAD_MODE = 0, SINGLE_THREAD_MODE };

enum { DTC_CODE_FAILED = -1, DTC_CODE_SUCCESS = 0 };

enum DTC_MODE {
	DTC_MODE_DATABASE_ADDITION = 0, //cache + database
	DTC_MODE_CACHE_ONLY, // cache only
	DTC_MODE_DATABASE_ONLY // database only
};

DTC_END_NAMESPACE

#endif
