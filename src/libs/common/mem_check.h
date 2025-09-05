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
#ifndef __H_MEMCHEC____
#define __H_MEMCHEC____

#include <malloc.h>

#if MEMCHECK
#define MALLOC(x) malloc_debug(x, __FILE__, __LINE__)
#define FREE(x) free_debug(x, __FILE__, __LINE__)
#define FREE_IF(x)                                                             \
	do {                                                                   \
		if ((x) != 0)                                                  \
			free_debug((void *)(x), __FILE__, __LINE__);           \
	} while (0)
#define FREE_CLEAR(x)                                                          \
	do {                                                                   \
		if ((x) != 0) {                                                \
			free_debug((void *)(x), __FILE__, __LINE__);           \
			(x) = 0;                                               \
		}                                                              \
	} while (0)
#if ROCKSDB_COMPILER
#define REALLOC(p, sz)                                                         \
	({                                                                     \
		void *a = realloc(p, sz);                                      \
		if (a)                                                         \
			p = (decltype(p))a;                                    \
		a;                                                             \
	})
#else
#define REALLOC(p, sz)                                                         \
	({                                                                     \
		void *a = realloc_debug(p, sz, __FILE__, __LINE__);            \
		if (a)                                                         \
			p = (typeof(p))a;                                      \
		a;                                                             \
	})
#endif
#define CALLOC(x, y) calloc_debug(x, y, __FILE__, __LINE__)
#define STRDUP(x) strdup_debug(x, __FILE__, __LINE__)

#if __cplusplus
extern "C" {
#endif
extern void *malloc_debug(size_t, const char *, int);
extern void free_debug(void *, const char *, int);
extern void *realloc_debug(void *, size_t, const char *, int);
extern void *calloc_debug(size_t, size_t, const char *, int);
extern char *strdup_debug(const char *, const char *, int);
#if __cplusplus
}
#endif
#else
#define MALLOC(x) malloc(x)
#define FREE(x) free(x)
#define FREE_IF(x)                                                             \
	do {                                                                   \
		if ((x) != 0)                                                  \
			free((void *)(x));                                     \
	} while (0)
#define FREE_CLEAR(x)                                                          \
	do {                                                                   \
		if ((x) != 0) {                                                \
			free((void *)(x));                                     \
			(x) = 0;                                               \
		}                                                              \
	} while (0)
#if ROCKSDB_COMPILER
#define REALLOC(p, sz)                                                         \
	({                                                                     \
		void *a = realloc(p, sz);                                      \
		if (a)                                                         \
			p = (decltype(p))a;                                    \
		a;                                                             \
	})
#else
#define REALLOC(p, sz)                                                         \
	({                                                                     \
		void *a = realloc(p, sz);                                      \
		if (a)                                                         \
			p = (typeof(p))a;                                      \
		a;                                                             \
	})
#endif
#define CALLOC(x, y) calloc(x, y)
#define STRDUP(x) (x) ? strdup(x) : NULL
#endif

#if __cplusplus

#if MEMCHECK
extern void enable_memchecker(void);
extern void dump_non_delete(void);
extern void report_mallinfo(void);
extern unsigned long count_virtual_size(void);
extern unsigned long count_alloc_size(void);
#else
static inline void enable_memchecker(void)
{
}
static inline void dump_non_delete(void)
{
}
static inline void report_mallinfo(void)
{
}
#endif

#define NEW(type, pointer)                                                     \
	do {                                                                   \
		try {                                                          \
			pointer = 0;                                           \
			pointer = new type;                                    \
		} catch (...) {                                                \
			pointer = 0;                                           \
		}                                                              \
	} while (0)

#define DELETE(pointer)                                                        \
	do {                                                                   \
		if (pointer) {                                                 \
			delete pointer;                                        \
			pointer = 0;                                           \
		}                                                              \
	} while (0)

#define DEC_DELETE(pointer)                                                    \
	do {                                                                   \
		if (pointer && pointer->decrease() == 0) {                          \
			delete pointer;                                        \
			pointer = 0;                                           \
		}                                                              \
	} while (0)

#define NEW_ARRAY(n, type, pointer)                                            \
	do {                                                                   \
		try {                                                          \
			pointer = 0;                                           \
			pointer = new type[n];                                 \
		} catch (...) {                                                \
			pointer = 0;                                           \
		}                                                              \
	} while (0)

#define DELETE_ARRAY(pointer)                                                  \
	do {                                                                   \
		delete[] pointer;                                              \
		pointer = 0;                                                   \
	} while (0)

#endif

#endif
