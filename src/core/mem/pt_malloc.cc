/*
* Copyright [2021] JD.com, Inc.
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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "log/log.h"
#include "pt_malloc.h"
#include "algorithm/singleton.h"

DTC_USING_NAMESPACE

/* conversion from malloc headers to user pointers, and back */
#define chunk2mem(h) (void *)(((char *)h) + 2 * sizeof(ALLOC_SIZE_T))
#define mem2chunk(h) (void *)(((char *)h) - 2 * sizeof(ALLOC_SIZE_T))
#define chunkhandle2memhandle(handle) (handle + 2 * sizeof(ALLOC_SIZE_T))
#define memhandle2chunkhandle(handle) (handle - 2 * sizeof(ALLOC_SIZE_T))
#if BIN_MEM_CHECK
#define chunksize2memsize(size) (size - 2 * sizeof(ALLOC_SIZE_T))
#define checked_chunksize2memsize(size)                                        \
	(size > 2 * sizeof(ALLOC_SIZE_T) ? (size - 2 * sizeof(ALLOC_SIZE_T)) : \
					   0)
#else
#define chunksize2memsize(size) (size - sizeof(ALLOC_SIZE_T))
#define checked_chunksize2memsize(size)                                        \
	(size > sizeof(ALLOC_SIZE_T) ? (size - sizeof(ALLOC_SIZE_T)) : 0)
#endif

/* Check if m has acceptable alignment */

#define aligned_OK(m) (((unsigned long)(m)&MALLOC_ALIGN_MASK) == 0)

#define misaligned_chunk(h)                                                    \
	((MALLOC_ALIGNMENT == 2 * SIZE_SZ ? (h) : chunkhandle2memhandle(h)) &  \
	 MALLOC_ALIGN_MASK)

/*
   Check if a request is so large that it would wrap around zero when
   padded and aligned. To simplify some other code, the bound is made
   low enough so that adding MINSIZE will also not wrap around zero.
*/

#define REQUEST_OUT_OF_RANGE(req)                                              \
	((unsigned long)(req) >= (unsigned long)(ALLOC_SIZE_T)(-2 * MINSIZE))

/* pad request bytes into a usable size -- internal version */
#if BIN_MEM_CHECK
#define request2size(req)                                                      \
	(((req) + 2 * SIZE_SZ + MALLOC_ALIGN_MASK < MINSIZE) ?                 \
		 MINSIZE :                                                     \
		 ((req) + 2 * SIZE_SZ + MALLOC_ALIGN_MASK) &                   \
			 ~MALLOC_ALIGN_MASK)
#else
#define request2size(req)                                                      \
	(((req) + SIZE_SZ + MALLOC_ALIGN_MASK < MINSIZE) ?                     \
		 MINSIZE :                                                     \
		 ((req) + SIZE_SZ + MALLOC_ALIGN_MASK) & ~MALLOC_ALIGN_MASK)
#endif

/*  Same, except also perform argument check */

#define checked_request2size(req, sz)                                          \
	if (REQUEST_OUT_OF_RANGE(req)) {                                       \
		return (INVALID_HANDLE);                                       \
	}                                                                      \
	(sz) = request2size(req);

/*
  --------------- Physical chunk operations ---------------
*/
/* size field is or'ed with PREV_INUSE when previous adjacent chunk in use */
#define PREV_INUSE 0x1
#define RESERVE_BITS (0x2 | 0x4)
/*
  Bits to mask off when extracting size
*/
#define SIZE_BITS (PREV_INUSE | RESERVE_BITS)

/* Get size, ignoring use bits */
#define CHUNK_SIZE(p) ((p)->m_tSize & ~(SIZE_BITS))
#define REAL_SIZE(sz) ((sz) & ~(SIZE_BITS))

/* extract inuse bit of previous chunk */
#define prev_inuse(p) ((p)->m_tSize & PREV_INUSE)
#define inuse_bit_at_offset(p, offset)                                         \
	(((MallocChunk *)(((char *)p) + offset))->m_tSize & PREV_INUSE)
#define set_inuse_bit_at_offset(p, s)                                          \
	(((MallocChunk *)(((char *)(p)) + (s)))->m_tSize |= PREV_INUSE)
#define clear_inuse_bit_at_offset(p, s)                                        \
	(((MallocChunk *)(((char *)(p)) + (s)))->m_tSize &= ~(PREV_INUSE))
#define set_size_at_offset(p, offset, size)                                    \
	(((MallocChunk *)(((char *)p) + (offset)))->m_tSize =                  \
		 REAL_SIZE(size) |                                             \
		 (((MallocChunk *)(((char *)p) + (offset)))->m_tSize &         \
		  SIZE_BITS))
#define set_presize_at_offset(p, offset, size)                                 \
	(((MallocChunk *)(((char *)p) + (offset)))->m_tPreSize =               \
		 REAL_SIZE(size))

#define in_smallbin_range(sz)                                                  \
	((unsigned long)(sz) < (unsigned long)MIN_LARGE_SIZE)

#define smallbin_index(sz) (((unsigned)(sz)) >> 3)

#define largebin_index(sz)                                                     \
	(((((unsigned long)(sz)) >> 6) <= 32) ?                                \
		 56 + (((unsigned long)(sz)) >> 6) :                           \
		 ((((unsigned long)(sz)) >> 9) <= 20) ?                        \
		 91 + (((unsigned long)(sz)) >> 9) :                           \
		 ((((unsigned long)(sz)) >> 12) <= 10) ?                       \
		 110 + (((unsigned long)(sz)) >> 12) :                         \
		 ((((unsigned long)(sz)) >> 15) <= 4) ?                        \
		 119 + (((unsigned long)(sz)) >> 15) :                         \
		 ((((unsigned long)(sz)) >> 18) <= 2) ?                        \
		 124 + (((unsigned long)(sz)) >> 18) :                         \
		 126)

#define bin_index(sz)                                                          \
	((in_smallbin_range(sz)) ? smallbin_index(sz) : largebin_index(sz))

#define NFASTBINS NSMALLBINS
#define FAST_MAX_SIZE MIN_LARGE_SIZE
#define fastbin_index(sz) smallbin_index(sz)

#define AT_TOP(chunk, sz)                                                      \
	(((char *)chunk) + sz == ((char *)m_pBaseAddr) + m_pstHead->m_hTop)

#define CAN_COMBILE(size, add)                                                 \
	((INTER_SIZE_T)size + add <= (INTER_SIZE_T)MAX_ALLOC_SIZE)

PtMalloc::PtMalloc()
{
	m_pBaseAddr = NULL;
	m_pstHead = NULL;
	m_ptBin = NULL;
	m_ptFastBin = NULL;
	m_ptUnsortedBin = NULL;
	statChunkTotal = g_stat_mgr.get_stat_int_counter(DTC_CHUNK_TOTAL);
	statDataSize = g_stat_mgr.get_stat_iterm(DTC_DATA_SIZE);
	statMemoryTop = g_stat_mgr.get_stat_iterm(DTC_MEMORY_TOP);
	statTmpDataSizeRecently = 0;
	statTmpDataAllocCountRecently = 0;
	statAverageDataSizeRecently =
		g_stat_mgr.get_stat_iterm(DATA_SIZE_AVG_RECENT);
	memset(err_message_, 0, sizeof(err_message_));
	minChunkSize = MINSIZE;
}

PtMalloc::~PtMalloc()
{
}

PtMalloc *PtMalloc::instance()
{
	return Singleton<PtMalloc>::instance();
}

void PtMalloc::destroy()
{
	Singleton<PtMalloc>::destory();
}
/*初始化header中的signature域*/
void PtMalloc::init_sign()
{
	static const unsigned int V4Sign[14] = {
		DTC_SIGN_0, DTC_SIGN_1, DTC_SIGN_2, DTC_SIGN_3, DTC_SIGN_4,
		DTC_SIGN_5, DTC_SIGN_6, DTC_SIGN_7, DTC_SIGN_8, DTC_SIGN_9,
		DTC_SIGN_A, DTC_SIGN_B, DTC_SIGN_C, DTC_SIGN_D
	};

	memcpy(m_pstHead->m_auiSign, V4Sign, sizeof(m_pstHead->m_auiSign));
}

#if __WORDSIZE == 64
#define UINT64FMT_T "%lu"
#else
#define UINT64FMT_T "%llu"
#endif
/*初始化cache头信息*/
/*传入参数，cache的起始地址，cache的总大小*/
int PtMalloc::do_init(void *pAddr, INTER_SIZE_T tSize)
{
	int i;

	if (tSize < sizeof(MemHead) + sizeof(CBin) * (NBINS + NFASTBINS + 1) +
			    DTC_RESERVE_SIZE + MINSIZE) {
		snprintf(err_message_, sizeof(err_message_),
			 "invalid size[" UINT64FMT_T "]", tSize);
		return (-1);
	}

	m_pBaseAddr = pAddr;
	m_pstHead = (MemHead *)m_pBaseAddr;
	memset(m_pstHead, 0, sizeof(MemHead));
	init_sign();
	m_pstHead->m_ushVer = DTC_VER_MIN;
	m_pstHead->m_ushHeadSize = sizeof(MemHead);
	m_pstHead->m_tSize = tSize;
	m_pstHead->m_tUserAllocChunkCnt = 0;
	m_pstHead->m_hReserveZone =
		sizeof(MemHead) + sizeof(CBin) * (NBINS + NFASTBINS + 1);
	m_pstHead->m_hReserveZone =
		(m_pstHead->m_hReserveZone + MALLOC_ALIGN_MASK) &
		~MALLOC_ALIGN_MASK;
	m_pstHead->m_hBottom = (m_pstHead->m_hReserveZone + DTC_RESERVE_SIZE +
				MALLOC_ALIGN_MASK) &
			       ~MALLOC_ALIGN_MASK;
	m_pstHead->m_hTop = m_pstHead->m_hBottom;
	m_pstHead->m_tUserAllocSize = m_pstHead->m_hBottom;
	statMemoryTop = m_pstHead->m_hTop;
	m_pstHead->m_tLastFreeChunkSize =
		(tSize > m_pstHead->m_hTop + MINSIZE) ?
			(tSize - m_pstHead->m_hTop - MINSIZE) :
			0;
	m_pstHead->m_ushBinCnt = NBINS;
	m_pstHead->m_ushFastBinCnt = NFASTBINS;
	memset(m_pstHead->m_auiBinBitMap, 0, sizeof(m_pstHead->m_auiBinBitMap));
	m_ptBin = (CBin *)(((char *)m_pBaseAddr) + sizeof(MemHead));
	m_ptFastBin = m_ptBin + NBINS;
	m_ptUnsortedBin = m_ptFastBin + NFASTBINS;

	for (i = 0; i < NBINS; i++) {
		m_ptBin[i].m_hPreChunk = INVALID_HANDLE;
		m_ptBin[i].m_hNextChunk = INVALID_HANDLE;
	}

	for (i = 0; i < NFASTBINS; i++) {
		m_ptFastBin[i].m_hPreChunk = INVALID_HANDLE;
		m_ptFastBin[i].m_hNextChunk = INVALID_HANDLE;
	}

	m_ptUnsortedBin[0].m_hPreChunk = INVALID_HANDLE;
	m_ptUnsortedBin[0].m_hNextChunk = INVALID_HANDLE;

	MallocChunk *pstChunk;
	pstChunk = (MallocChunk *)handle_to_ptr(m_pstHead->m_hTop);
	pstChunk->m_tPreSize = 0;
	pstChunk->m_tSize = PREV_INUSE;

	// init stat
	statChunkTotal = m_pstHead->m_tUserAllocChunkCnt;
	statDataSize = m_pstHead->m_tUserAllocSize;

	return (0);
}
/*校验cache的版本是否正确*/
int PtMalloc::detect_version()
{
	if (m_pstHead->m_auiSign[0] != DTC_SIGN_0 ||
	    m_pstHead->m_auiSign[1] != DTC_SIGN_1)
		return 1;
	if (m_pstHead->m_ushVer == 2)
		return (2);
	if (m_pstHead->m_ushVer == 3)
		return (3);
	if (m_pstHead->m_ushVer == 4)
		return (4);

	snprintf(err_message_, sizeof(err_message_),
		 "unknown version signature %u", m_pstHead->m_ushVer);
	return (0);
}
/*查看cache是否一致：在启动dtc，加载cache的时候，只要是需要写cache，就会设置不一致，防止dtc在运行时crash，重启后不经检查使用乱掉的内存*/
int PtMalloc::share_memory_integrity()
{
	return (int)m_pstHead->m_shmIntegrity;
}

void PtMalloc::set_share_memory_integrity(const int flags)
{
	if (flags)
		m_pstHead->m_shmIntegrity = 1;
	else
		m_pstHead->m_shmIntegrity = 0;
}
/*对于已经存在的IPC shared memory，dtc在启动后会将这个块内存作为cache，在这里检查这块cache的头信息，是否正确*/
int PtMalloc::do_attach(void *pAddr, INTER_SIZE_T tSize)
{
	if (tSize < sizeof(MemHead) + sizeof(CBin) * (NBINS + NFASTBINS + 1) +
			    MINSIZE) {
		snprintf(err_message_, sizeof(err_message_),
			 "invalid size[" UINT64FMT_T "]", tSize);
		return (-1);
	}

	m_pBaseAddr = pAddr;
	m_pstHead = (MemHead *)m_pBaseAddr;
	if (detect_version() != DTC_VER_MIN) {
		snprintf(err_message_, sizeof(err_message_),
			 "Unsupported preferred version %u",
			 m_pstHead->m_ushVer);
		return (-2);
	}

	if (m_pstHead->m_tSize != tSize) {
		snprintf(err_message_, sizeof(err_message_),
			 "invalid argument");
		return (-3);
	}
	if (m_pstHead->m_hTop >= m_pstHead->m_tSize) {
		snprintf(err_message_, sizeof(err_message_),
			 "memory corruption-invalid bottom value");
		return (-4);
	}
	m_ptBin = (CBin *)(((char *)m_pBaseAddr) + sizeof(MemHead));
	m_ptFastBin = m_ptBin + NBINS;
	m_ptUnsortedBin = m_ptFastBin + NFASTBINS;

	// init stat
	statChunkTotal = m_pstHead->m_tUserAllocChunkCnt;
	statDataSize = m_pstHead->m_tUserAllocSize;

	return (0);
}

ALLOC_HANDLE_T PtMalloc::get_reserve_zone()
{
	return m_pstHead->m_hReserveZone;
}
/*输入参数是chunk的用户handle*/
/*返回这块chunk的用户使用空间的大小*/
ALLOC_SIZE_T PtMalloc::chunk_size(ALLOC_HANDLE_T hHandle)
{
	MallocChunk *pstChunk;

	if (hHandle >= m_pstHead->m_hTop || hHandle <= m_pstHead->m_hBottom) {
		snprintf(err_message_, sizeof(err_message_),
			 "[chunk_size]-invalid handle");
		return (0);
	}

	pstChunk = (MallocChunk *)mem2chunk(handle_to_ptr(hHandle));

	if (check_inuse_chunk(pstChunk) != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "[chunk_size]-invalid chunk");
		return (0);
	}

	return chunksize2memsize(CHUNK_SIZE(pstChunk));
}
/*设置输入bin上的头chunk为使用状态，并将这个chunk从bin上拖链*/
void *PtMalloc::bin_malloc(CBin &ptBin)
{
	MallocChunk *pstChunk;
	void *p;

	if (ptBin.m_hNextChunk == INVALID_HANDLE)
		return (NULL);

	p = handle_to_ptr(ptBin.m_hNextChunk);
	pstChunk = (MallocChunk *)p;
	set_inuse_bit_at_offset(pstChunk, REAL_SIZE(pstChunk->m_tSize));
	unlink_bin(ptBin, ptBin.m_hNextChunk);

	return p;
}
/*对所有的bin检查：small&large bins, fast bins, unsorted bins*/
/*校验方法：每个bin组成一个双向的循环链表*/
int PtMalloc::check_bin()
{
	int i;

	INTER_HANDLE_T hHandle;
	MallocChunk *pstChunk;
	for (i = 0; i < NBINS; i++) {
		hHandle = m_ptBin[i].m_hNextChunk;
		if (hHandle != INVALID_HANDLE) {
			do {
				pstChunk =
					(MallocChunk *)handle_to_ptr(hHandle);
				if (pstChunk->m_hNextChunk != INVALID_HANDLE)
					hHandle = pstChunk->m_hNextChunk;
			} while (pstChunk->m_hNextChunk != INVALID_HANDLE);
		}
		if (m_ptBin[i].m_hPreChunk != hHandle) {
			snprintf(err_message_, sizeof(err_message_),
				 "bad bin[%d]", i);
			return (-1);
		}
	}

	for (i = 0; i < NFASTBINS; i++) {
		hHandle = m_ptFastBin[i].m_hNextChunk;
		if (hHandle != INVALID_HANDLE) {
			do {
				pstChunk =
					(MallocChunk *)handle_to_ptr(hHandle);
				if (pstChunk->m_hNextChunk != INVALID_HANDLE)
					hHandle = pstChunk->m_hNextChunk;
			} while (pstChunk->m_hNextChunk != INVALID_HANDLE);
		}
		if (m_ptFastBin[i].m_hPreChunk != hHandle) {
			snprintf(err_message_, sizeof(err_message_),
				 "bad fast-bin[%d]", i);
			return (-2);
		}
	}

	hHandle = m_ptUnsortedBin[0].m_hNextChunk;
	if (hHandle != INVALID_HANDLE) {
		do {
			pstChunk = (MallocChunk *)handle_to_ptr(hHandle);
			if (pstChunk->m_hNextChunk != INVALID_HANDLE)
				hHandle = pstChunk->m_hNextChunk;
		} while (pstChunk->m_hNextChunk != INVALID_HANDLE);
	}
	if (m_ptUnsortedBin[0].m_hPreChunk != hHandle) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "bad unsorted-bin[%d] %lu!=%lu", 0,
			 m_ptUnsortedBin[0].m_hPreChunk, hHandle);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "bad unsorted-bin[%d] %llu!=%llu", 0,
			 m_ptUnsortedBin[0].m_hPreChunk, hHandle);
#endif
		return (-3);
	}

	return (0);
}
/*校验存放在bin中的chunk的一致性*/
/*检验方法：从分配的top线开始向bottom方向，一个chunk一个chunk的检查，检查这个chunk的大小是不是和它的后一个chunk的presize一致*/
#if BIN_MEM_CHECK
int PtMalloc::check_mem()
{
	INTER_HANDLE_T hHandle;
	MallocChunk *pstChunk;
	ALLOC_SIZE_T tSize;

	tSize = 0;
	hHandle = m_pstHead->m_hTop;
	while (hHandle > m_pstHead->m_hBottom) {
		pstChunk = (MallocChunk *)handle_to_ptr(hHandle);
		if (CHUNK_SIZE(pstChunk) != tSize) {
#if __WORDSIZE == 64
			snprintf(err_message_, sizeof(err_message_),
				 "bad memory1 handle[%lu]", hHandle);
#else
			snprintf(err_message_, sizeof(err_message_),
				 "bad memory1 handle[%llu]", hHandle);
#endif
			return (-1);
		}
		tSize = pstChunk->m_tPreSize;
		if (hHandle < tSize) {
#if __WORDSIZE == 64
			snprintf(err_message_, sizeof(err_message_),
				 "bad memory handle[%lu]", hHandle);
#else
			snprintf(err_message_, sizeof(err_message_),
				 "bad memory handle[%llu]", hHandle);
#endif
			return (-2);
		}
		hHandle -= tSize;
	}

	return (0);
}
#endif
/*从fastbins的一个bin下取一个空闲chunk,满足tsize大小。*/
/*bin的索引查找方法是：按照在smallbins中查找bin的方法进行*/
void *PtMalloc::fast_malloc(ALLOC_SIZE_T tSize)
{
	return bin_malloc(m_ptFastBin[smallbin_index(tSize)]);
}
/*从smallbins的一个bin下取一个空闲chunk满足tsize大小*/
void *PtMalloc::small_bin_malloc(ALLOC_SIZE_T tSize)
{
	void *p;
	unsigned int uiBinIdx;

	uiBinIdx = smallbin_index(tSize);
	p = bin_malloc(m_ptBin[uiBinIdx]);
	if (empty_bin(uiBinIdx))
		clear_bin_bit_map(uiBinIdx);

	return (p);
}
/*释放fastbins的每个bin下的空闲chunk*/
/*对于每个chunk试探是否可以和内存里的前后chunk合并，合并如果可以，并设置新chunk为使用状态，并从bin上拖链，最后将拖链的chunk存放在unsortedbin下*/
int PtMalloc::free_fast()
{
	if (!(m_pstHead->m_uiFlags & MALLOC_FLAG_FAST)) // no fast chunk
		return (0);

	for (int i = 0; i < NFASTBINS; i++) {
		if (m_ptFastBin[i].m_hNextChunk != INVALID_HANDLE) {
			MallocChunk *pstChunk;
			//			MallocChunk* pstPreChunk;
			MallocChunk *pstNextChunk;
			ALLOC_SIZE_T tSize;
			ALLOC_SIZE_T tPreSize;
			//			ALLOC_SIZE_T tNextSize;
			unsigned int uiBinIdx;

			do { // free fast-chunk & put it into unsorted chunk list
				pstChunk = (MallocChunk *)handle_to_ptr(
					m_ptFastBin[i].m_hNextChunk);
				unlink_bin(m_ptFastBin[i],
					   m_ptFastBin[i].m_hNextChunk);

				tSize = CHUNK_SIZE(pstChunk);
				if (!prev_inuse(pstChunk) &&
				    CAN_COMBILE(tSize, pstChunk->m_tPreSize)) {
					tPreSize = pstChunk->m_tPreSize;
					tSize += tPreSize;
					pstChunk =
						(MallocChunk
							 *)(((char *)pstChunk) -
							    tPreSize);

					uiBinIdx = bin_index(tPreSize);
					unlink_bin(m_ptBin[uiBinIdx],
						   ptr_to_handle(pstChunk));
					if (empty_bin(uiBinIdx))
						clear_bin_bit_map(uiBinIdx);
					set_inuse_bit_at_offset(pstChunk,
								tSize);
				}

				if (!AT_TOP(pstChunk, tSize)) {
					pstNextChunk =
						(MallocChunk
							 *)(((char *)pstChunk) +
							    tSize);
					ALLOC_SIZE_T tNextSize =
						CHUNK_SIZE(pstNextChunk);
					uiBinIdx = bin_index(tNextSize);
					if (!inuse_bit_at_offset(pstNextChunk,
								 tNextSize) &&
					    CAN_COMBILE(tSize, tNextSize)) {
						tSize += tNextSize;
						unlink_bin(
							m_ptBin[uiBinIdx],
							ptr_to_handle(
								pstNextChunk));
						if (empty_bin(uiBinIdx))
							clear_bin_bit_map(
								uiBinIdx);
						set_inuse_bit_at_offset(
							pstChunk, tSize);
					} else {
						//						clear_inuse_bit_at_offset(pstNextChunk, 0);
					}
				}

				if (m_pstHead->m_tLastFreeChunkSize <
				    REAL_SIZE(tSize))
					m_pstHead->m_tLastFreeChunkSize =
						REAL_SIZE(tSize);
				pstChunk->m_tSize =
					REAL_SIZE(tSize) |
					(pstChunk->m_tSize & SIZE_BITS);
				if (AT_TOP(pstChunk, tSize)) {
					// combine into bottom
					m_pstHead->m_hTop -= tSize;
					statMemoryTop = m_pstHead->m_hTop;
					//					clear_inuse_bit_at_offset(pstChunk, 0);
				} else {
					link_bin(m_ptUnsortedBin[0],
						 ptr_to_handle(pstChunk));
				}
				pstNextChunk =
					(MallocChunk *)(((char *)pstChunk) +
							REAL_SIZE(tSize));
				pstNextChunk->m_tPreSize = REAL_SIZE(tSize);

			} while (m_ptFastBin[i].m_hNextChunk != INVALID_HANDLE);
		}
	}

	m_pstHead->m_uiFlags &= ~MALLOC_FLAG_FAST;

	return (0);
}
/*从top线上面分配一个chunk满足tsize*/
void *PtMalloc::top_alloc(ALLOC_SIZE_T tSize)
{
	if (m_pstHead->m_hTop + tSize + MINSIZE >= m_pstHead->m_tSize) {
		snprintf(err_message_, sizeof(err_message_), "out of memory");
		return (NULL);
	}

	void *p;
	MallocChunk *pstChunk;
	pstChunk = (MallocChunk *)handle_to_ptr(m_pstHead->m_hTop);
	pstChunk->m_tSize = (pstChunk->m_tSize & SIZE_BITS) | REAL_SIZE(tSize);
	p = (void *)pstChunk;

	pstChunk = (MallocChunk *)(((char *)pstChunk) + tSize);
	pstChunk->m_tPreSize = REAL_SIZE(tSize);
	pstChunk->m_tSize = PREV_INUSE;

	m_pstHead->m_hTop += tSize;
	statMemoryTop = m_pstHead->m_hTop;

	return chunk2mem(p);
}
/*从输入的bin上将handle指定的chunk拖链*/
int PtMalloc::unlink_bin(CBin &stBin, INTER_HANDLE_T hHandle)
{
	MallocChunk *pstChunk;
	MallocChunk *pstTmp;

	if (hHandle == INVALID_HANDLE)
		return (-1);

	if (stBin.m_hNextChunk == INVALID_HANDLE ||
	    stBin.m_hPreChunk == INVALID_HANDLE) {
		snprintf(err_message_, sizeof(err_message_),
			 "unlink-bin: bad bin!");
		return (-2);
	}

	pstChunk = (MallocChunk *)handle_to_ptr(hHandle);
	if (pstChunk->m_hPreChunk == INVALID_HANDLE) {
		//remove head
		stBin.m_hNextChunk = pstChunk->m_hNextChunk;
	} else {
		pstTmp = (MallocChunk *)handle_to_ptr(pstChunk->m_hPreChunk);
		pstTmp->m_hNextChunk = pstChunk->m_hNextChunk;
	}
	if (pstChunk->m_hNextChunk == INVALID_HANDLE) {
		stBin.m_hPreChunk = pstChunk->m_hPreChunk;
	} else {
		pstTmp = (MallocChunk *)handle_to_ptr(pstChunk->m_hNextChunk);
		pstTmp->m_hPreChunk = pstChunk->m_hPreChunk;
	}

	return (0);
}
/*将handle指定的chunk插入到bin上*/
int PtMalloc::link_bin(CBin &stBin, INTER_HANDLE_T hHandle)
{
	MallocChunk *pstChunk;
	MallocChunk *pstTmp;

	if (hHandle == INVALID_HANDLE)
		return (-1);

	pstChunk = (MallocChunk *)handle_to_ptr(hHandle);
	pstChunk->m_hNextChunk = stBin.m_hNextChunk;
	pstChunk->m_hPreChunk = INVALID_HANDLE;
	if (stBin.m_hNextChunk != INVALID_HANDLE) {
		pstTmp = (MallocChunk *)handle_to_ptr(stBin.m_hNextChunk);
		pstTmp->m_hPreChunk = hHandle;
		if (stBin.m_hPreChunk == INVALID_HANDLE) {
			snprintf(err_message_, sizeof(err_message_),
				 "link-bin: bad bin");
			return (-2);
		}
	} else {
		if (stBin.m_hPreChunk != INVALID_HANDLE) {
			snprintf(err_message_, sizeof(err_message_),
				 "link-bin: bad bin");
			return (-3);
		}
		stBin.m_hPreChunk = hHandle;
	}
	stBin.m_hNextChunk = hHandle;

	return (0);
}
/*在bin中查找一个合适的位置，将hanlde指定的chunk插入进去*/
/*寻找位置的方法：从bin的尾部开始，找到第一个位置，它的大小介于前后chunk的大小之间*/
int PtMalloc::link_sorted_bin(CBin &stBin, INTER_HANDLE_T hHandle,
			      ALLOC_SIZE_T tSize)
{
	MallocChunk *pstChunk;
	MallocChunk *pstNextChunk;

	if (hHandle == INVALID_HANDLE)
		return (-1);

	pstChunk = (MallocChunk *)handle_to_ptr(hHandle);
	pstChunk->m_hNextChunk = INVALID_HANDLE;
	pstChunk->m_hPreChunk = INVALID_HANDLE;

	if (stBin.m_hNextChunk == INVALID_HANDLE) { // empty bin
		pstChunk->m_hPreChunk = INVALID_HANDLE;
		pstChunk->m_hNextChunk = INVALID_HANDLE;
		stBin.m_hNextChunk = hHandle;
		stBin.m_hPreChunk = hHandle;
	} else {
		INTER_HANDLE_T hPre;
		hPre = stBin.m_hPreChunk;
		tSize = REAL_SIZE(tSize) | PREV_INUSE;
		MallocChunk *pstPreChunk = 0;
		while (hPre != INVALID_HANDLE) {
			pstPreChunk = (MallocChunk *)handle_to_ptr(hPre);
			if (tSize <= pstPreChunk->m_tSize)
				break;
			hPre = pstPreChunk->m_hPreChunk;
		}
		if (hPre == INVALID_HANDLE) {
			if (stBin.m_hPreChunk == INVALID_HANDLE) {
				// empty list
				snprintf(err_message_, sizeof(err_message_),
					 "memory corruction");
				return (-1);
			}

			// place chunk at list head
			link_bin(stBin, hHandle);
		} else {
			pstChunk->m_hPreChunk = hPre;
			pstChunk->m_hNextChunk = pstPreChunk->m_hNextChunk;
			pstPreChunk->m_hNextChunk = hHandle;
			if (pstChunk->m_hNextChunk != INVALID_HANDLE) {
				pstNextChunk = (MallocChunk *)handle_to_ptr(
					pstChunk->m_hNextChunk);
				pstNextChunk->m_hPreChunk =
					ptr_to_handle(pstChunk);
			} else {
				// list tail
				stBin.m_hPreChunk = hHandle;
			}
		}
	}

	return (0);
}
/*分配chunk满足tsize的主体逻辑*/
ALLOC_HANDLE_T PtMalloc::inter_malloc(ALLOC_SIZE_T tSize)
{
	void *p;

	checked_request2size(tSize, tSize);

	/* no more use fast bin
	if(tSize < FAST_MAX_SIZE){
		p = fast_malloc(tSize);
		if(p != NULL)
			return ptr_to_handle(chunk2mem(p));
	}
	*/

	if (in_smallbin_range(tSize)) {
		p = small_bin_malloc(tSize);
		if (p != NULL)
			return ptr_to_handle(chunk2mem(p));
	}

	for (;;) {
		MallocChunk *pstChunk = NULL;
		MallocChunk *pstNextChunk = NULL;

		unsigned int uiBinIdx = bin_index(tSize);
		if (!in_smallbin_range(tSize)) {
			INTER_HANDLE_T v = m_ptBin[uiBinIdx].m_hNextChunk;
			unsigned int try_search_count = 0;

			/* 每个bin最多只搜索100次，如果失败则跳至下一个bin */
			while (v != INVALID_HANDLE &&
			       ++try_search_count < 100) {
				pstChunk = (MallocChunk *)handle_to_ptr(v);
				if (CHUNK_SIZE(pstChunk) >= tSize)
					break;

				v = pstChunk->m_hNextChunk;
			}

			if (!(v != INVALID_HANDLE && try_search_count < 100))
				goto SEARCH_NEXT_BIN;

			ALLOC_SIZE_T tRemainSize;
			tRemainSize = CHUNK_SIZE(pstChunk) - tSize;
			// unlink
			unlink_bin(m_ptBin[uiBinIdx], ptr_to_handle(pstChunk));
			if (empty_bin(uiBinIdx))
				clear_bin_bit_map(uiBinIdx);

			if (tRemainSize < get_min_chunk_size()) {
				set_inuse_bit_at_offset(pstChunk,
							CHUNK_SIZE(pstChunk));
			} else {
				pstChunk->m_tSize =
					tSize | (pstChunk->m_tSize & SIZE_BITS);
				pstNextChunk =
					(MallocChunk *)(((char *)pstChunk) +
							tSize);
				pstNextChunk->m_tSize = tRemainSize;
				pstNextChunk->m_tPreSize = tSize;
				set_inuse_bit_at_offset(pstNextChunk, 0);
				((MallocChunk *)(((char *)pstChunk) + tSize +
						 tRemainSize))
					->m_tPreSize = tRemainSize;
				set_inuse_bit_at_offset(pstNextChunk,
							tRemainSize);
				ALLOC_SIZE_T user_size;
				inter_free(chunkhandle2memhandle(
						   ptr_to_handle(pstNextChunk)),
					   user_size);
			}

			p = (void *)pstChunk;
			return ptr_to_handle(chunk2mem(p));
		}

		/*
		   do_search for a chunk by scanning bins, starting with next largest
		   bin. This search is strictly by best-fit; i.e., the smallest
		   (with ties going to approximately the least recently used) chunk
		   that fits is selected.
		   */
	SEARCH_NEXT_BIN:
		uiBinIdx++;
		unsigned int uiBitMapIdx = uiBinIdx / 32;
		if (m_pstHead->m_auiBinBitMap[uiBitMapIdx] == 0) {
			uiBitMapIdx++;
			uiBinIdx = uiBitMapIdx * 32;
			while (uiBitMapIdx <
				       sizeof(m_pstHead->m_auiBinBitMap) &&
			       m_pstHead->m_auiBinBitMap[uiBitMapIdx] == 0) {
				uiBitMapIdx++;
				uiBinIdx += 32;
			}
		}
		while (uiBinIdx < NBINS &&
		       m_ptBin[uiBinIdx].m_hNextChunk == INVALID_HANDLE)
			uiBinIdx++;

		if (uiBinIdx >= NBINS) {
			goto MALLOC_BOTTOM;
		}

		INTER_HANDLE_T hPre;
		hPre = m_ptBin[uiBinIdx].m_hPreChunk;
		do {
			pstChunk = (MallocChunk *)handle_to_ptr(hPre);
			hPre = pstChunk->m_hPreChunk;
		} while (CHUNK_SIZE(pstChunk) < tSize);
		ALLOC_SIZE_T tRemainSize;
		tRemainSize = CHUNK_SIZE(pstChunk) - tSize;
		// unlink
		unlink_bin(m_ptBin[uiBinIdx], ptr_to_handle(pstChunk));
		if (empty_bin(uiBinIdx))
			clear_bin_bit_map(uiBinIdx);

		if (tRemainSize < get_min_chunk_size()) {
			set_inuse_bit_at_offset(pstChunk, CHUNK_SIZE(pstChunk));
		} else {
			/* disable unsorted bins */
			pstChunk->m_tSize =
				tSize | (pstChunk->m_tSize & SIZE_BITS);
			pstNextChunk =
				(MallocChunk *)(((char *)pstChunk) + tSize);
			pstNextChunk->m_tSize = tRemainSize;
			pstNextChunk->m_tPreSize = tSize;
			set_inuse_bit_at_offset(pstNextChunk, 0);
			((MallocChunk *)(((char *)pstChunk) + tSize +
					 tRemainSize))
				->m_tPreSize = tRemainSize;
			set_inuse_bit_at_offset(pstNextChunk, tRemainSize);
			ALLOC_SIZE_T user_size;
			inter_free(chunkhandle2memhandle(
					   ptr_to_handle(pstNextChunk)),
				   user_size);
		}

		p = (void *)pstChunk;
		return ptr_to_handle(chunk2mem(p));
	}

MALLOC_BOTTOM:
	return ptr_to_handle(top_alloc(tSize));
}
/*对intermalloc的包装，对返回结果进行了简单检查*/
ALLOC_HANDLE_T PtMalloc::Malloc(ALLOC_SIZE_T tSize)
{
	MallocChunk *pstChunk;

	m_pstHead->m_tLastFreeChunkSize = 0;
	ALLOC_HANDLE_T hHandle = inter_malloc(tSize);
	if (hHandle != INVALID_HANDLE) {
		//		log4cplus_error("MALLOC: %lu", hHandle);
		pstChunk = (MallocChunk *)mem2chunk(handle_to_ptr(hHandle));
		m_pstHead->m_tUserAllocSize += CHUNK_SIZE(pstChunk);
		m_pstHead->m_tUserAllocChunkCnt++;
		++statChunkTotal;
		statDataSize = m_pstHead->m_tUserAllocSize;
		add_alloc_size_to_stat(tSize);
	}
	return (hHandle);
}
/*对intermalloc的包装，对返回结果进行了简单检查,并将返回的chunk的用户部分清空*/
ALLOC_HANDLE_T PtMalloc::Calloc(ALLOC_SIZE_T tSize)
{
	ALLOC_HANDLE_T hHandle = Malloc(tSize);
	if (hHandle != INVALID_HANDLE) {
		char *p = Pointer<char>(hHandle);
		memset(p, 0x00, tSize);
	}

	return hHandle;
}

/*当输入的chunk在使用中时候返回0*/
int PtMalloc::check_inuse_chunk(MallocChunk *pstChunk)
{
	if (!inuse_bit_at_offset(pstChunk, CHUNK_SIZE(pstChunk))) {
		snprintf(err_message_, sizeof(err_message_),
			 "chunk not inuse!");
		return (-1);
	}

	MallocChunk *pstTmp;
	if (!prev_inuse(pstChunk)) {
		pstTmp = (MallocChunk *)(((char *)pstChunk) -
					 pstChunk->m_tPreSize);
		if (ptr_to_handle(pstTmp) < m_pstHead->m_hBottom ||
		    CHUNK_SIZE(pstTmp) != pstChunk->m_tPreSize) {
			snprintf(err_message_, sizeof(err_message_),
				 "invalid pre-chunk size!");
			return (-2);
		}
	}

	pstTmp = (MallocChunk *)(((char *)pstChunk) + CHUNK_SIZE(pstChunk));
	if (!AT_TOP(pstTmp, 0)) {
		if (CHUNK_SIZE(pstTmp) < MINSIZE) {
			snprintf(err_message_, sizeof(err_message_),
				 "invalid next chunk!");
			return (-3);
		}
	}

	return (0);
}
/*realloc的主体逻辑*/
ALLOC_HANDLE_T PtMalloc::inter_re_alloc(ALLOC_HANDLE_T hHandle,
					ALLOC_SIZE_T tSize,
					ALLOC_SIZE_T &tOldMemSize)
{
	INTER_HANDLE_T hNewHandle;
	INTER_SIZE_T tNewSize;
	MallocChunk *pstChunk;

	ALLOC_SIZE_T tUserReqSize = tSize;

	tOldMemSize = 0;
	if (hHandle == INVALID_HANDLE) {
		//		return inter_malloc(tSize - MALLOC_ALIGN_MASK);
		return inter_malloc(tSize);
	}

	if (tSize == 0) {
		inter_free(hHandle, tOldMemSize);
		return (INVALID_HANDLE);
	}

	checked_request2size(tSize, tSize);

	if (hHandle >= m_pstHead->m_hTop || hHandle <= m_pstHead->m_hBottom) {
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid handle");
		return (INVALID_HANDLE);
	}

	ALLOC_SIZE_T tOldSize;
	pstChunk = (MallocChunk *)mem2chunk(handle_to_ptr(hHandle));
	tOldSize = CHUNK_SIZE(pstChunk);
	hHandle = ptr_to_handle((void *)pstChunk);
	if (hHandle + tOldSize > m_pstHead->m_hTop) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid handle: %lu, size: %u", hHandle,
			 tOldSize);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid handle: %llu, size: %u", hHandle,
			 tOldSize);
#endif
		return (INVALID_HANDLE);
	}

	if (misaligned_chunk(hHandle)) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid handle: %lu, size: %u", hHandle,
			 tOldSize);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid handle: %llu, size: %u", hHandle,
			 tOldSize);
#endif
		return (INVALID_HANDLE);
	}

	if (tOldSize < MINSIZE) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid old-size: %lu, size: %u", hHandle,
			 tOldSize);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid old-size: %llu, size: %u", hHandle,
			 tOldSize);
#endif
		return (INVALID_HANDLE);
	}

	if (check_inuse_chunk(pstChunk) != 0) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid chunk: %lu, size: %u", hHandle,
			 tOldSize);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "realloc-invalid chunk: %llu, size: %u", hHandle,
			 tOldSize);
#endif
		return (INVALID_HANDLE);
	}
	tOldMemSize = tOldSize;

	int iPreInUse = prev_inuse(pstChunk);
	ALLOC_SIZE_T tPreSize = pstChunk->m_tPreSize;

	MallocChunk *pstTmp;
	MallocChunk *pstNextChunk;
	pstNextChunk =
		(MallocChunk *)(((char *)pstChunk) + CHUNK_SIZE(pstChunk));

	if (tOldSize >= tSize) {
		hNewHandle = hHandle;
		tNewSize = tOldSize;
	} else {
		/* Try to expand forward into top */
		if (AT_TOP(pstChunk, tOldSize) &&
		    m_pstHead->m_hTop + (tSize - tOldSize) + MINSIZE <
			    m_pstHead->m_tSize) {
			pstChunk->m_tSize = REAL_SIZE(tSize) |
					    (pstChunk->m_tSize & SIZE_BITS);
			pstNextChunk = (MallocChunk *)handle_to_ptr(
				m_pstHead->m_hTop + (tSize - tOldSize));
			pstNextChunk->m_tPreSize = REAL_SIZE(tSize);
			pstNextChunk->m_tSize = PREV_INUSE;

			m_pstHead->m_hTop += (tSize - tOldSize);
			statMemoryTop = m_pstHead->m_hTop;
			return ptr_to_handle(chunk2mem(pstChunk));
		} else if (!AT_TOP(pstChunk, tOldSize) &&
			   !inuse_bit_at_offset(pstNextChunk,
						CHUNK_SIZE(pstNextChunk)) &&
			   ((INTER_SIZE_T)tOldSize +
			    CHUNK_SIZE(pstNextChunk)) >= tSize) {
			hNewHandle = hHandle;
			tNewSize = (INTER_SIZE_T)tOldSize +
				   CHUNK_SIZE(pstNextChunk);
			unlink_bin(m_ptBin[bin_index(CHUNK_SIZE(pstNextChunk))],
				   ptr_to_handle(pstNextChunk));
		}
		/* ada: defrag */
		else if (!prev_inuse(pstChunk) &&
			 (tOldSize + pstChunk->m_tPreSize) >= tSize) {
			pstTmp = (MallocChunk *)(((char *)pstChunk) -
						 pstChunk->m_tPreSize);
			iPreInUse = prev_inuse(pstTmp);
			tPreSize = pstTmp->m_tPreSize;
			// copy & move
			hNewHandle = hHandle - pstChunk->m_tPreSize;
			tNewSize =
				(INTER_SIZE_T)tOldSize + pstChunk->m_tPreSize;
			unlink_bin(m_ptBin[bin_index(pstChunk->m_tPreSize)],
				   hNewHandle);
			// copy user data
			memmove(chunk2mem(handle_to_ptr(hNewHandle)),
				chunk2mem(handle_to_ptr(hHandle)),
				chunksize2memsize(tOldSize));
		} else {
			// alloc , copy & free
			hNewHandle = inter_malloc(tUserReqSize);
			if (hNewHandle == INVALID_HANDLE) {
				snprintf(err_message_, sizeof(err_message_),
					 "realloc-out of memory");
				return (INVALID_HANDLE);
			}
			pstTmp = (MallocChunk *)mem2chunk(
				handle_to_ptr(hNewHandle));
			hNewHandle = ptr_to_handle(pstTmp);
			tNewSize = CHUNK_SIZE(pstTmp);
			// copy user data
			memcpy(chunk2mem(pstTmp),
			       chunk2mem(handle_to_ptr(hHandle)),
			       chunksize2memsize(tOldSize));
			ALLOC_SIZE_T tTmpSize;
			inter_free(chunkhandle2memhandle(hHandle), tTmpSize);
			return chunkhandle2memhandle(hNewHandle);
		}
	}

	assert(tNewSize >= tSize);
	MallocChunk *pstNewChunk;
	pstNewChunk = (MallocChunk *)handle_to_ptr(hNewHandle);
	INTER_SIZE_T tRemainderSize = tNewSize - tSize;
	if (tRemainderSize >= get_min_chunk_size()) {
		// split
		MallocChunk *pstRemainChunk;
		pstRemainChunk = (MallocChunk *)(((char *)pstNewChunk) + tSize);
		//	ALLOC_SIZE_T tPreChunkSize = tSize;
		do {
			ALLOC_SIZE_T tThisChunkSize;
			if (tRemainderSize > MAX_ALLOC_SIZE) {
				if (tRemainderSize - MAX_ALLOC_SIZE >= MINSIZE)
					tThisChunkSize =
						REAL_SIZE(MAX_ALLOC_SIZE);
				else
					tThisChunkSize = REAL_SIZE(
						tRemainderSize - MINSIZE);
			} else {
				tThisChunkSize = tRemainderSize;
			}
			pstRemainChunk->m_tSize =
				REAL_SIZE(tThisChunkSize) | PREV_INUSE;

			// next chunk
			pstNextChunk =
				(MallocChunk *)(((char *)pstRemainChunk) +
						REAL_SIZE(tThisChunkSize));
			pstNextChunk->m_tPreSize = REAL_SIZE(tThisChunkSize);
			pstNextChunk->m_tSize |= PREV_INUSE;
			/* Mark remainder as inuse so free() won't complain */
			set_inuse_bit_at_offset(pstRemainChunk, tThisChunkSize);
			ALLOC_SIZE_T tTmpSize;
			inter_free(ptr_to_handle(chunk2mem(pstRemainChunk)),
				   tTmpSize);

			//		tPreChunkSize = tThisChunkSize;
			tRemainderSize -= tThisChunkSize;
			pstRemainChunk =
				(MallocChunk *)(((char *)pstRemainChunk) +
						REAL_SIZE(tThisChunkSize));
		} while (tRemainderSize > 0);

		tNewSize = tSize;
	} else {
		// next chunk
		pstNextChunk = (MallocChunk *)(((char *)pstNewChunk) +
					       REAL_SIZE(tNewSize));
		pstNextChunk->m_tSize |= PREV_INUSE;
	}
	pstNewChunk->m_tSize = REAL_SIZE(tNewSize);
	if (iPreInUse)
		pstNewChunk->m_tSize |= PREV_INUSE;
	pstNewChunk->m_tPreSize = tPreSize;

	return ptr_to_handle(chunk2mem(pstNewChunk));
}
/*对intserrealloc的包装，对返回结果进行了简单的检查*/
ALLOC_HANDLE_T PtMalloc::ReAlloc(ALLOC_HANDLE_T hHandle, ALLOC_SIZE_T tSize)
{
	ALLOC_HANDLE_T hNewHandle;
	ALLOC_SIZE_T tOldSize;
	MallocChunk *pstChunk;

	m_pstHead->m_tLastFreeChunkSize = 0;
	hNewHandle = inter_re_alloc(hHandle, tSize, tOldSize);
	if (hNewHandle != INVALID_HANDLE) {
		pstChunk = (MallocChunk *)mem2chunk(handle_to_ptr(hNewHandle));
		m_pstHead->m_tUserAllocSize += CHUNK_SIZE(pstChunk);
		m_pstHead->m_tUserAllocSize -= tOldSize;
		if (hHandle == INVALID_HANDLE) {
			m_pstHead->m_tUserAllocChunkCnt++;
			++statChunkTotal;
		}
		add_alloc_size_to_stat(tSize);
		statDataSize = m_pstHead->m_tUserAllocSize;
	} else if (tSize == 0) {
		m_pstHead->m_tUserAllocSize -= tOldSize;
		m_pstHead->m_tUserAllocChunkCnt--;
		--statChunkTotal;
		statDataSize = m_pstHead->m_tUserAllocSize;
	}

	return (hNewHandle);
}
/*free接口的主体逻辑*/
int PtMalloc::inter_free(ALLOC_HANDLE_T hHandle, ALLOC_SIZE_T &tMemSize)
{
	tMemSize = 0;
	if (hHandle == INVALID_HANDLE)
		return (0);

	if (hHandle >= m_pstHead->m_tSize) {
		snprintf(err_message_, sizeof(err_message_),
			 "free-invalid handle");
		return (-1);
	}

	//	log4cplus_error("FREE: %lu", hHandle);

	MallocChunk *pstChunk;
	ALLOC_SIZE_T tSize;
	pstChunk = (MallocChunk *)mem2chunk(handle_to_ptr(hHandle));
	tSize = CHUNK_SIZE(pstChunk);
	tMemSize = tSize;
	hHandle = ptr_to_handle((void *)pstChunk);
	if (hHandle + tSize >= m_pstHead->m_tSize) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "free-invalid handle: %lu, size: %u", hHandle, tSize);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "free-invalid handle: %llu, size: %u", hHandle, tSize);
#endif
		return (-2);
	}

	if (!inuse_bit_at_offset(pstChunk, tSize)) {
#if __WORDSIZE == 64
		snprintf(
			err_message_, sizeof(err_message_),
			"free-memory[handle %lu, size: %u, top: %lu] not in use",
			hHandle, tSize, m_pstHead->m_hTop);
#else
		snprintf(
			err_message_, sizeof(err_message_),
			"free-memory[handle %llu, size: %u, top: %llu] not in use",
			hHandle, tSize, m_pstHead->m_hTop);
#endif
		return (-3);
	}

	if (misaligned_chunk(hHandle)) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "free-invalid handle: %lu, size: %u", hHandle, tSize);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "free-invalid handle: %llu, size: %u", hHandle, tSize);
#endif
		return (INVALID_HANDLE);
	}

	if (check_inuse_chunk(pstChunk) != 0) {
#if __WORDSIZE == 64
		snprintf(err_message_, sizeof(err_message_),
			 "free-invalid chunk: %lu, size: %u", hHandle, tSize);
#else
		snprintf(err_message_, sizeof(err_message_),
			 "free-invalid chunk: %llu, size: %u", hHandle, tSize);
#endif
		return (INVALID_HANDLE);
	}

	unsigned int uiBinIdx;
	MallocChunk *pstNextChunk;

	if (!prev_inuse(pstChunk) && CAN_COMBILE(tSize, pstChunk->m_tPreSize)) {
		tSize += pstChunk->m_tPreSize;
		hHandle -= pstChunk->m_tPreSize;
		uiBinIdx = bin_index(pstChunk->m_tPreSize);
		pstChunk = (MallocChunk *)(((char *)pstChunk) -
					   pstChunk->m_tPreSize);
		// unlink
		unlink_bin(m_ptBin[uiBinIdx], ptr_to_handle(pstChunk));
		if (empty_bin(uiBinIdx))
			clear_bin_bit_map(uiBinIdx);
		set_size_at_offset(pstChunk, 0, tSize);
		set_presize_at_offset(pstChunk, tSize, tSize);
	}

	if ((hHandle + tSize) != m_pstHead->m_hTop) {
		pstNextChunk = (MallocChunk *)handle_to_ptr(hHandle + tSize);
		if (CHUNK_SIZE(pstNextChunk) < MINSIZE) {
			snprintf(err_message_, sizeof(err_message_),
				 "free-invalid handle: " UINT64FMT_T
				 ", size: %u",
				 hHandle, tSize);
			return (-4);
		}
		if (!inuse_bit_at_offset(pstNextChunk,
					 REAL_SIZE(pstNextChunk->m_tSize)) &&
		    CAN_COMBILE(tSize, CHUNK_SIZE(pstNextChunk))) {
			tSize += CHUNK_SIZE(pstNextChunk);
			uiBinIdx = bin_index(CHUNK_SIZE(pstNextChunk));
			// unlink
			unlink_bin(m_ptBin[uiBinIdx],
				   ptr_to_handle(pstNextChunk));
			if (empty_bin(uiBinIdx))
				clear_bin_bit_map(uiBinIdx);
			set_size_at_offset(pstChunk, 0, tSize);
			set_presize_at_offset(pstChunk, tSize, tSize);
		}
	}

	set_size_at_offset(pstChunk, 0, tSize);
	set_presize_at_offset(pstChunk, tSize, tSize);
	set_inuse_bit_at_offset(pstChunk, tSize);

	if (m_pstHead->m_tLastFreeChunkSize < tSize)
		m_pstHead->m_tLastFreeChunkSize = tSize;

	if ((hHandle + tSize) == m_pstHead->m_hTop) {
		m_pstHead->m_hTop -= tSize;
		statMemoryTop = m_pstHead->m_hTop;
		pstChunk->m_tSize = PREV_INUSE;
		if (m_pstHead->m_tSize > (m_pstHead->m_hTop + MINSIZE) &&
		    m_pstHead->m_tLastFreeChunkSize <
			    m_pstHead->m_tSize - m_pstHead->m_hTop - MINSIZE)
			m_pstHead->m_tLastFreeChunkSize = m_pstHead->m_tSize -
							  m_pstHead->m_hTop -
							  MINSIZE;
		return (0);
	}

	clear_inuse_bit_at_offset(pstChunk, tSize);

	// place chunk into bin
	if (in_smallbin_range(tSize)) {
		link_bin(m_ptBin[smallbin_index(tSize)],
			 ptr_to_handle(pstChunk));
		set_bin_bit_map(smallbin_index(tSize));
	} else {
		link_bin(m_ptBin[largebin_index(tSize)],
			 ptr_to_handle(pstChunk));
		set_bin_bit_map(largebin_index(tSize));
	}
	//#endif

	return (0);
}
/*对interfree的包装，对返回结果进行了简单检查*/
int PtMalloc::Free(ALLOC_HANDLE_T hHandle)
{
	int iRet;
	ALLOC_SIZE_T tSize;

	tSize = 0;
	iRet = inter_free(hHandle, tSize);
	if (iRet == 0) {
		m_pstHead->m_tUserAllocSize -= tSize;
		m_pstHead->m_tUserAllocChunkCnt--;
		--statChunkTotal;
		statDataSize = m_pstHead->m_tUserAllocSize;
	}

	return (iRet);
}
/*返回如果free掉handle指定chunk能够给cache共享多少空闲内存*/
/*前后合并chunk可能导致释放比指定handle的大小更大的空间*/
unsigned PtMalloc::ask_for_destroy_size(ALLOC_HANDLE_T hHandle)
{
	//	ALLOC_SIZE_T logic_size = 0;
	ALLOC_SIZE_T physic_size = 0;
	ALLOC_HANDLE_T physic_handle = 0;

	MallocChunk *current_chunk = 0;
	MallocChunk *next_chunk = 0;

	if (INVALID_HANDLE == hHandle || hHandle >= m_pstHead->m_tSize)
		goto ERROR;

	/* physic pointer */
	current_chunk = (MallocChunk *)mem2chunk(handle_to_ptr(hHandle));
	physic_size = CHUNK_SIZE(current_chunk);
	//	logic_size = chunksize2memsize(physic_size);
	physic_handle = ptr_to_handle((void *)current_chunk);

	/* start error check. */
	/* overflow */
	if (physic_handle + physic_size > m_pstHead->m_tSize)
		goto ERROR;

	/* current chunk is not inuse */
	if (!inuse_bit_at_offset(current_chunk, physic_size))
		goto ERROR;

	/* not aligned */
	if (misaligned_chunk(physic_handle))
		goto ERROR;

	/* */
	if (0 != check_inuse_chunk(current_chunk))
		goto ERROR;

	/* try combile prev-chunk */
	if (!prev_inuse(current_chunk) &&
	    CAN_COMBILE(physic_size, current_chunk->m_tPreSize)) {
		physic_size += current_chunk->m_tPreSize;

		/* forward handle */
		physic_handle -= current_chunk->m_tPreSize;
		current_chunk = (MallocChunk *)((char *)current_chunk -
						current_chunk->m_tPreSize);
	}

	/* try combile next-chunk */
	if (physic_handle + physic_size != m_pstHead->m_hTop) {
		next_chunk = (MallocChunk *)(handle_to_ptr(physic_handle +
							   physic_size));
		if (CHUNK_SIZE(next_chunk) < MINSIZE)
			goto ERROR;

		/* can combile */
		if (!inuse_bit_at_offset(next_chunk, CHUNK_SIZE(next_chunk)) &&
		    CAN_COMBILE(physic_size, CHUNK_SIZE(next_chunk))) {
			physic_size += CHUNK_SIZE(next_chunk);
		}
	}

	/* 释放到top边界，合并成一大块内存 */
	if (physic_handle + physic_size == m_pstHead->m_hTop) {
		ALLOC_SIZE_T physic_free = m_pstHead->m_tSize -
					   m_pstHead->m_hTop - MINSIZE +
					   physic_size;
		physic_size =
			physic_size < physic_free ? physic_free : physic_size;
	}

	return chunksize2memsize(physic_size);

ERROR:
	snprintf(err_message_, sizeof(err_message_),
		 "found invalid handle, can't destroy");
	return 0;
}

ALLOC_SIZE_T PtMalloc::last_free_size()
{
	free_fast();

	return chunksize2memsize(m_pstHead->m_tLastFreeChunkSize);
}

/**************************************************************************
 * for test
 * dump all bins and chunks
 *************************************************************************/

/*对所有的bin检查：small&large bins, fast bins, unsorted bins*/
/*校验方法：每个bin组成一个双向的循环链表*/
int PtMalloc::dump_bins()
{
	int i;
	int count;
	uint64_t size;

	INTER_HANDLE_T hHandle;
	MallocChunk *pstChunk;
	printf("dump bins\n");
	for (i = 0; i < NBINS; i++) {
		hHandle = m_ptBin[i].m_hNextChunk;
		count = 0;
		size = 0;
		if (hHandle != INVALID_HANDLE) {
			do {
				pstChunk =
					(MallocChunk *)handle_to_ptr(hHandle);
				if (pstChunk->m_hNextChunk != INVALID_HANDLE)
					hHandle = pstChunk->m_hNextChunk;
				size += CHUNK_SIZE(pstChunk);
				++count;
			} while (pstChunk->m_hNextChunk != INVALID_HANDLE);
		}
		if (m_ptBin[i].m_hPreChunk != hHandle) {
			printf("bad bin[%d]", i);
			return (-1);
		}
		if (count) {
#if __WORDSIZE == 64
			printf("bins[%d] chunk num[%d] size[%lu]\n", i, count,
			       size);
#else
			printf("bins[%d] chunk num[%d] size[%llu]\n", i, count,
			       size);
#endif
		}
	}

	printf("dump fast bins\n");
	for (i = 0; i < NFASTBINS; i++) {
		hHandle = m_ptFastBin[i].m_hNextChunk;
		count = 0;
		if (hHandle != INVALID_HANDLE) {
			do {
				pstChunk =
					(MallocChunk *)handle_to_ptr(hHandle);
				if (pstChunk->m_hNextChunk != INVALID_HANDLE)
					hHandle = pstChunk->m_hNextChunk;
				++count;
			} while (pstChunk->m_hNextChunk != INVALID_HANDLE);
		}
		if (m_ptFastBin[i].m_hPreChunk != hHandle) {
			printf("bad fast-bin[%d]\n", i);
			return (-2);
		}
		if (count) {
			printf("fast bins[%d] chunk num[%d]\n", i, count);
		}
	}
	printf("dump unsorted bins\n");
	hHandle = m_ptUnsortedBin[0].m_hNextChunk;
	count = 0;
	if (hHandle != INVALID_HANDLE) {
		do {
			pstChunk = (MallocChunk *)handle_to_ptr(hHandle);
			printf("%d\n", CHUNK_SIZE(pstChunk));
			if (pstChunk->m_hNextChunk != INVALID_HANDLE)
				hHandle = pstChunk->m_hNextChunk;
		} while (pstChunk->m_hNextChunk != INVALID_HANDLE);
	}
	if (m_ptUnsortedBin[0].m_hPreChunk != hHandle) {
#if __WORDSIZE == 64
		printf("bad unsorted-bin[%d] %lu!=%lu\n", 0,
		       m_ptUnsortedBin[0].m_hPreChunk, hHandle);
#else
		printf("bad unsorted-bin[%d] %llu!=%llu\n", 0,
		       m_ptUnsortedBin[0].m_hPreChunk, hHandle);
#endif
		return (-3);
	}
	printf("unsorted bins:chunk num[%d]\n", count);

	return (0);
}

int PtMalloc::dump_mem()
{
	INTER_HANDLE_T hHandle;
	MallocChunk *pstChunk;
	//	ALLOC_SIZE_T tSize;

	//	tSize = 0;
	printf("dump_mem\n");
	hHandle = m_pstHead->m_hBottom;
	while (hHandle < m_pstHead->m_hTop) {
		pstChunk = (MallocChunk *)handle_to_ptr(hHandle);
		printf("%d\t\t%d\n", CHUNK_SIZE(pstChunk),
		       prev_inuse(pstChunk));
		hHandle += CHUNK_SIZE(pstChunk);
	}

	return (0);
}
