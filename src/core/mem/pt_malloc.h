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

#ifndef BIN_MALLOC_H
#define BIN_MALLOC_H

#include <stdint.h>
#include <stdlib.h>
#include "namespace.h"
#include "mallocator.h"
#include "log/log.h"
#include "stat_dtc.h"

DTC_BEGIN_NAMESPACE

#define MALLOC_FLAG_FAST 0x1

/*
  This struct declaration is misleading (but accurate and necessary).
  It declares a "view" into memory allowing access to necessary
  fields at known offsets from a given base. See explanation below.
*/

typedef struct {
	ALLOC_SIZE_T m_tPreSize; /* Size of previous chunk (if free).  */
	ALLOC_SIZE_T m_tSize; /* Size in bytes, including overhead. */

	INTER_HANDLE_T m_hPreChunk; /* double links -- used only if free. */
	INTER_HANDLE_T m_hNextChunk;
} MallocChunk;

typedef struct {
	INTER_HANDLE_T m_hPreChunk;
	INTER_HANDLE_T m_hNextChunk;
} CBin;

/* The smallest possible chunk */
#define MIN_CHUNK_SIZE (sizeof(MallocChunk))

/* The smallest size we can malloc is an aligned minimal chunk */
#define MINSIZE                                                                \
	(unsigned long)(((MIN_CHUNK_SIZE + MALLOC_ALIGN_MASK) &                \
			 ~MALLOC_ALIGN_MASK))

#define NBINS 128
#define NSMALLBINS 64
#define SMALLBIN_WIDTH 8
#define MIN_LARGE_SIZE 512

#define DTC_SIGN_0 0
#define DTC_SIGN_1 0x4D635474U
#define DTC_SIGN_2 1
#define DTC_SIGN_3 0xFFFFFFFFU
#define DTC_SIGN_4 0xFFFFFFFFU
#define DTC_SIGN_5 0xFFFFFFFFU
#define DTC_SIGN_6 4
#define DTC_SIGN_7 0
#define DTC_SIGN_8 16
#define DTC_SIGN_9 0xFFFFFFFFU
#define DTC_SIGN_A 0
#define DTC_SIGN_B 0
#define DTC_SIGN_C 0xFFFFFFFFU
#define DTC_SIGN_D 0xFFFFFFFFU

#define DTC_VER_MIN 4 // Minimum DTC memory version recognized by this code

#define DTC_RESERVE_SIZE (4 * 1024UL)

#define EC_NO_MEM 2041 // Insufficient memory error code
#define EC_KEY_EXIST 2042
#define EC_KEY_NOT_EXIST 2043
#define MAXSTATCOUNT 10000 * 3600 * 12

struct _MemHead {
	uint32_t m_auiSign[14]; // Memory format signature
	unsigned short m_ushVer; // Memory format version number
	unsigned short m_ushHeadSize; // Header size
	INTER_SIZE_T m_tSize; // Total memory size
	INTER_SIZE_T m_tUserAllocSize; // Memory size available to upper layer applications
	INTER_SIZE_T m_tUserAllocChunkCnt; // Number of memory chunks allocated by upper layer applications
	uint32_t m_uiFlags; // Feature flags
	INTER_HANDLE_T m_hBottom; // Bottom address of memory available to upper layer applications
	INTER_HANDLE_T m_hReserveZone; // Reserved address for upper layer applications
	INTER_HANDLE_T m_hTop; // Current highest allocated address
	INTER_SIZE_T m_tLastFreeChunkSize; // Merged chunk size after the last free operation
	uint16_t m_ushBinCnt; // Number of bins
	uint16_t m_ushFastBinCnt; // Number of fast bins
	uint32_t m_auiBinBitMap[(NBINS - 1) / 32 + 1]; // Bin bitmap
	uint32_t m_shmIntegrity; // Shared memory integrity flag
	char m_achReserv
		[872]; // Reserved fields (making CMemHead size 1008 bytes, reaching 4K with bins that follow)
} __attribute__((__aligned__(4)));
typedef struct _MemHead MemHead;

#define GET_OBJ(mallocter, handle, obj_ptr)                                    \
	do {                                                                   \
		obj_ptr = (typeof(obj_ptr))mallocter.handle_to_ptr(handle);    \
	} while (0)

class PtMalloc : public MallocBase {
    private:
	void *m_pBaseAddr;
	MemHead *m_pstHead;
	CBin *m_ptBin;
	CBin *m_ptFastBin;
	CBin *m_ptUnsortedBin;
	char err_message_[200];

	// stat
	StatCounter statChunkTotal;
	StatItem statDataSize;
	StatItem statMemoryTop;

	uint64_t statTmpDataSizeRecently; // Recently allocated memory size
	uint64_t statTmpDataAllocCountRecently; // Recent memory allocation count
	StatItem statAverageDataSizeRecently;
	inline void add_alloc_size_to_stat(uint64_t size)
	{
		if (statTmpDataAllocCountRecently > MAXSTATCOUNT) {
			statTmpDataSizeRecently = 0;
			statTmpDataAllocCountRecently = 0;
			statAverageDataSizeRecently = MINSIZE;
		} else {
			statTmpDataSizeRecently += size;
			statTmpDataAllocCountRecently++;
			statAverageDataSizeRecently =
				statTmpDataSizeRecently /
				statTmpDataAllocCountRecently;
		}
	}

	// Minimum chunk size
	unsigned int minChunkSize;
	inline unsigned int get_min_chunk_size(void)
	{
		return minChunkSize == 1 ?
			       ((statChunkTotal <= 0) ?
					MINSIZE :
					statDataSize / statChunkTotal) :
			       minChunkSize;
	}

    public:
	void set_min_chunk_size(unsigned int size)
	{
		minChunkSize =
			size == 1 ? 1 : (size < MINSIZE ? MINSIZE : size);
	}

    protected:
	void init_sign();

	void *bin_malloc(CBin &ptBin);
	void *small_bin_malloc(ALLOC_SIZE_T tSize);
	void *fast_malloc(ALLOC_SIZE_T tSize);
	void *top_alloc(ALLOC_SIZE_T tSize);
	int unlink_bin(CBin &stBin, INTER_HANDLE_T hHandle);
	int link_bin(CBin &stBin, INTER_HANDLE_T hHandle);
	int link_sorted_bin(CBin &stBin, INTER_HANDLE_T hHandle,
			    ALLOC_SIZE_T tSize);
	int check_inuse_chunk(MallocChunk *pstChunk);
	int free_fast();

	inline void set_bin_bit_map(unsigned int uiBinIdx)
	{
		m_pstHead->m_auiBinBitMap[uiBinIdx / 32] |=
			(1UL << (uiBinIdx % 32));
	}
	inline void clear_bin_bit_map(unsigned int uiBinIdx)
	{
		m_pstHead->m_auiBinBitMap[uiBinIdx / 32] &=
			(~(1UL << (uiBinIdx % 32)));
	}
	inline int empty_bin(unsigned int uiBinIdx)
	{
		return (m_ptBin[uiBinIdx].m_hNextChunk == INVALID_HANDLE);
	}

	// Internal statistics
	ALLOC_HANDLE_T inter_malloc(ALLOC_SIZE_T tSize);
	ALLOC_HANDLE_T inter_re_alloc(ALLOC_HANDLE_T hHandle,
				      ALLOC_SIZE_T tSize,
				      ALLOC_SIZE_T &tOldMemSize);
	int inter_free(ALLOC_HANDLE_T hHandle, ALLOC_SIZE_T &tMemSize);

    public:
	PtMalloc();
	~PtMalloc();

	static PtMalloc *instance();
	static void destroy();

	template <class T> T *Pointer(ALLOC_HANDLE_T hHandle)
	{
		return reinterpret_cast<T *>(handle_to_ptr(hHandle));
	}

	ALLOC_HANDLE_T get_handle(void *p)
	{
		return ptr_to_handle(p);
	}

	const char *get_err_msg()
	{
		return err_message_;
	}
	const MemHead *get_head_info() const
	{
		return m_pstHead;
	}

	/*************************************************
	  Description:	Format memory
	  Input:		pAddr	Memory block address
				tSize		Memory block size
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int do_init(void *pAddr, INTER_SIZE_T tSize);

	/*************************************************
	  Description:	Attach to already formatted memory block
	  Input:		pAddr	Memory block address
				tSize		Memory block size
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int do_attach(void *pAddr, INTER_SIZE_T tSize);

	/*************************************************
	  Description:	Detect DTC version of memory block
	  Input:		pAddr	Memory block address
				tSize		Memory block size
	   Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int detect_version();

	/* Shared memory integrity check interface */
	int share_memory_integrity();
	void set_share_memory_integrity(const int flag);

	/*************************************************
	  Description:	Check if internal data structure bin is correct
	  Input:		
	  Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int check_bin();
#if BIN_MEM_CHECK
	int check_mem();
#endif
	int dump_bins();
	int dump_mem();

	/*************************************************
	  Description:	Allocate memory
	  Input:		tSize		Size of memory to allocate
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure
	**************************************************/
	ALLOC_HANDLE_T Malloc(ALLOC_SIZE_T tSize);

	/*************************************************
	  Description:	Allocate memory and initialize to 0
	  Input:		tSize		Size of memory to allocate
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure
	**************************************************/
	ALLOC_HANDLE_T Calloc(ALLOC_SIZE_T tSize);

	/*************************************************
	  Description:	Reallocate memory
	  Input:		hHandle	Old memory handle
				tSize		New memory size
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure (old memory block not freed on failure)
	**************************************************/
	ALLOC_HANDLE_T ReAlloc(ALLOC_HANDLE_T hHandle, ALLOC_SIZE_T tSize);

	/*************************************************
	  Description:	Free memory
	  Input:		hHandle	Memory handle
	  Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int Free(ALLOC_HANDLE_T hHandle);

	/*************************************************
	  Description: Get how much free space can be obtained after freeing this memory	
	  Input:		hHandle	Memory handle
	  Output:		
	  Return:		>0 on success, 0 on failure
	**************************************************/
	unsigned ask_for_destroy_size(ALLOC_HANDLE_T hHandle);

	/*************************************************
	  Description:	Get memory block size
	  Input:		hHandle	Memory handle
	  Output:		
	  Return:		Memory size
	**************************************************/
	ALLOC_SIZE_T chunk_size(ALLOC_HANDLE_T hHandle);

	/*************************************************
	  Description:	Get total size of memory already allocated by user
	  Input:		
	  Output:		
	  Return:		Memory size
	**************************************************/
	INTER_SIZE_T user_alloc_size()
	{
		return m_pstHead->m_tUserAllocSize;
	}

	/*************************************************
	  Description:	Get total memory size
	  Input:		
	  Output:		
	  Return:		Memory size
	**************************************************/
	INTER_SIZE_T total_size()
	{
		return m_pstHead->m_tSize;
	}

	/*************************************************
	  Description:	Size of merged chunk after the last memory free operation
	  Input:		
	  Output:		
	  Return:		Memory size
	**************************************************/
	ALLOC_SIZE_T last_free_size();

	/*************************************************
	  Description:	Get reserved memory block for upper layer applications (size = DTC_RESERVE_SIZE = 4K)
	  Input:		
	  Output:		
	  Return:		Memory handle
	**************************************************/
	ALLOC_HANDLE_T get_reserve_zone();

	/*************************************************
	  Description:	Convert handle to memory address
	  Input:		Memory handle
	  Output:		
	  Return:		Memory address, NULL if handle is invalid
	**************************************************/
	inline void *handle_to_ptr(ALLOC_HANDLE_T hHandle)
	{
		if (hHandle == INVALID_HANDLE)
			return (NULL);
		return (void *)(((char *)m_pBaseAddr) + hHandle);
	}

	/*************************************************
	  Description:	Convert memory address to handle
	  Input:		Memory address
	  Output:		
	  Return:		Memory handle, INVALID_HANDLE if address is invalid
	**************************************************/
	inline ALLOC_HANDLE_T ptr_to_handle(void *p)
	{
		if ((char *)p < (char *)m_pBaseAddr ||
		    (char *)p >= ((char *)m_pBaseAddr) + m_pstHead->m_tSize)
			return INVALID_HANDLE;
		return (ALLOC_HANDLE_T)(((char *)p) - ((char *)m_pBaseAddr));
	}

	/*************************************************
	  Description:	Check if handle is valid
	  Input:		Memory handle
	  Output:		
      Return:	    0: valid; -1: invalid
	**************************************************/
	virtual int handle_is_valid(ALLOC_HANDLE_T mem_handle)
	{
		return 0;
	}
};

DTC_END_NAMESPACE

#endif
