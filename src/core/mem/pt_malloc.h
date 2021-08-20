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

#define DTC_VER_MIN 4 // 本代码认识的dtc内存最小版本

#define DTC_RESERVE_SIZE (4 * 1024UL)

#define EC_NO_MEM 2041 // 内存不足错误码
#define EC_KEY_EXIST 2042
#define EC_KEY_NOT_EXIST 2043
#define MAXSTATCOUNT 10000 * 3600 * 12

struct _MemHead {
	uint32_t m_auiSign[14]; // 内存格式标记
	unsigned short m_ushVer; // 内存格式版本号
	unsigned short m_ushHeadSize; // 头大小
	INTER_SIZE_T m_tSize; // 内存总大小
	INTER_SIZE_T m_tUserAllocSize; // 上层应用分配到可用的内存大小
	INTER_SIZE_T m_tUserAllocChunkCnt; // 上层应用分配的内存块数量
	uint32_t m_uiFlags; // 特性标记
	INTER_HANDLE_T m_hBottom; // 上层应用可用内存底地址
	INTER_HANDLE_T m_hReserveZone; // 为上层应用保留的地址
	INTER_HANDLE_T m_hTop; // 目前分配到的最高地址
	INTER_SIZE_T m_tLastFreeChunkSize; // 最近一次free后，合并得到的chunk大小
	uint16_t m_ushBinCnt; // bin的数量
	uint16_t m_ushFastBinCnt; // fastbin数量
	uint32_t m_auiBinBitMap[(NBINS - 1) / 32 + 1]; // bin的bitmap
	uint32_t m_shmIntegrity; //共享内存完整性标记
	char m_achReserv
		[872]; // 保留字段 （使CMemHead的大小为1008Bytes，加上后面的bins后达到4K）
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

	uint64_t statTmpDataSizeRecently; //最近分配的内存大小
	uint64_t statTmpDataAllocCountRecently; //最近分配的内存次数
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

	//最小的chrunk size,
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

	// 内部做一下统计
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
	  Description:	格式化内存
	  Input:		pAddr	内存块地址
				tSize		内存块大小
	  Return:		0为成功，非0失败
	*************************************************/
	int do_init(void *pAddr, INTER_SIZE_T tSize);

	/*************************************************
	  Description:	attach已经格式化好的内存块
	  Input:		pAddr	内存块地址
				tSize		内存块大小
	  Return:		0为成功，非0失败
	*************************************************/
	int do_attach(void *pAddr, INTER_SIZE_T tSize);

	/*************************************************
	  Description:	检测内存块的dtc版本
	  Input:		pAddr	内存块地址
				tSize		内存块大小
	   Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int detect_version();

	/* 共享内存完整性检测接口 */
	int share_memory_integrity();
	void set_share_memory_integrity(const int flag);

	/*************************************************
	  Description:	检测内部数据结构bin是否正确
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int check_bin();
#if BIN_MEM_CHECK
	int check_mem();
#endif
	int dump_bins();
	int dump_mem();

	/*************************************************
	  Description:	分配内存
	  Input:		tSize		分配的内存大小
	  Output:		
	  Return:		内存块句柄，INVALID_HANDLE为失败
	*************************************************/
	ALLOC_HANDLE_T Malloc(ALLOC_SIZE_T tSize);

	/*************************************************
	  Description:	分配内存，并将内存初始化为0
	  Input:		tSize		分配的内存大小
	  Output:		
	  Return:		内存块句柄，INVALID_HANDLE为失败
	*************************************************/
	ALLOC_HANDLE_T Calloc(ALLOC_SIZE_T tSize);

	/*************************************************
	  Description:	重新分配内存
	  Input:		hHandle	老内存句柄
				tSize		新分配的内存大小
	  Output:		
	  Return:		内存块句柄，INVALID_HANDLE为失败(失败时不会释放老内存块)
	*************************************************/
	ALLOC_HANDLE_T ReAlloc(ALLOC_HANDLE_T hHandle, ALLOC_SIZE_T tSize);

	/*************************************************
	  Description:	释放内存
	  Input:		hHandle	内存句柄
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int Free(ALLOC_HANDLE_T hHandle);

	/*************************************************
	  Description: 获取释放这块内存后可以得到多少free空间	
	  Input:		hHandle	内存句柄
	  Output:		
	  Return:		>0为成功，0失败
	*************************************************/
	unsigned ask_for_destroy_size(ALLOC_HANDLE_T hHandle);

	/*************************************************
	  Description:	获取内存块大小
	  Input:		hHandle	内存句柄
	  Output:		
	  Return:		内存大小
	*************************************************/
	ALLOC_SIZE_T chunk_size(ALLOC_HANDLE_T hHandle);

	/*************************************************
	  Description:	获取用户已经分配的内存总大小
	  Input:		
	  Output:		
	  Return:		内存大小
	*************************************************/
	INTER_SIZE_T user_alloc_size()
	{
		return m_pstHead->m_tUserAllocSize;
	}

	/*************************************************
	  Description:	获取内存总大小
	  Input:		
	  Output:		
	  Return:		内存大小
	*************************************************/
	INTER_SIZE_T total_size()
	{
		return m_pstHead->m_tSize;
	}

	/*************************************************
	  Description:	最近一次释放内存，合并后的chunk大小
	  Input:		
	  Output:		
	  Return:		内存大小
	*************************************************/
	ALLOC_SIZE_T last_free_size();

	/*************************************************
	  Description:	获取为上层应用保留的内存块（大小为DTC_RESERVE_SIZE＝4K）
	  Input:		
	  Output:		
	  Return:		内存句柄
	*************************************************/
	ALLOC_HANDLE_T get_reserve_zone();

	/*************************************************
	  Description:	将句柄转换成内存地址
	  Input:		内存句柄
	  Output:		
	  Return:		内存地址，如果句柄无效返回NULL
	*************************************************/
	inline void *handle_to_ptr(ALLOC_HANDLE_T hHandle)
	{
		if (hHandle == INVALID_HANDLE)
			return (NULL);
		return (void *)(((char *)m_pBaseAddr) + hHandle);
	}

	/*************************************************
	  Description:	将内存地址转换为句柄
	  Input:		内存地址
	  Output:		
	  Return:		内存句柄，如果地址无效返回INVALID_HANDLE
	*************************************************/
	inline ALLOC_HANDLE_T ptr_to_handle(void *p)
	{
		if ((char *)p < (char *)m_pBaseAddr ||
		    (char *)p >= ((char *)m_pBaseAddr) + m_pstHead->m_tSize)
			return INVALID_HANDLE;
		return (ALLOC_HANDLE_T)(((char *)p) - ((char *)m_pBaseAddr));
	}

	/*************************************************
	  Description:	检测handle是否有效
	  Input:		内存句柄
	  Output:		
      Return:	    0: 有效; -1:无效
	*************************************************/
	virtual int handle_is_valid(ALLOC_HANDLE_T mem_handle)
	{
		return 0;
	}
};

DTC_END_NAMESPACE

#endif
