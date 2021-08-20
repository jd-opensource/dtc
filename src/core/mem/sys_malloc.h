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

#ifndef SYS_MALLOC_H
#define SYS_MALLOC_H

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "namespace.h"
#include "mallocator.h"

DTC_BEGIN_NAMESPACE

class SysMalloc : public MallocBase {
    private:
	char err_message_[200];

    public:
	SysMalloc()
	{
	}
	virtual ~SysMalloc()
	{
	}

	template <class T> T *Pointer(ALLOC_HANDLE_T hHandle)
	{
		return reinterpret_cast<T *>(handle_to_ptr(hHandle));
	}

	ALLOC_HANDLE_T get_handle(void *p)
	{
		return (ALLOC_HANDLE_T)((char *)p - (char *)0);
	}

	const char *get_err_msg()
	{
		return err_message_;
	}

	/*************************************************
	  Description:	分配内存
	  Input:		tSize		分配的内存大小
	  Output:		
	  Return:		内存块句柄，INVALID_HANDLE为失败
	*************************************************/
	ALLOC_HANDLE_T Malloc(ALLOC_SIZE_T tSize)
	{
		void *p = malloc(sizeof(ALLOC_SIZE_T) + tSize);
		if (p == NULL) {
			snprintf(err_message_, sizeof(err_message_), "%m");
			return (INVALID_HANDLE);
		}
		*(ALLOC_SIZE_T *)p = tSize;
		return get_handle((void *)((char *)p + sizeof(ALLOC_SIZE_T)));
	}

	/*************************************************
	  Description:	分配内存，并将内存初始化为0
	  Input:		tSize		分配的内存大小
	  Output:		
	  Return:		内存块句柄，INVALID_HANDLE为失败
	*************************************************/
	ALLOC_HANDLE_T Calloc(ALLOC_SIZE_T tSize)
	{
		void *p = calloc(1, sizeof(ALLOC_SIZE_T) + tSize);
		if (p == NULL) {
			snprintf(err_message_, sizeof(err_message_), "%m");
			return (INVALID_HANDLE);
		}
		*(ALLOC_SIZE_T *)p = tSize;
		return get_handle((void *)((char *)p + sizeof(ALLOC_SIZE_T)));
	}

	/*************************************************
	  Description:	重新分配内存
	  Input:		hHandle	老内存句柄
				tSize		新分配的内存大小
	  Output:		
	  Return:		内存块句柄，INVALID_HANDLE为失败(失败时不会释放老内存块)
	*************************************************/
	ALLOC_HANDLE_T ReAlloc(ALLOC_HANDLE_T hHandle, ALLOC_SIZE_T tSize)
	{
		char *old;
		if (hHandle == INVALID_HANDLE)
			old = NULL;
		else
			old = (char *)0 + (hHandle - sizeof(ALLOC_SIZE_T));
		if (tSize == 0) {
			free(old);
			return (INVALID_HANDLE);
		}
		void *p = realloc(old, sizeof(ALLOC_SIZE_T) + tSize);
		if (p == NULL) {
			snprintf(err_message_, sizeof(err_message_), "%m");
			return (INVALID_HANDLE);
		}
		*(ALLOC_SIZE_T *)p = tSize;
		return get_handle((void *)((char *)p + sizeof(ALLOC_SIZE_T)));
	}

	/*************************************************
	  Description:	释放内存
	  Input:		hHandle	内存句柄
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int Free(ALLOC_HANDLE_T hHandle)
	{
		if (hHandle == INVALID_HANDLE)
			return (0);

		char *old = (char *)0 + (hHandle - sizeof(ALLOC_SIZE_T));
		free(old);
		return (0);
	}

	/*************************************************
	  Description:	获取内存块大小
	  Input:		hHandle	内存句柄
	  Output:		
	  Return:		内存大小
	*************************************************/
	ALLOC_SIZE_T chunk_size(ALLOC_HANDLE_T hHandle)
	{
		if (hHandle == INVALID_HANDLE)
			return (0);

		char *old = (char *)0 + (hHandle - sizeof(ALLOC_SIZE_T));
		return *(ALLOC_SIZE_T *)old;
	}

	/*************************************************
	  Description:	将句柄转换成内存地址
	  Input:		内存句柄
	  Output:		
	  Return:		内存地址，如果句柄无效返回NULL
	*************************************************/
	void *handle_to_ptr(ALLOC_HANDLE_T hHandle)
	{
		return (char *)0 + hHandle;
	}

	/*************************************************
	  Description:	将内存地址转换为句柄
	  Input:		内存地址
	  Output:		
	  Return:		内存句柄，如果地址无效返回INVALID_HANDLE
	*************************************************/
	ALLOC_HANDLE_T ptr_to_handle(void *p)
	{
		return get_handle(p);
	}

	/* not implement */
	ALLOC_SIZE_T ask_for_destroy_size(ALLOC_HANDLE_T hHandle)
	{
		return (ALLOC_SIZE_T)0;
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

extern SysMalloc g_stSysMalloc;

DTC_END_NAMESPACE

#endif
