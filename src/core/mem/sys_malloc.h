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
	  Description:	Allocate memory
	  Input:		tSize		Size of memory to allocate
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure
	**************************************************/
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
	  Description:	Allocate memory and initialize to 0
	  Input:		tSize		Size of memory to allocate
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure
	**************************************************/
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
	  Description:	Reallocate memory
	  Input:		hHandle	Old memory handle
				tSize		New memory size
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure (old memory block not freed on failure)
	**************************************************/
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
	  Description:	Free memory
	  Input:		hHandle	Memory handle
	  Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int Free(ALLOC_HANDLE_T hHandle)
	{
		if (hHandle == INVALID_HANDLE)
			return (0);

		char *old = (char *)0 + (hHandle - sizeof(ALLOC_SIZE_T));
		free(old);
		return (0);
	}

	/*************************************************
	  Description:	Get memory block size
	  Input:		hHandle	Memory handle
	  Output:		
	  Return:		Memory size
	**************************************************/
	ALLOC_SIZE_T chunk_size(ALLOC_HANDLE_T hHandle)
	{
		if (hHandle == INVALID_HANDLE)
			return (0);

		char *old = (char *)0 + (hHandle - sizeof(ALLOC_SIZE_T));
		return *(ALLOC_SIZE_T *)old;
	}

	/*************************************************
	  Description:	Convert handle to memory address
	  Input:		Memory handle
	  Output:		
	  Return:		Memory address, NULL if handle is invalid
	**************************************************/
	void *handle_to_ptr(ALLOC_HANDLE_T hHandle)
	{
		return (char *)0 + hHandle;
	}

	/*************************************************
	  Description:	Convert memory address to handle
	  Input:		Memory address
	  Output:		
	  Return:		Memory handle, INVALID_HANDLE if address is invalid
	**************************************************/
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

extern SysMalloc g_stSysMalloc;

DTC_END_NAMESPACE

#endif
