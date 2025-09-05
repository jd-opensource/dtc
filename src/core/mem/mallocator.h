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

#ifndef MALLOCATOR_H
#define MALLOCATOR_H

#include <stdint.h>
#include <stdlib.h>
#include "namespace.h"

DTC_BEGIN_NAMESPACE

#define ALLOC_SIZE_T uint32_t
#define ALLOC_HANDLE_T uint64_t
#define INTER_SIZE_T uint64_t
#define INTER_HANDLE_T uint64_t

#define INVALID_HANDLE 0ULL

#define SIZE_SZ (sizeof(ALLOC_SIZE_T))
#define MALLOC_ALIGNMENT (2 * SIZE_SZ)
#define MALLOC_ALIGN_MASK (MALLOC_ALIGNMENT - 1)
#define MAX_ALLOC_SIZE (((ALLOC_SIZE_T)-1) & ~MALLOC_ALIGN_MASK)

class MallocBase {
    public:
	MallocBase()
	{
	}
	virtual ~MallocBase()
	{
	}

	template <class T> T *Pointer(ALLOC_HANDLE_T hHandle)
	{
		return reinterpret_cast<T *>(handle_to_ptr(hHandle));
	}

	virtual ALLOC_HANDLE_T get_handle(void *p) = 0;

	virtual const char *get_err_msg() = 0;

	/*************************************************
	  Description:	Allocate memory
	  Input:		tSize		Size of memory to allocate
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure
	*************************************************/
	virtual ALLOC_HANDLE_T Malloc(ALLOC_SIZE_T tSize) = 0;

	/*************************************************
	  Description:	Allocate memory and initialize it to zero
	  Input:		tSize		Size of memory to allocate
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure
	*************************************************/
	virtual ALLOC_HANDLE_T Calloc(ALLOC_SIZE_T tSize) = 0;

	/*************************************************
	  Description:	Reallocate memory
	  Input:		hHandle	Old memory handle
				tSize		New memory size to allocate
	  Output:		
	  Return:		Memory block handle, INVALID_HANDLE on failure (old memory block will not be freed on failure)
	*************************************************/
	virtual ALLOC_HANDLE_T ReAlloc(ALLOC_HANDLE_T hHandle,
				       ALLOC_SIZE_T tSize) = 0;

	/*************************************************
	  Description:	Free memory
	  Input:		hHandle	Memory handle
	  Output:		
	  Return:		0 on success, non-zero on failure
	*************************************************/
	virtual int Free(ALLOC_HANDLE_T hHandle) = 0;

	/*************************************************
	  Description:	Get memory block size
	  Input:		hHandle	Memory handle
	  Output:		
	  Return:		Memory size
	*************************************************/
	virtual ALLOC_SIZE_T chunk_size(ALLOC_HANDLE_T hHandle) = 0;

	/*************************************************
	  Description:	Convert handle to memory address
	  Input:		Memory handle
	  Output:		
	  Return:		Memory address, returns NULL if handle is invalid
	*************************************************/
	virtual void *handle_to_ptr(ALLOC_HANDLE_T hHandle) = 0;

	/*************************************************
	  Description:	Convert memory address to handle
	  Input:		Memory address
	  Output:		
	  Return:		Memory handle, returns INVALID_HANDLE if address is invalid
	*************************************************/
	virtual ALLOC_HANDLE_T ptr_to_handle(void *p) = 0;

	virtual ALLOC_SIZE_T ask_for_destroy_size(ALLOC_HANDLE_T hHandl) = 0;

	/*************************************************
	  Description:	Check if handle is valid
	  Input:		Memory handle
	  Output:		
      Return:	    0: valid; -1: invalid
	*************************************************/
	virtual int handle_is_valid(ALLOC_HANDLE_T mem_handle) = 0;
};

DTC_END_NAMESPACE

#endif
