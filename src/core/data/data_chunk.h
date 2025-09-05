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

#ifndef DATA_CHUNK_H
#define DATA_CHUNK_H

#include <stdint.h>
#include "raw/raw_data.h"
#include "tree/tree_data.h"

class DataChunk {
    protected:
	unsigned char data_type_; // Type of data chunk

    public:
	/*************************************************
	  Description:	Calculate basic structure size
	  Input:		
	  Output:		
	  Return:		Memory size
	***********************************************/
	ALLOC_SIZE_T base_size()
	{
		if (data_type_ == DATA_TYPE_RAW)
			return (sizeof(RawFormat));
		else
			return (sizeof(RootData));
	}

	/*************************************************
	  Description:	index key
	  Input:		
	  Output:		
	  Return:		key
	*************************************************/
	char *index_key()
	{
		char *indexKey = (char *)this;
		return indexKey + sizeof(unsigned char) * 2 +
		       sizeof(uint32_t) * 2;
	}

	/*************************************************
	  Description:	Get formatted key
	  Input:		
	  Output:		
	  Return:		key pointer
	***********************************************/
	const char *key() const
	{
		if ((data_type_ & 0x7f) == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return pstRaw->p_key_;
		} else if ((data_type_ & 0x7f) == DATA_TYPE_TREE_ROOT) {
			RootData *pstRoot = (RootData *)this;
			return pstRoot->p_key_;
		}
		return NULL;
	}

	/*************************************************
	  Description:	Get formatted key
	  Input:		
	  Output:		
	  Return:		key pointer
	***********************************************/
	char *key()
	{
		if ((data_type_ & 0x7f) == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return pstRaw->p_key_;
		} else if ((data_type_ & 0x7f) == DATA_TYPE_TREE_ROOT) {
			RootData *pstRoot = (RootData *)this;
			return pstRoot->p_key_;
		}
		return NULL;
	}

	/*************************************************
	  Description:	Save key
	  Input:		key	actual value of key
	  Output:		
	  Return:		
	**********************************************/

#define SET_KEY_FUNC(type, key)                                                \
	void set_key(type key)                                                 \
	{                                                                      \
		if (data_type_ == DATA_TYPE_RAW) {                             \
			RawFormat *pstRaw = (RawFormat *)this;                 \
			*(type *)(void *)pstRaw->p_key_ = key;                 \
		} else {                                                       \
			RootData *pstRoot = (RootData *)this;                  \
			*(type *)(void *)pstRoot->p_key_ = key;                \
		}                                                              \
	}

	SET_KEY_FUNC(int32_t, iKey)
	SET_KEY_FUNC(uint32_t, uiKey)
	SET_KEY_FUNC(int64_t, llKey)
	SET_KEY_FUNC(uint64_t, ullKey)

	/*************************************************
	  Description:	Save string key
	  Input:		key	actual value of key
				iLen	length of key
	  Output:		
	  Return:		
	**********************************************/
	void set_key(const char *pchKey, int iLen)
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			*(unsigned char *)pstRaw->p_key_ = iLen;
			memcpy(pstRaw->p_key_ + 1, pchKey, iLen);
		} else {
			RootData *pstRoot = (RootData *)this;
			*(unsigned char *)pstRoot->p_key_ = iLen;
			memcpy(pstRoot->p_key_ + 1, pchKey, iLen);
		}
	}

	/*************************************************
	  Description:	Save formatted string key
	  Input:		key	actual value of key, requires key[0] to be length
	  Output:		
	  Return:		
	**********************************************/
	void set_key(const char *pchKey)
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			memcpy(pstRaw->p_key_, pchKey,
			       *(unsigned char *)pchKey);
		} else {
			RootData *pstRoot = (RootData *)this;
			memcpy(pstRoot->p_key_, pchKey,
			       *(unsigned char *)pchKey);
		}
	}

	/*************************************************
	  Description:	Query string key size
	  Input:		
	  Output:		
	  Return:		key size
	*************************************************/
	int str_key_size()
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return *(unsigned char *)pstRaw->p_key_;
		} else {
			RootData *pstRoot = (RootData *)this;
			return *(unsigned char *)pstRoot->p_key_;
		}
	}

	/*************************************************
	  Description:	Query binary key size
	  Input:		
	  Output:		
	  Return:		key size
	*************************************************/
	int bin_key_size()
	{
		return str_key_size();
	}

	unsigned int head_size()
	{
		if (data_type_ == DATA_TYPE_RAW)
			return sizeof(RawFormat);
		else
			return sizeof(RootData);
	}

	/*************************************************
	  Description:	Query data header size. If it's CRawData chunk, data_size() does not include Row length, only header info and key
	  Input:		
	  Output:		
	  Return:		memory size
	*************************************************/
	unsigned int data_size(int iKeySize)
	{
		int iKeyLen = iKeySize ? iKeySize : 1 + str_key_size();
		return head_size() + iKeyLen;
	}

	unsigned int node_size()
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return pstRaw->data_size_;
		} else {
			return 0; // unknow
		}
	}

	unsigned int create_time()
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return pstRaw->create_time_;
		} else {
			return 0; // unknow
		}
	}
	unsigned last_access_time()
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return pstRaw->latest_request_time_;
		} else {
			return 0; // unknow
		}
	}
	unsigned int last_update_time()
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return pstRaw->latest_update_time_;
		} else {
			return 0; // unknow
		}
	}

	uint32_t total_rows()
	{
		if (data_type_ == DATA_TYPE_RAW) {
			RawFormat *pstRaw = (RawFormat *)this;
			return pstRaw->row_count_;
		} else {
			RootData *pstRoot = (RootData *)this;
			return pstRoot->row_count_;
		}
	}

	/*************************************************
	  Description:	Destroy and free memory
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int destory(MallocBase *pstMalloc)
	{
		MEM_HANDLE_T hHandle = pstMalloc->ptr_to_handle(this);
		if (data_type_ == DATA_TYPE_RAW) {
			return pstMalloc->Free(hHandle);
		} else if (data_type_ == DATA_TYPE_TREE_ROOT) {
			TreeData stTree(pstMalloc);
			int iRet = stTree.do_attach(hHandle);
			if (iRet != 0) {
				return (iRet);
			}
			return stTree.destory();
		}
		return (-1);
	}

	/* Query how much space can be freed if this memory is destroyed (including merging) */
	unsigned ask_for_destroy_size(MallocBase *pstMalloc)
	{
		MEM_HANDLE_T hHandle = pstMalloc->ptr_to_handle(this);

		if (data_type_ == DATA_TYPE_RAW) {
			return pstMalloc->ask_for_destroy_size(hHandle);
		} else if (data_type_ == DATA_TYPE_TREE_ROOT) {
			TreeData stTree(pstMalloc);
			if (stTree.do_attach(hHandle))
				return 0;

			return stTree.ask_for_destroy_size();
		}

		log4cplus_debug("ask_for_destroy_size failed");
		return 0;
	}
};

#endif
