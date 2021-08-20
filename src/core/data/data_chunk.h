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

#ifndef DATA_CHUNK_H
#define DATA_CHUNK_H

#include <stdint.h>
#include "raw/raw_data.h"
#include "tree/tree_data.h"

class DataChunk {
    protected:
	unsigned char data_type_; // 数据chunk的类型

    public:
	/*************************************************
	  Description:	计算基本结构大小
	  Input:		
	  Output:		
	  Return:		内存大小
	*************************************************/
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
	  Description:	获取格式化后的key
	  Input:		
	  Output:		
	  Return:		key指针
	*************************************************/
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
	  Description:	获取格式化后的key
	  Input:		
	  Output:		
	  Return:		key指针
	*************************************************/
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
	  Description:	保存key
	  Input:		key	key的实际值
	  Output:		
	  Return:		
	*************************************************/

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
	  Description:	保存字符串key
	  Input:		key	key的实际值
				iLen	key的长度
	  Output:		
	  Return:		
	*************************************************/
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
	  Description:	保存格式化好的字符串key
	  Input:		key	key的实际值, 要求key[0]是长度
	  Output:		
	  Return:		
	*************************************************/
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
	  Description:	查询字符串key大小
	  Input:		
	  Output:		
	  Return:		key大小
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
	  Description:	查询二进制key大小
	  Input:		
	  Output:		
	  Return:		key大小
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
	  Description:	查询数据头大小，如果是CRawData的chunk，data_size()是不包括Row的长度，仅包括头部信息以及key
	  Input:		
	  Output:		
	  Return:		内存大小
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
	  Description:	销毁内存并释放内存
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
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

	/* 查询如果destroy这块内存，能释放多少空间出来 （包括合并）*/
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
