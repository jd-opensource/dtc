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

#ifndef RAW_DATA_H
#define RAW_DATA_H

#include "mem/pt_malloc.h"
#include "global.h"
#include "field/field.h"
#include "data/col_expand.h"
#include "table/table_def_manager.h"
#include "node/node.h"

#define PRE_DECODE_ROW 1

typedef enum _EnumDataType {
	DATA_TYPE_RAW, // 平板数据结构
	DATA_TYPE_TREE_ROOT, // 树的根节点
	DATA_TYPE_TREE_NODE // 树的节点
} EnumDataType;

typedef enum _enum_oper_type_ {
	OPER_DIRTY = 0x02, // cover INSERT, DELETE, UPDATE
	OPER_SELECT = 0x30,
	OPER_INSERT_OLD = 0x31, // old stuff, same as SELECT aka useless
	OPER_UPDATE = 0x32,
	OPER_DELETE_NA = 0x33, // async DELETE require quite a lot change
	OPER_FLUSH = 0x34, // useless too, same as SELECT
	OPER_RESV1 = 0x35,
	OPER_INSERT = 0x36,
	OPER_RESV2 = 0x37,
} TOperType;

struct RawFormat {
	unsigned char data_type_; // 数据类型EnumDataType
	uint32_t data_size_; // 数据总大小
	uint32_t row_count_; // 行数
	uint8_t get_request_count_; // get次数
	uint16_t latest_request_time_; // 最近访问时间
	uint16_t latest_update_time_; // 最近更新时间
	uint16_t create_time_; // 创建时间
	char p_key_[0]; // key
	char p_rows_data_[0]; // 行数据
} __attribute__((packed));

// 注意：修改操作可能会导致handle改变，因此需要检查重新保存
class RawData {
    private:
	char *p_content_; // 注意：地址可能会因为realloc而改变
	uint32_t data_size_; // 包括data_type,data_size,rowcnt,key,rows等总数据大小
	uint32_t row_count_;
	uint8_t key_index_;
	int key_size_;
	int m_iLAId;
	int m_iLCmodId;
	int expire_id_;
	int table_index_;

	ALLOC_SIZE_T key_start_;
	ALLOC_SIZE_T data_start_;
	ALLOC_SIZE_T row_offset_;
	ALLOC_SIZE_T offset_;
	ALLOC_SIZE_T m_uiLAOffset;
	int get_request_count_offset_;
	int time_stamp_offset_;
	uint8_t get_request_count_;
	uint16_t latest_request_time_;
	uint16_t latest_update_time_;
	uint16_t create_time_;
	ALLOC_SIZE_T need_new_bufer_size; // 最近一次分配内存失败需要的大小

	MEM_HANDLE_T handle_;
	uint64_t size_;
	MallocBase *mallocator_;
	int auto_destory_;

	RawData *p_reference_;
	char err_message_[200];

	DTCTableDefinition *table_definition_;

    protected:
	template <class T> T *Pointer(void) const
	{
		return reinterpret_cast<T *>(
			mallocator_->handle_to_ptr(handle_));
	}

	int set_data_size();
	int set_row_count();
	int expand_chunk(ALLOC_SIZE_T expand_size);
	int re_alloc_chunk(ALLOC_SIZE_T tSize);
	int skip_row(const RowValue &stRow);
	int encode_row(const RowValue &stRow, unsigned char uchOp,
		       bool expendBuf = true);

    public:
	/*************************************************
	  Description:    构造函数
	  Input:          pstMalloc	内存分配器
	                     iAutoDestroy	析构的时候是否自动释放内存
	  Output:         
	  Return:         
	*************************************************/
	RawData(MallocBase *pstMalloc, int iAutoDestroy = 0);

	~RawData();

	void change_mallocator(MallocBase *pstMalloc)
	{
		mallocator_ = pstMalloc;
	}

	const char *get_err_msg()
	{
		return err_message_;
	}

	/*************************************************
	  Description:	新分配一块内存，并初始化
	  Input:		 uchKeyIdx	作为key的字段在table里的下标
				iKeySize	key的格式，0为变长，非0为定长长度
				pchKey	为格式化后的key，变长key的第0字节为长度
				uiDataSize	为数据的大小，用于一次分配足够大的chunk。如果设置为0，则insert row的时候再realloc扩大
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int init(uint8_t uchKeyIdx, int iKeySize, const char *pchKey,
		 ALLOC_SIZE_T uiDataSize = 0, int laid = -1, int expireid = -1,
		 int nodeIdx = -1);
	int do_init(const char *pchKey, ALLOC_SIZE_T uiDataSize = 0);

	/*************************************************
	  Description:	attach一块已经格式化好的内存
	  Input:		hHandle	内存的句柄
				uchKeyIdx	作为key的字段在table里的下标
				iKeySize	key的格式，0为变长，非0为定长长度
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int do_attach(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
		      int laid = -1, int lastcmod = -1, int expireid = -1);
	int do_attach(MEM_HANDLE_T hHandle);

	/*************************************************
	  Description:	获取内存块的句柄
	  Input:		
	  Output:		
	  Return:		句柄。 注意：任何修改操作可能会导致handle改变，因此需要检查重新保存
	*************************************************/
	MEM_HANDLE_T get_handle()
	{
		return handle_;
	}

	const char *get_addr() const
	{
		return p_content_;
	}

	/*************************************************
	  Description:	设置一个refrence，在调用CopyRow()或者CopyAll()的时候使用
	  Input:		pstRef	refrence指针
	  Output:		
	  Return:		
	*************************************************/
	void set_refrence(RawData *pstRef)
	{
		p_reference_ = pstRef;
	}

	/*************************************************
	  Description:	包括key、rows等所有内存的大小
	  Input:		
	  Output:		
	  Return:		所有内存的大小
	*************************************************/
	uint32_t data_size() const
	{
		return data_size_;
	}

	/*************************************************
	  Description:	rows的开始偏移量
	  Input:		
	  Output:		
	  Return:		rows的开始偏移量
	*************************************************/
	uint32_t data_start() const
	{
		return data_start_;
	}

	/*************************************************
	  Description:	内存分配失败时，返回所需要的内存大小
	  Input:		
	  Output:		
	  Return:		返回所需要的内存大小
	*************************************************/
	ALLOC_SIZE_T need_size()
	{
		return need_new_bufer_size;
	}

	/*************************************************
	  Description:	计算插入该行所需要的内存大小
	  Input:		stRow	行数据
	  Output:		
	  Return:		返回所需要的内存大小
	*************************************************/
	ALLOC_SIZE_T calc_row_size(const RowValue &stRow, int keyIndex);

	/*************************************************
	  Description:	获取格式化后的key
	  Input:		
	  Output:		
	  Return:		格式化后的key
	*************************************************/
	const char *key() const
	{
		return p_content_ ? (p_content_ + key_start_) : NULL;
	}
	char *key()
	{
		return p_content_ ? (p_content_ + key_start_) : NULL;
	}

	/*************************************************
	  Description:	获取key的格式
	  Input:		
	  Output:		
	  Return:		变长返回0，定长key返回定长的长度
	*************************************************/
	int key_format() const
	{
		return key_size_;
	}

	/*************************************************
	  Description:	获取key的实际长度
	  Input:		
	  Output:		
	  Return:		key的实际长度
	*************************************************/
	int key_size();

	unsigned int total_rows() const
	{
		return row_count_;
	}
	void rewind(void)
	{
		offset_ = data_start_;
		row_offset_ = data_start_;
	}

	/*************************************************
	  Description:	销毁释放内存
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int destory();

	/*************************************************
	  Description:	释放多余的内存（通常在delete一些row后调用一次）
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int strip_mem();

	/*************************************************
	  Description:	读取一行数据
	  Input:		
	  Output:		stRow	保存行数据
				uchRowFlags	行数据是否脏数据等flag
				iDecodeFlag	是否只是pre-read，不fetch_row移动指针
	  Return:		0为成功，非0失败
	*************************************************/
	int decode_row(RowValue &stRow, unsigned char &uchRowFlags,
		       int iDecodeFlag = 0);

	/*************************************************
	  Description:	插入一行数据
	  Input:		stRow	需要插入的行数据
	  Output:		
				byFirst	是否插入到最前面，默认添加到最后面
				isDirty	是否脏数据
	  Return:		0为成功，非0失败
	*************************************************/
	int insert_row(const RowValue &stRow, bool byFirst, bool isDirty);

	/*************************************************
	  Description:	插入一行数据
	  Input:		stRow	需要插入的行数据
	  Output:		
				byFirst	是否插入到最前面，默认添加到最后面
				uchOp	row的标记
	  Return:		0为成功，非0失败
	*************************************************/
	int insert_row_flag(const RowValue &stRow, bool byFirst,
			    unsigned char uchOp);

	/*************************************************
	  Description:	插入若干行数据
	  Input:		uiNRows	行数
				stRow	需要插入的行数据
	  Output:		
				byFirst	是否插入到最前面，默认添加到最后面
				isDirty	是否脏数据
	  Return:		0为成功，非0失败
	*************************************************/
	int insert_n_rows(unsigned int uiNRows, const RowValue *pstRow,
			  bool byFirst, bool isDirty);

	/*************************************************
	  Description:	用指定数据替换当前行
	  Input:		stRow	新的行数据
	  Output:		
				isDirty	是否脏数据
	  Return:		0为成功，非0失败
	*************************************************/
	int replace_cur_row(const RowValue &stRow, bool isDirty);

	/*************************************************
	  Description:	删除当前行
	  Input:		stRow	仅使用row的字段类型等信息，不需要实际数据
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int delete_cur_row(const RowValue &stRow);

	/*************************************************
	  Description:	删除所有行
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int delete_all_rows();

	/*************************************************
	  Description:	设置当前行的标记
	  Input:		uchFlag	行的标记
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int set_cur_row_flag(unsigned char uchFlag);

	/*************************************************
	  Description:	从refrence copy当前行到本地buffer末尾
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int copy_row();

	/*************************************************
	  Description:	用refrence的数据替换本地数据
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int copy_all();

	/*************************************************
	  Description:	添加N行已经格式化好的数据到末尾
	  Input:		
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int append_n_records(unsigned int uiNRows, const char *pchData,
			     const unsigned int uiLen);

	/*************************************************
	  Description:	更新最后访问时间戳
	  Input:	时间戳	
	  Output:		
	  Return:
	*************************************************/
	void update_lastacc(uint32_t now)
	{
		if (m_uiLAOffset > 0)
			*(uint32_t *)(p_content_ + m_uiLAOffset) = now;
	}
	int get_expire_time(DTCTableDefinition *t, uint32_t &expire);
	/*************************************************
	  Description:	获取最后需改时间
	  Input:	时间戳	
	  Output:		
	  Return:
	*************************************************/
	int get_lastcmod(uint32_t &lastcmod);
	int check_size(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
		       int size);

	/*************************************************
	  Description:	初始化时间戳，包括最后访问时间
	  、最后更新时间、创建时间三部分
	  Input:	时间戳(以某个绝对事件为开始的小时数)
	  虽然名字为Update，其实只会被调用一次
	  tomchen
	*************************************************/
	void init_timp_stamp();
	/*************************************************
	  Description:	更新节点最后访问时间
	  Input:	时间戳(以某个绝对事件为开始的小时数)
	   tomchen
	*************************************************/
	void update_last_access_time_by_hour();
	/*************************************************
	  Description:	更新节点最后更新时间
	  Input:	时间戳(以某个绝对事件为开始的小时数)
	   tomchen
	*************************************************/
	void update_last_update_time_by_hour();
	/*************************************************
	  Description:	增加节点被select请求的次数
	 tomchen
	*************************************************/
	void inc_select_count();
	/*************************************************
	  Description:	获取节点创建时间
	 tomchen
	*************************************************/
	uint32_t get_create_time_by_hour();
	/*************************************************
	  Description:	获取节点最后访问时间
	 tomchen
	*************************************************/
	uint32_t get_last_access_time_by_hour();
	/*************************************************
	  Description:	获取节点最后更新时间
	 tomchen
	*************************************************/
	uint32_t get_last_update_time_by_hour();
	/*************************************************
	  Description:	获取节点被select操作的次数
	 tomchen
	*************************************************/
	uint32_t get_select_op_count();
	/*************************************************
	  Description:	attach上时间戳
	 tomchen
	*************************************************/
	void attach_time_stamp();

	DTCTableDefinition *get_node_table_def();
};

inline int RawData::key_size()
{
	return key_size_ > 0 ? key_size_ :
			       (sizeof(char) + *(unsigned char *)key());
}

#endif
