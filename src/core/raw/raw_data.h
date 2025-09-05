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
	DATA_TYPE_RAW, // flat data structure
	DATA_TYPE_TREE_ROOT, // tree root node
	DATA_TYPE_TREE_NODE // tree node
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
	unsigned char data_type_; // Data type EnumDataType
	uint32_t data_size_; // Total data size
	uint32_t row_count_; // Number of rows
	uint8_t get_request_count_; // Get request count
	uint16_t latest_request_time_; // Latest access time
	uint16_t latest_update_time_; // Latest update time
	uint16_t create_time_; // Creation time
	char p_key_[0]; // Key
	char p_rows_data_[0]; // Row data
} __attribute__((packed));

// Note: Modification operations may cause handle changes, so it's necessary to check and save again
class RawData {
    private:
	char *p_content_; // Note: Address may change due to realloc
	uint32_t data_size_; // Total data size including data_type, data_size, rowcnt, key, rows, etc.
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
	ALLOC_SIZE_T need_new_bufer_size; // Size needed for the most recent failed memory allocation

	MEM_HANDLE_T handle_;
	uint64_t size_;
	MallocBase *mallocator_;
	int auto_destory_;

	RawData *p_reference_;
	char err_message_[4096];

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
	  Description:    Constructor
	  Input:          pstMalloc	Memory allocator
	                     iAutoDestroy	Whether to automatically free memory during destruction
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
	  Description:	Allocate a new block of memory and initialize it
	  Input:		 uchKeyIdx	Index of the field used as key in the table
				iKeySize	Key format, 0 for variable length, non-zero for fixed length
				pchKey	Formatted key, for variable length key, byte 0 is the length
				uiDataSize	Data size, used to allocate a sufficiently large chunk at once. If set to 0, realloc will expand during insert row
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int init(uint8_t uchKeyIdx, int iKeySize, const char *pchKey,
		 ALLOC_SIZE_T uiDataSize = 0, int laid = -1, int expireid = -1,
		 int nodeIdx = -1);
	int do_init(const char *pchKey, ALLOC_SIZE_T uiDataSize = 0);

	/*************************************************
	  Description:	Attach to a block of pre-formatted memory
	  Input:		hHandle	Memory handle
				uchKeyIdx	Index of the field used as key in the table
				iKeySize	Key format, 0 for variable length, non-zero for fixed length
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int do_attach(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
		      int laid = -1, int lastcmod = -1, int expireid = -1);
	int do_attach(MEM_HANDLE_T hHandle);

	/*************************************************
	  Description:	Get the memory block handle
	  Input:		
	  Output:		
	  Return:		Handle. Note: Any modification operation may cause handle changes, so it's necessary to check and save again
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
	  Description:	Set a reference, used when calling CopyRow() or CopyAll()
	  Input:		pstRef	Reference pointer
	  Output:		
	  Return:		
	*************************************************/
	void set_refrence(RawData *pstRef)
	{
		p_reference_ = pstRef;
	}

	/*************************************************
	  Description:	Size of all memory including key, rows, etc.
	  Input:		
	  Output:		
	  Return:		Total size of all memory
	*************************************************/
	uint32_t data_size() const
	{
		return data_size_;
	}

	/*************************************************
	  Description:	Starting offset of rows
	  Input:		
	  Output:		
	  Return:		Starting offset of rows
	*************************************************/
	uint32_t data_start() const
	{
		return data_start_;
	}

	/*************************************************
	  Description:	Return the required memory size when memory allocation fails
	  Input:		
	  Output:		
	  Return:		Return the required memory size
	*************************************************/
	ALLOC_SIZE_T need_size()
	{
		return need_new_bufer_size;
	}

	/*************************************************
	  Description:	Calculate the memory size required for inserting this row
	  Input:		stRow	Row data
	  Output:		
	  Return:		Return the required memory size
	*************************************************/
	ALLOC_SIZE_T calc_row_size(const RowValue &stRow, int keyIndex);

	/*************************************************
	  Description:	Get the formatted key
	  Input:		
	  Output:		
	  Return:		Formatted key
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
	  Description:	Get the key format
	  Input:		
	  Output:		
	  Return:		Returns 0 for variable length, returns fixed length for fixed-length key
	*************************************************/
	int key_format() const
	{
		return key_size_;
	}

	/*************************************************
	  Description:	Get the actual length of the key
	  Input:		
	  Output:		
	  Return:		Actual length of the key
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
	  Description:	Destroy and free memory
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int destory();

	/*************************************************
	  Description:	Free excess memory (usually called once after deleting some rows)
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int strip_mem();

	/*************************************************
	  Description:	Read one row of data
	  Input:		
	  Output:		stRow	Stores row data
				uchRowFlags	Flags for whether row data is dirty, etc.
				iDecodeFlag	Whether it's just pre-read, without fetch_row moving pointer
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int decode_row(RowValue &stRow, unsigned char &uchRowFlags,
		       int iDecodeFlag = 0);

	/*************************************************
	  Description:	Insert one row of data
	  Input:		stRow	Row data to be inserted
	  Output:		
				byFirst	Whether to insert at the front, default is to add at the end
				isDirty	Whether it's dirty data
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int insert_row(const RowValue &stRow, bool byFirst, bool isDirty);

	/*************************************************
	  Description:	Insert one row of data
	  Input:		stRow	Row data to be inserted
	  Output:		
				byFirst	Whether to insert at the front, default is to add at the end
				uchOp	Row flag
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int insert_row_flag(const RowValue &stRow, bool byFirst,
			    unsigned char uchOp);

	/*************************************************
	  Description:	Insert multiple rows of data
	  Input:		uiNRows	Number of rows
				stRow	Row data to be inserted
	  Output:		
				byFirst	Whether to insert at the front, default is to add at the end
				isDirty	Whether it's dirty data
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int insert_n_rows(unsigned int uiNRows, const RowValue *pstRow,
			  bool byFirst, bool isDirty);

	/*************************************************
	  Description:	Replace current row with specified data
	  Input:		stRow	New row data
	  Output:		
				isDirty	Whether it's dirty data
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int replace_cur_row(const RowValue &stRow, bool isDirty);

	/*************************************************
	  Description:	Delete current row
	  Input:		stRow	Only uses field type information from row, no actual data needed
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int delete_cur_row(const RowValue &stRow);

	/*************************************************
	  Description:	Delete all rows
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int delete_all_rows();

	/*************************************************
	  Description:	Set the flag for current row
	  Input:		uchFlag	Row flag
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int set_cur_row_flag(unsigned char uchFlag);

	/*************************************************
	  Description:	Copy current row from reference to end of local buffer
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int copy_row();

	/*************************************************
	  Description:	Replace local data with reference data
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int copy_all();

	/*************************************************
	  Description:	Append N rows of pre-formatted data to the end
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int append_n_records(unsigned int uiNRows, const char *pchData,
			     const unsigned int uiLen);

	/*************************************************
	  Description:	Update last access timestamp
	  Input:	Timestamp	
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
	  Description:	Get last modification time
	  Input:	Timestamp	
	  Output:		
	  Return:
	*************************************************/
	int get_lastcmod(uint32_t &lastcmod);
	int check_size(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
		       int size);

	/*************************************************
	  Description:	Initialize timestamp, including last access time,
	  last update time, and creation time
	  Input:	Timestamp (hours since some absolute event)
	  Although named Update, it's actually only called once
	  tomchen
	*************************************************/
	void init_timp_stamp();
	/*************************************************
	  Description:	Update node's last access time
	  Input:	Timestamp (hours since some absolute event)
	   tomchen
	*************************************************/
	void update_last_access_time_by_hour();
	/*************************************************
	  Description:	Update node's last update time
	  Input:	Timestamp (hours since some absolute event)
	   tomchen
	*************************************************/
	void update_last_update_time_by_hour();
	/*************************************************
	  Description:	Increment the number of select requests for the node
	 tomchen
	*************************************************/
	void inc_select_count();
	/*************************************************
	  Description:	Get node creation time
	 tomchen
	*************************************************/
	uint32_t get_create_time_by_hour();
	/*************************************************
	  Description:	Get node's last access time
	 tomchen
	*************************************************/
	uint32_t get_last_access_time_by_hour();
	/*************************************************
	  Description:	Get node's last update time
	 tomchen
	*************************************************/
	uint32_t get_last_update_time_by_hour();
	/*************************************************
	  Description:	Get the number of select operations on the node
	 tomchen
	*************************************************/
	uint32_t get_select_op_count();
	/*************************************************
	  Description:	Attach timestamp
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
