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

#ifndef TREE_DATA_H
#define TREE_DATA_H

#include "raw/raw_data.h"
#include "t_tree.h"
#include "protocol.h"
#include "task/task_request.h"
#include "value.h"
#include "field/field.h"
#include "section.h"
#include "table/table_def.h"

typedef enum _TreeCheckResult {
	CHK_CONTINUE, // Continue visiting this subtree
	CHK_SKIP, // Skip this subtree, continue visiting other nodes
	CHK_STOP, // Terminate visit loop
	CHK_DESTROY // Destroy this subtree
} TreeCheckResult;

#define TTREE_INDEX_POS 1

typedef TreeCheckResult (*CheckTreeFunc)(MallocBase &stMalloc,
					 uint8_t uchIndexCnt,
					 uint8_t uchCurIdxCnt,
					 const RowValue *pstIndexValue,
					 const uint32_t uiTreeRowNum,
					 void *pCookie);
typedef int (*VisitRawData)(MallocBase &stMalloc, uint8_t uchIndexCnt,
			    const RowValue *pstIndexValue,
			    ALLOC_HANDLE_T &hHandle, int64_t &llRowNumInc,
			    void *pCookie);
class TreeData;
typedef int (TreeData::*SubRowProcess)(DTCJobOperation &job_op,
				       MEM_HANDLE_T hRecord);

class DTCFlushRequest;

/************************************************************
  Description:    Data structure of t-tree root node
  Version:         DTC 3.0
***********************************************************/
struct _RootData {
	unsigned char data_type_;
	uint32_t tree_size_;
	uint32_t total_raw_size_; // Sum of all RawData, excluding Header
	uint32_t node_count_; // Total number of Nodes in index T-tree
	uint32_t row_count_; // Total number of rows in index T-tree
	uint8_t get_request_count_;
	uint16_t latest_request_time_;
	uint16_t latest_update_time_;
	uint16_t create_time_;
	MEM_HANDLE_T root_handle_;
	char p_key_[0];
} __attribute__((packed));
typedef struct _RootData RootData;

class DTCTableDefinition;
typedef struct _CmpCookie {
	const DTCTableDefinition *p_table_;
	uint8_t m_index_;
	_CmpCookie(const DTCTableDefinition *p_table_definition_,
		   uint8_t index_)
	{
		p_table_ = p_table_definition_;
		m_index_ = index_;
	}
} CmpCookie;

typedef struct _pCookie {
	MEM_HANDLE_T *p_handle;
	uint32_t has_got_node_count; // Number of nodes traversed
	uint32_t need_find_node_count; // Number of nodes to traverse, 0 means unlimited
	uint32_t has_got_row_count; // Number of data rows traversed
	_pCookie()
		: p_handle(NULL), has_got_node_count(0),
		  need_find_node_count(0), has_got_row_count(0)
	{
	}
} pResCookie;

typedef enum _CondType {
	COND_VAL_SET, // Query specific value list
	COND_RANGE, // Query value[0] ~ Key-value[0]<=value[1].s64
	COND_GE, // Query keys greater than or equal to value[0]
	COND_LE, // Query keys less than or equal to value[0]
	COND_ALL // Traverse all keys
} CondType;

typedef enum _Order {
	ORDER_ASC, // Ascending order
	ORDER_DEC, // Descending order
	ORDER_POS, // Post-order access
} Order;

typedef struct {
	unsigned char cond_type;
	unsigned char ch_order;
	unsigned int value_num;
	DTCValue *p_value;
} TtreeCondition;

class TreeData {
    private:
	RootData *p_tree_root_; // Note: address may change due to realloc
	Ttree t_tree_;
	DTCTableDefinition *p_table_;
	uint8_t index_depth_;
	int table_index_;
	char err_message_[4096];

	ALLOC_SIZE_T need_new_bufer_size; // Size needed for the most recent memory allocation failure
	uint64_t affected_rows_;

	MEM_HANDLE_T handle_;
	uint32_t size_;
	uint32_t _root_size;
	MallocBase *mallocator_;
	Node *m_pstNode;
	bool m_async;
	int64_t rows_count_;
	int64_t dirty_rows_count_;

	int key_size_;
	uint8_t key_index_;
	int expire_id_;
	int m_iLAId;
	int m_iLCmodId;
	ALLOC_SIZE_T m_uiLAOffset;

	ALLOC_SIZE_T offset_;
	ALLOC_SIZE_T row_offset_;
	char *p_content_;

	bool index_part_of_uniq_field_;
	MEM_HANDLE_T p_record_;

	/************************************************************
	  Description:    Cookie parameters for recursive data search
	  Version:         DTC 3.0
	***********************************************************/
	typedef struct {
		TreeData *m_pst_tree_;
		uint8_t m_uch_cond_idx_cnt_;
		uint8_t m_uch_cur_index_;
		MEM_HANDLE_T m_h_handle_;
		int64_t m_ll_affect_rows_;
		const int *pi_inclusion_;
		KeyComparator m_pf_comp_;
		const RowValue *m_pst_cond_;
		RowValue *m_pst_index_value_;
		VisitRawData m_pf_visit_;
		void *m_pCookie_;
	} CIndexCookie;

	typedef struct {
		TreeData *m_pst_tree_;
		uint8_t m_uch_cur_cond_;
		MEM_HANDLE_T m_h_handle_;
		int64_t m_ll_affect_rows_;
		const TtreeCondition *m_pst_cond_;
		KeyComparator m_pf_comp_;
		RowValue *m_pst_index_value_;
		CheckTreeFunc m_pf_check_;
		VisitRawData m_pf_visit_;
		void *m_p_cookie_;
	} CSearchCookie;

	int set_data_size(unsigned int data_size);
	int set_row_count(unsigned int count);
	unsigned int get_data_size();
	unsigned int get_row_count();

    protected:
	template <class T> T *Pointer(void) const
	{
		return reinterpret_cast<T *>(
			mallocator_->handle_to_ptr(handle_));
	}

	template <class T> T *Pointer(MEM_HANDLE_T handle) const
	{
		return reinterpret_cast<T *>(
			mallocator_->handle_to_ptr(handle));
	}

	int encode_to_private_area(RawData &raw, RowValue &value,
				   unsigned char value_flag);

	inline int pack_key(const RowValue &stRow, uint8_t uchKeyIdx,
			    int &iKeySize, char *&pchKey,
			    unsigned char achKeyBuf[]);
	inline int pack_key(const DTCValue *pstVal, uint8_t uchKeyIdx,
			    int &iKeySize, char *&pchKey,
			    unsigned char achKeyBuf[]);
	inline int unpack_key(char *pchKey, uint8_t uchKeyIdx, RowValue &stRow);

	int insert_sub_tree(uint8_t uchCurIndex, uint8_t uchCondIdxCnt,
			    const RowValue &stCondition, KeyComparator pfComp,
			    ALLOC_HANDLE_T hRoot);
	int insert_sub_tree(uint8_t uchCondIdxCnt, const RowValue &stCondition,
			    KeyComparator pfComp, ALLOC_HANDLE_T hRoot);
	int insert_sub_tree(uint8_t uchCondIdxCnt, KeyComparator pfComp,
			    ALLOC_HANDLE_T hRoot);
	int insert_row_flag(uint8_t uchCurIndex, const RowValue &stRow,
			    KeyComparator pfComp, unsigned char uchFlag);
	int do_find(CIndexCookie *pstIdxCookie);
	int do_find(uint8_t uchCondIdxCnt, const RowValue &stCondition,
		    KeyComparator pfComp, ALLOC_HANDLE_T &hRecord);
	int do_find(uint8_t uchCondIdxCnt, const RowValue &stCondition,
		    KeyComparator pfComp, ALLOC_HANDLE_T *&hRecord);
	static int search_visit(MallocBase &stMalloc, ALLOC_HANDLE_T &hRecord,
				void *pCookie);
	int do_search(CSearchCookie *pstSearchCookie);
	int Delete(CIndexCookie *pstIdxCookie);
	int Delete(uint8_t uchCondIdxCnt, const RowValue &stCondition,
		   KeyComparator pfComp, ALLOC_HANDLE_T &hRecord);

    public:
	TreeData(MallocBase *pstMalloc);
	~TreeData();

	const char *get_err_msg()
	{
		return err_message_;
	}
	MEM_HANDLE_T get_handle()
	{
		return handle_;
	}
	int do_attach(MEM_HANDLE_T hHandle);
	int do_attach(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
		      int laid = -1, int lcmodid = -1, int expireid = -1);

	const MEM_HANDLE_T get_tree_root() const
	{
		return t_tree_.Root();
	}

	/*************************************************
	  Description:	Allocate new memory block and initialize
	  Input:		 iKeySize	Key format, 0 for variable length, non-zero for fixed length
				pchKey	Formatted key, byte 0 of variable length key is the length
	  Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int do_init(int iKeySize, const char *pchKey);
	int do_init(uint8_t uchKeyIdx, int iKeySize, const char *pchKey,
		    int laId = -1, int expireId = -1, int nodeIdx = -1);
	int do_init(const char *pchKey);

	const char *key() const
	{
		return p_tree_root_ ? p_tree_root_->p_key_ : NULL;
	}
	char *key()
	{
		return p_tree_root_ ? p_tree_root_->p_key_ : NULL;
	}

	unsigned int total_rows()
	{
		return p_tree_root_->row_count_;
	}
	uint64_t get_affectedrows()
	{
		return affected_rows_;
	}
	void set_affected_rows(int num)
	{
		affected_rows_ = num;
	}

	/*************************************************
	  Description:	Memory size needed for the most recent memory allocation failure
	  Input:		
	  Output:		
	  Return:		Returns the required memory size
	**************************************************/
	ALLOC_SIZE_T need_size()
	{
		return need_new_bufer_size;
	}

	/*************************************************
	  Description:	Destroy subtrees at uchLevel and below
	  Input:		uchLevel	Destroy subtrees at uchLevel and below, obviously uchLevel should be between 1 and uchIndexDepth
	  Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	//	int destory(uint8_t uchLevel=1);
	int destory();

	/*************************************************
	  Description:	Insert a row of data
	  Input:		stRow	Values of index fields and subsequent fields
				pfComp	User-defined key comparison function
				uchFlag	Row flag
	  Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int insert_row_flag(const RowValue &stRow, KeyComparator pfComp,
			    unsigned char uchFlag);

	/*************************************************
	  Description:	Insert a row of data
	  Input:		stRow	Values of index fields and subsequent fields
				pfComp	User-defined key comparison function
				isDirty	Whether it's dirty data
	  Output:		
	  Return:		0 on success, non-zero on failure
	**************************************************/
	int insert_row(const RowValue &stRow, KeyComparator pfComp,
		       bool isDirty);

	/*************************************************
	  Description:	Find a row of data
	  Input:		stCondition	Values of index fields at all levels
				pfComp	User-defined key comparison function
				
	  Output:		hRecord	Handle pointing to found CRawData
	  Return:		0 if not found, 1 if data is found
	**************************************************/
	int do_find(const RowValue &stCondition, KeyComparator pfComp,
		    ALLOC_HANDLE_T &hRecord);

	/*************************************************
	  Description:	Search by index condition
	  Input:		pstCond	An array with size exactly equal to uchIndexDepth
				pfComp	User-defined key comparison function
				pfVisit	User-defined data access function when record is found
				pCookie	Cookie parameter used by the data access function
	  Output:		
	  Return:		0 on success, other values indicate error
	**************************************************/
	int do_search(const TtreeCondition *pstCond, KeyComparator pfComp,
		      VisitRawData pfVisit, CheckTreeFunc pfCheck,
		      void *pCookie);

	/*************************************************
	  Description:	Traverse all data from small to large
	  Input:		pfComp	User-defined key comparison function
				pfVisit	User-defined data access function when record is found
				pCookie	Cookie parameter used by the data access function
	  Output:		
	  Return:		0 on success, other values indicate error
	**************************************************/
	int traverse_forward(KeyComparator pfComp, VisitRawData pfVisit,
			     void *pCookie);

	/*************************************************
	  Description:	Delete all rows matching conditions based on specified index values (including subtrees)
	  Input:		uchCondIdxCnt	Number of condition indexes
				stCondition		Values of index fields at all levels
				pfComp		User-defined key comparison function
				
	  Output:		
	  Return:		0 on success, other values indicate error
	**************************************************/
	int delete_sub_row(uint8_t uchCondIdxCnt, const RowValue &stCondition,
			   KeyComparator pfComp);

	/*************************************************
	  Description:	Modify index value at a certain level to another value
	  Input:		uchCondIdxCnt	Number of condition indexes
				stCondition		Values of index fields at all levels
				pfComp		User-defined key comparison function
				pstNewValue	New index value for the last condition field
	  Output:		
	  Return:		0 on success, other values indicate error
	**************************************************/
	int update_index(uint8_t uchCondIdxCnt, const RowValue &stCondition,
			 KeyComparator pfComp, const DTCValue *pstNewValue);
	unsigned ask_for_destroy_size(void);

	DTCTableDefinition *get_node_table_def()
	{
		return p_table_;
	}

	void change_mallocator(MallocBase *pstMalloc)
	{
		mallocator_ = pstMalloc;
	}

	int expand_tree_chunk(MEM_HANDLE_T *pRecord, ALLOC_SIZE_T expand_size);

	/*************************************************
	  Description:	destroy data in t-tree
	  Output:		
	*************************************************/
	int destroy_sub_tree();

	/*************************************************
	  Description:	copy data from raw to t-tree
	  Output:		
	*************************************************/
	int copy_tree_all(RawData *new_data);

	/*************************************************
	  Description:	copy data from t-tree to raw
	  Output:		
	*************************************************/
	int copy_raw_all(RawData *new_data);

	/*************************************************
	  Description:	get tree data from t-tree
	  Output:		
	*************************************************/
	int decode_tree_row(RowValue &stRow, unsigned char &uchRowFlags,
			    int iDecodeFlag = 0);

	/*************************************************
	  Description:	set tree data from t-tree
	  Output:		
	*************************************************/
	int encode_tree_row(const RowValue &stRow, unsigned char uchOp);

	/*************************************************
	  Description: compare row data value	
	  Output:		
	*************************************************/
	int compare_tree_data(RowValue *stpNodeRow);

	/*************************************************
	  Description:	get data in t-tree
	  Output:		
	*************************************************/
	int get_tree_data(DTCJobOperation &job_op);

	/*************************************************
	  Description:	flush data in t-tree
	  Output:		
	*************************************************/
	int flush_tree_data(DTCFlushRequest *flush_req, Node *p_node,
			    unsigned int &affected_count);

	/*************************************************
	  Description:	get data in t-tree
	  Output:		
	*************************************************/
	int delete_tree_data(DTCJobOperation &job_op);

	/*************************************************
	  Description:	Get data from each row of Raw type in T-tree
	  Output:		
	**************************************************/
	int get_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	Delete data from Raw type rows in T-tree
	  Output:		
	**************************************************/
	int delete_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	Modify data from Raw type rows in T-tree
	  Output:		
	**************************************************/
	int update_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	Replace data from Raw type rows in T-tree, create if row doesn't exist
	  Output:		
	**************************************************/
	int replace_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	Process flat type operations in T-tree
	  Output:		
	*************************************************/
	int get_sub_raw(DTCJobOperation &job_op, unsigned int nodeCnt,
			bool isAsc, SubRowProcess subRowProc);

	/*************************************************
	  Description:	Match index condition
	  Output:		
	*************************************************/
	int match_index_condition(DTCJobOperation &job_op, unsigned int rowCnt,
				  SubRowProcess subRowProc);

	/*************************************************
	  Description:	update data in t-tree
	  Output:		
	*************************************************/
	int update_tree_data(DTCJobOperation &job_op, Node *p_node,
			     RawData *affected_data, bool async, bool setrows);

	/*************************************************
	  Description:	replace data in t-tree
	  Output:		
	*************************************************/
	int replace_tree_data(DTCJobOperation &job_op, Node *p_node,
			      RawData *affected_data, bool async,
			      unsigned char &RowFlag, bool setrows);

	/*************************************************
	  Description:	calculate row data size
	  Output:		
	*************************************************/
	ALLOC_SIZE_T calc_tree_row_size(const RowValue &stRow, int keyIdx);

	/*************************************************
	  Description:	get expire time
	  Output:		
	*************************************************/
	int get_expire_time(DTCTableDefinition *t, uint32_t &expire);

	/*************************************************
	  Description:	Replace current row
	  Input:		stRow	Only uses row field type info, no actual data needed
	  Output:		
	  Return:		0 on success, non-zero on failure
	*************************************************/
	int replace_cur_row(const RowValue &stRow, bool isDirty,
			    MEM_HANDLE_T *hRecord);

	/*************************************************
	  Description:	Delete current row
	  Input:		stRow	Only uses row field type info, no actual data needed
	  Output:		
	  Return:		0 on success, non-zero on failure
	*************************************************/
	int delete_cur_row(const RowValue &stRow);

	/*************************************************
	  Description:	Skip to next row
	  Input:		stRow	Only uses row field type info, no actual data needed
	  Output:		m_uiOffset will point to next row data offset
	  Return:		0 on success, non-zero on failure
	*************************************************/
	int skip_row(const RowValue &stRow);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int64_t get_increase_dirty_row_count()
	{
		return dirty_rows_count_;
	}

	/*************************************************
	  Description:	Query number of rows added by this operation (can be negative)
	  Input:		
	  Output:		
	  Return:		Number of rows
	*************************************************/
	int64_t get_increase_row_count()
	{
		return rows_count_;
	}

	int set_cur_row_flag(unsigned char uchFlag);

	int get_dirty_row_count();
};

#endif
