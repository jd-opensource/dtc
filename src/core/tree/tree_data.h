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
	CHK_CONTINUE, // 继续访问这棵子树
	CHK_SKIP, // 忽略这棵子树，继续访问其他节点
	CHK_STOP, // 终止访问循环
	CHK_DESTROY // 销毁这棵子树
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
  Description:    t-tree根节点的数据结构
  Version:         DTC 3.0
***********************************************************/
struct _RootData {
	unsigned char data_type_;
	uint32_t tree_size_;
	uint32_t total_raw_size_; //所有RawData总和，不包含Header
	uint32_t node_count_; //索引T树中Node总计个数
	uint32_t row_count_; //索引T树中总计行数
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
	uint32_t has_got_node_count; //已经遍历到的节点个数
	uint32_t need_find_node_count; //需要遍历的节点个数，0代表不限
	uint32_t has_got_row_count; //已经遍历到的数据行数
	_pCookie()
		: p_handle(NULL), has_got_node_count(0),
		  need_find_node_count(0), has_got_row_count(0)
	{
	}
} pResCookie;

typedef enum _CondType {
	COND_VAL_SET, // 查询特定的值列表
	COND_RANGE, // 查询value[0] ~ Key-value[0]<=value[1].s64
	COND_GE, // 查询大于等于value[0]的key
	COND_LE, // 查询小于等于value[0]的key
	COND_ALL // 遍历所有key
} CondType;

typedef enum _Order {
	ORDER_ASC, // 升序
	ORDER_DEC, // 降序
	ORDER_POS, // 后序访问
} Order;

typedef struct {
	unsigned char cond_type;
	unsigned char ch_order;
	unsigned int value_num;
	DTCValue *p_value;
} TtreeCondition;

class TreeData {
    private:
	RootData *p_tree_root_; // 注意：地址可能会因为realloc而改变
	Ttree t_tree_;
	DTCTableDefinition *p_table_;
	uint8_t index_depth_;
	int table_index_;
	char err_message_[100];

	ALLOC_SIZE_T need_new_bufer_size; // 最近一次分配内存失败需要的大小
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
	  Description:    递归查找数据的cookie参数
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
	  Description:	新分配一块内存，并初始化
	  Input:		 iKeySize	key的格式，0为变长，非0为定长长度
				pchKey	为格式化后的key，变长key的第0字节为长度
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
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
	  Description:	最近一次分配内存失败所需要的内存大小
	  Input:		
	  Output:		
	  Return:		返回所需要的内存大小
	*************************************************/
	ALLOC_SIZE_T need_size()
	{
		return need_new_bufer_size;
	}

	/*************************************************
	  Description:	销毁uchLevel以及以下级别的子树
	  Input:		uchLevel	销毁uchLevel以及以下级别的子树，显然uchLevel应该在1到uchIndexDepth之间
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	//	int destory(uint8_t uchLevel=1);
	int destory();

	/*************************************************
	  Description:	插入一行数据
	  Input:		stRow	包含index字段以及后面字段的值
				pfComp	用户自定义的key比较函数
				uchFlag	行标记
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int insert_row_flag(const RowValue &stRow, KeyComparator pfComp,
			    unsigned char uchFlag);

	/*************************************************
	  Description:	插入一行数据
	  Input:		stRow	包含index字段以及后面字段的值
				pfComp	用户自定义的key比较函数
				isDirty	是否脏数据
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int insert_row(const RowValue &stRow, KeyComparator pfComp,
		       bool isDirty);

	/*************************************************
	  Description:	查找一行数据
	  Input:		stCondition	包含各级index字段的值
				pfComp	用户自定义的key比较函数
				
	  Output:		hRecord	查找到的一个指向CRawData的句柄
	  Return:		0为找不到，1为找到数据
	*************************************************/
	int do_find(const RowValue &stCondition, KeyComparator pfComp,
		    ALLOC_HANDLE_T &hRecord);

	/*************************************************
	  Description:	按索引条件查找
	  Input:		pstCond	一个数组，而且大小刚好是uchIndexDepth
				pfComp	用户自定义的key比较函数
				pfVisit	当查找到记录时，用户自定义的访问数据函数
				pCookie	访问数据函数使用的cookie参数
	  Output:		
	  Return:		0为成功，其他值为错误
	*************************************************/
	int do_search(const TtreeCondition *pstCond, KeyComparator pfComp,
		      VisitRawData pfVisit, CheckTreeFunc pfCheck,
		      void *pCookie);

	/*************************************************
	  Description:	从小到大遍历所有数据
	  Input:		pfComp	用户自定义的key比较函数
				pfVisit	当查找到记录时，用户自定义的访问数据函数
				pCookie	访问数据函数使用的cookie参数
	  Output:		
	  Return:		0为成功，其他值为错误
	*************************************************/
	int traverse_forward(KeyComparator pfComp, VisitRawData pfVisit,
			     void *pCookie);

	/*************************************************
	  Description:	根据指定的index值，删除符合条件的所有行（包括子树）
	  Input:		uchCondIdxCnt	条件index的数量
				stCondition		包含各级index字段的值
				pfComp		用户自定义的key比较函数
				
	  Output:		
	  Return:		0为成功，其他值为错误
	*************************************************/
	int delete_sub_row(uint8_t uchCondIdxCnt, const RowValue &stCondition,
			   KeyComparator pfComp);

	/*************************************************
	  Description:	将某个级别的index值修改为另外一个值
	  Input:		uchCondIdxCnt	条件index的数量
				stCondition		包含各级index字段的值
				pfComp		用户自定义的key比较函数
				pstNewValue	对应最后一个条件字段的新index值
	  Output:		
	  Return:		0为成功，其他值为错误
	*************************************************/
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
	  Description:	获得T树中的Raw类型的每一行的数据
	  Output:		
	*************************************************/
	int get_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	删除T树中的Raw类型的行的数据
	  Output:		
	*************************************************/
	int delete_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	修改T树中的Raw类型的行的数据
	  Output:		
	*************************************************/
	int update_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	替换T树中的Raw类型的行的数据，如没有此行则创建
	  Output:		
	*************************************************/
	int replace_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord);

	/*************************************************
	  Description:	处理T树中平板类型业务
	  Output:		
	*************************************************/
	int get_sub_raw(DTCJobOperation &job_op, unsigned int nodeCnt,
			bool isAsc, SubRowProcess subRowProc);

	/*************************************************
	  Description:	匹配索引
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
	  Description:	替换当前行
	  Input:		stRow	仅使用row的字段类型等信息，不需要实际数据
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int replace_cur_row(const RowValue &stRow, bool isDirty,
			    MEM_HANDLE_T *hRecord);

	/*************************************************
	  Description:	删除当前行
	  Input:		stRow	仅使用row的字段类型等信息，不需要实际数据
	  Output:		
	  Return:		0为成功，非0失败
	*************************************************/
	int delete_cur_row(const RowValue &stRow);

	/*************************************************
	  Description:	调到下一行
	  Input:		stRow	仅使用row的字段类型等信息，不需要实际数据
	  Output:		m_uiOffset会指向下一行数据的偏移
	  Return:		0为成功，非0失败
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
	  Description:	查询本次操作增加的行数（可以为负数）
	  Input:		
	  Output:		
	  Return:		行数
	*************************************************/
	int64_t get_increase_row_count()
	{
		return rows_count_;
	}

	int set_cur_row_flag(unsigned char uchFlag);

	int get_dirty_row_count();
};

#endif
