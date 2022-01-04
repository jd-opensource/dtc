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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tree_data.h"
#include "global.h"
#include "task/task_pkey.h"
#include "buffer_flush.h"
#include "algorithm/relative_hour_calculator.h"

#ifndef likely
#if __GCC_MAJOR >= 3
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif
#endif

#define GET_TREE_VALUE(x, t)                                                   \
	do {                                                                   \
		if (unlikely(offset_ + sizeof(t) > size_))                     \
			goto ERROR_RET;                                        \
		x = (typeof(x)) * (t *)(p_content_ + offset_);                 \
		offset_ += sizeof(t);                                          \
	} while (0)

#define GET_TREE_VALUE_AT_OFFSET(x, t, offset)                                 \
	do {                                                                   \
		if (unlikely(offset + sizeof(t) > size_))                      \
			goto ERROR_RET;                                        \
		x = (typeof(x)) * (t *)(p_content_ + offset);                  \
	} while (0)

#define SET_TREE_VALUE_AT_OFFSET(x, t, offset)                                 \
	do {                                                                   \
		if (unlikely(offset + sizeof(t) > size_))                      \
			goto ERROR_RET;                                        \
		*(t *)(p_content_ + offset) = x;                               \
	} while (0)

#define SET_TREE_VALUE(x, t)                                                   \
	do {                                                                   \
		if (unlikely(offset_ + sizeof(t) > size_))                     \
			goto ERROR_RET;                                        \
		*(t *)(p_content_ + offset_) = x;                              \
		offset_ += sizeof(t);                                          \
	} while (0)

#define SET_TREE_BIN_VALUE(p, len)                                             \
	do {                                                                   \
		if (unlikely(offset_ + sizeof(int) + len > size_))             \
			goto ERROR_RET;                                        \
		*(int *)(p_content_ + offset_) = len;                          \
		offset_ += sizeof(int);                                        \
		if (likely(len != 0))                                          \
			memcpy(p_content_ + offset_, p, len);                  \
		offset_ += len;                                                \
	} while (0)

#define SKIP_TREE_SIZE(s)                                                      \
	do {                                                                   \
		if (unlikely(offset_ + s > size_))                             \
			goto ERROR_RET;                                        \
		offset_ += s;                                                  \
	} while (0)

TreeData::TreeData(MallocBase *pstMalloc) : t_tree_(*pstMalloc)
{
	p_tree_root_ = NULL;
	index_depth_ = 0;
	need_new_bufer_size = 0;
	key_size_ = 0;
	handle_ = INVALID_HANDLE;
	table_index_ = -1;
	size_ = 0;
	_root_size = 0;
	mallocator_ = pstMalloc;
	memset(err_message_, 0, sizeof(err_message_));

	key_index_ = -1;
	expire_id_ = -1;
	m_iLAId = -1;
	m_iLCmodId = -1;

	offset_ = 0;
	row_offset_ = 0;
	affected_rows_ = 0;

	index_part_of_uniq_field_ = false;
	p_record_ = INVALID_HANDLE;
}

TreeData::~TreeData()
{
	handle_ = INVALID_HANDLE;
	_root_size = 0;
}

int TreeData::do_init(uint8_t uchKeyIdx, int iKeySize, const char *pchKey,
		      int laId, int expireId, int nodeIdx)
{
	int ks = iKeySize != 0 ? iKeySize : 1 + *(unsigned char *)pchKey;
	int uiDataSize = 2 + sizeof(uint32_t) * 4 + sizeof(uint16_t) * 3 +
			 sizeof(MEM_HANDLE_T) + ks;

	handle_ = INVALID_HANDLE;
	_root_size = 0;

	handle_ = mallocator_->Malloc(uiDataSize);
	if (handle_ == INVALID_HANDLE) {
		snprintf(err_message_, sizeof(err_message_), "malloc error");
		need_new_bufer_size = uiDataSize;
		return (EC_NO_MEM);
	}
	_root_size = mallocator_->chunk_size(handle_);

	p_tree_root_ = Pointer<RootData>();
	p_tree_root_->data_type_ =
		((table_index_ << 7) & 0x80) + DATA_TYPE_TREE_ROOT;
	p_tree_root_->tree_size_ = 0;
	p_tree_root_->total_raw_size_ = 0;
	p_tree_root_->node_count_ = 0;
	p_tree_root_->row_count_ = 0;
	p_tree_root_->root_handle_ = INVALID_HANDLE;

	p_tree_root_->get_request_count_ = 1;

	m_uiLAOffset = 0;

	key_size_ = iKeySize;
	key_index_ = uchKeyIdx;
	m_iLAId = laId;
	expire_id_ = expireId;
	if (nodeIdx != -1) {
		table_index_ = nodeIdx;
	}
	if (table_index_ != 0 && table_index_ != 1) {
		snprintf(err_message_, sizeof(err_message_), "node idx error");
		return -100;
	}

	if (iKeySize != 0) {
		memcpy(p_tree_root_->p_key_, pchKey, iKeySize);
	} else {
		memcpy(p_tree_root_->p_key_, pchKey, ks);
	}

	t_tree_.do_attach(INVALID_HANDLE);

	return (0);
}

int TreeData::do_init(const char *pchKey)
{
	if (DTCColExpand::instance()->is_expanding())
		table_index_ =
			(DTCColExpand::instance()->cur_table_idx() + 1) % 2;
	else
		table_index_ = DTCColExpand::instance()->cur_table_idx() % 2;
	if (table_index_ != 0 && table_index_ != 1) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach error, nodeIdx[%d] error", table_index_);
		return -1;
	}
	p_table_ = TableDefinitionManager::instance()->get_table_def_by_idx(
		table_index_);
	if (p_table_ == NULL) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach error, tabledef[NULL]");
		return -1;
	}

	return do_init(p_table_->key_fields() - 1, p_table_->key_format(),
		       pchKey, p_table_->lastacc_field_id(),
		       p_table_->expire_time_field_id());
}

int TreeData::do_attach(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
			int laid, int lcmodid, int expireid)
{
	_root_size = mallocator_->chunk_size(hHandle);
	if (unlikely(_root_size == 0)) {
		snprintf(err_message_, sizeof(err_message_), "attach error: %s",
			 mallocator_->get_err_msg());
		return (-1);
	}
	handle_ = hHandle;

	p_tree_root_ = Pointer<RootData>();

	unsigned char uchType;
	uchType = p_tree_root_->data_type_;
	if (unlikely((uchType & 0x7f) != DATA_TYPE_TREE_ROOT)) {
		snprintf(err_message_, sizeof(err_message_),
			 "invalid data type: %u", uchType);
		return (-2);
	}

	m_uiLAOffset = 0;

	key_size_ = iKeySize;
	key_index_ = uchKeyIdx;
	expire_id_ = expireid;
	m_iLAId = laid;
	m_iLCmodId = lcmodid;

	t_tree_.do_attach(p_tree_root_->root_handle_);

	return (0);
}

int TreeData::do_attach(MEM_HANDLE_T hHandle)
{
	handle_ = hHandle;
	char *p = Pointer<char>();
	table_index_ = (*p >> 7) & 0x01;
	if (table_index_ != 0 && table_index_ != 1) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach error, nodeIdx[%d] error", table_index_);
		return -1;
	}
	p_table_ = TableDefinitionManager::instance()->get_table_def_by_idx(
		table_index_);
	if (p_table_ == NULL) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach error, tabledef[NULL]");
		return -1;
	}
	return do_attach(hHandle, p_table_->key_fields() - 1,
			 p_table_->key_format(), p_table_->lastacc_field_id(),
			 p_table_->lastcmod_field_id(),
			 p_table_->expire_time_field_id());
}

int TreeData::encode_tree_row(const RowValue &stRow, unsigned char uchOp)
{
	SET_TREE_VALUE(uchOp, unsigned char);
	for (int j = 1; j <= stRow.num_fields(); j++) //¿½±´Ò»ÐÐÊý¾Ý
	{
		if (stRow.table_definition()->is_discard(j))
			continue;
		const DTCValue *const v = stRow.field_value(j);
		switch (stRow.field_type(j)) {
		case DField::Signed:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(int32_t)))
				SET_TREE_VALUE(v->s64, int64_t);
			else
				SET_TREE_VALUE(v->s64, int32_t);
			break;

		case DField::Unsigned:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(uint32_t)))
				SET_TREE_VALUE(v->u64, uint64_t);
			else
				SET_TREE_VALUE(v->u64, uint32_t);
			break;

		case DField::Float:
			if (likely(stRow.field_size(j) > (int)sizeof(float)))
				SET_TREE_VALUE(v->flt, double);
			else
				SET_TREE_VALUE(v->flt, float);
			break;

		case DField::String:
		case DField::Binary:
		default: {
			SET_TREE_BIN_VALUE(v->bin.ptr, v->bin.len);
			break;
		}
		} //end of switch
	}

	return 0;

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "encode row error");
	return (-100);
}

int TreeData::expand_tree_chunk(MEM_HANDLE_T *pRecord, ALLOC_SIZE_T expand_size)
{
	if (pRecord == NULL) {
		snprintf(err_message_, sizeof(err_message_),
			 "tree data not init yet");
		return (-1);
	}

	uint32_t dataSize = *(uint32_t *)(p_content_ + sizeof(unsigned char));
	if (dataSize + expand_size > size_) {
		ALLOC_HANDLE_T hTmp = mallocator_->ReAlloc(
			(*pRecord), dataSize + expand_size);
		if (hTmp == INVALID_HANDLE) {
			snprintf(err_message_, sizeof(err_message_),
				 "realloc error[%s]",
				 mallocator_->get_err_msg());
			need_new_bufer_size = dataSize + expand_size;
			return (EC_NO_MEM);
		}
		p_tree_root_->tree_size_ -= size_;
		*pRecord = hTmp;
		size_ = mallocator_->chunk_size(hTmp);
		p_content_ = Pointer<char>(*pRecord);
		p_tree_root_->tree_size_ += size_;
	}
	return (0);
}

int TreeData::insert_sub_tree(uint8_t uchCondIdxCnt,
			      const RowValue &stCondition, KeyComparator pfComp,
			      ALLOC_HANDLE_T hRoot)
{
	int iRet;
	if (uchCondIdxCnt != TTREE_INDEX_POS) {
		snprintf(err_message_, sizeof(err_message_),
			 "index field error");
		return (-100);
	}

	bool isAllocNode = false;
	DTCValue value = stCondition[TTREE_INDEX_POS];
	char *indexKey = reinterpret_cast<char *>(&value);
	CmpCookie cookie(p_table_, uchCondIdxCnt);
	iRet = t_tree_.do_insert(indexKey, &cookie, pfComp, hRoot, isAllocNode);
	if (iRet == 0 && isAllocNode) {
		p_tree_root_->tree_size_ += sizeof(TtreeNode);
	}
	return iRet;
}

int TreeData::do_find(uint8_t uchCondIdxCnt, const RowValue &stCondition,
		      KeyComparator pfComp, ALLOC_HANDLE_T *&hRecord)
{
	int iRet;
	if (uchCondIdxCnt != TTREE_INDEX_POS) {
		snprintf(err_message_, sizeof(err_message_),
			 "index field error");
		return (-100);
	}

	DTCValue value = stCondition[TTREE_INDEX_POS];
	char *indexKey = reinterpret_cast<char *>(&value);
	CmpCookie cookie(p_table_, uchCondIdxCnt);
	iRet = t_tree_.do_find(indexKey, &cookie, pfComp, hRecord);
	return iRet;
}

int TreeData::insert_row_flag(const RowValue &stRow, KeyComparator pfComp,
			      unsigned char uchFlag)
{
	int iRet;
	uint32_t rowCnt = 0;
	MEM_HANDLE_T *pRecord = NULL;
	MEM_HANDLE_T hRecord = INVALID_HANDLE;
	int trowSize = calc_tree_row_size(stRow, 0);
	int tSize = 0;
	offset_ = 0;

	iRet = do_find(TTREE_INDEX_POS, stRow, pfComp, pRecord);
	if (iRet == -100)
		return iRet;
	if (pRecord == NULL) {
		tSize = trowSize + sizeof(unsigned char) + sizeof(uint32_t) * 2;
		hRecord = mallocator_->Malloc(tSize);
		if (hRecord == INVALID_HANDLE) {
			need_new_bufer_size = tSize;
			snprintf(err_message_, sizeof(err_message_),
				 "malloc error");
			return (EC_NO_MEM);
		}
		size_ = mallocator_->chunk_size(hRecord);
		p_content_ = Pointer<char>(hRecord);
		*p_content_ = DATA_TYPE_TREE_NODE; //RawFormat->DataType
		offset_ += sizeof(unsigned char);
		*(uint32_t *)(p_content_ + offset_) = 0; //RawFormat->data_size
		offset_ += sizeof(uint32_t);
		*(uint32_t *)(p_content_ + offset_) = 0; //RawFormat->RowCount
		offset_ += sizeof(uint32_t);

		iRet = encode_tree_row(stRow, uchFlag);
		if (iRet != 0) {
			goto ERROR_INSERT_RET;
		}

		iRet = insert_sub_tree(TTREE_INDEX_POS, stRow, pfComp, hRecord);
		if (iRet != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "insert error");
			need_new_bufer_size = sizeof(TtreeNode);
			mallocator_->Free(hRecord);
			goto ERROR_INSERT_RET;
		}
		p_tree_root_->tree_size_ += size_;
		p_tree_root_->node_count_++;
	} else {
		p_content_ = Pointer<char>(*pRecord);
		size_ = mallocator_->chunk_size(*pRecord);
		iRet = expand_tree_chunk(pRecord, trowSize);
		if (iRet != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "expand tree chunk error");
			return iRet;
		}

		offset_ = *(uint32_t *)(p_content_ +
					sizeof(unsigned char)); //datasize

		iRet = encode_tree_row(stRow, uchFlag);
		if (iRet != 0) {
			goto ERROR_INSERT_RET;
		}
	}

	/*每次insert数据之后，更新头部信息*/
	rowCnt = *(uint32_t *)(p_content_ + sizeof(unsigned char) +
			       sizeof(uint32_t));
	*(uint32_t *)(p_content_ + sizeof(unsigned char)) = offset_;
	*(uint32_t *)(p_content_ + sizeof(unsigned char) + sizeof(uint32_t)) =
		rowCnt + 1;
	p_tree_root_->root_handle_ = t_tree_.Root();
	p_tree_root_->row_count_ += 1;
	p_tree_root_->total_raw_size_ += trowSize;

ERROR_INSERT_RET:
	offset_ = 0;
	size_ = 0;
	hRecord = INVALID_HANDLE;
	p_content_ = NULL;

	return (iRet);
}

int TreeData::insert_row(const RowValue &stRow, KeyComparator pfComp,
			 bool isDirty)
{
	return insert_row_flag(stRow, pfComp,
			       isDirty ? OPER_INSERT : OPER_SELECT);
}

unsigned TreeData::ask_for_destroy_size(void)
{
	if (unlikely(_root_size == 0)) {
		snprintf(err_message_, sizeof(err_message_), "attach error: %s",
			 mallocator_->get_err_msg());
		return (-1);
	}
	return p_tree_root_->tree_size_ + _root_size;
}

int TreeData::destory()
{
	if (unlikely(_root_size == 0)) {
		snprintf(err_message_, sizeof(err_message_), "attach error: %s",
			 mallocator_->get_err_msg());
		return (-1);
	}
	t_tree_.destory();
	mallocator_->Free(handle_);

	handle_ = INVALID_HANDLE;
	_root_size = 0;
	return (0);
}

int TreeData::copy_raw_all(RawData *new_data)
{
	int iRet;
	uint32_t totalNodeCnt = p_tree_root_->node_count_;
	if (totalNodeCnt == 0) {
		return 1;
	}
	pResCookie resCookie;
	MEM_HANDLE_T pCookie[totalNodeCnt];
	resCookie.p_handle = pCookie;
	resCookie.need_find_node_count = 0;
	iRet = t_tree_.traverse_forward(Visit, &resCookie);
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 " traverse tree-data rows error:%d", iRet);
		return (-1);
	}
	ALLOC_SIZE_T headlen = sizeof(unsigned char) + sizeof(uint32_t) * 2;
	for (uint32_t i = 0; i < resCookie.has_got_node_count; i++) {
		char *pch = Pointer<char>(pCookie[i]);
		ALLOC_SIZE_T dtsize =
			*(uint32_t *)(pch + sizeof(unsigned char));

		uint32_t rowcnt = *(uint32_t *)(pch + sizeof(unsigned char) +
						sizeof(uint32_t));
		iRet = new_data->append_n_records(rowcnt, pch + headlen,
						  dtsize - headlen);
		if (iRet != 0)
			return iRet;
	}
	if ((iRet = new_data->do_attach(new_data->get_handle())) != 0)
		return (iRet);

	return 0;
}

int TreeData::copy_tree_all(RawData *new_data)
{
	int iRet;
	if (p_table_->num_fields() < 1) {
		log4cplus_error("field nums is too short");
		return -1;
	}

	unsigned int uiTotalRows = new_data->total_rows();
	if (uiTotalRows == 0)
		return (0);

	new_data->rewind();
	RowValue stOldRow(p_table_);
	for (unsigned int i = 0; i < uiTotalRows; i++) {
		unsigned char uchRowFlags;
		stOldRow.default_value();
		if (new_data->decode_row(stOldRow, uchRowFlags, 0) != 0) {
			log4cplus_error("raw-data decode row error: %s",
					new_data->get_err_msg());
			return (-1);
		}

		iRet = insert_row(stOldRow, KeyCompare, false);
		if (iRet == EC_NO_MEM) {
			/*这里为了下次完全重新建立T树，把未建立完的树全部删除*/
			need_new_bufer_size =
				new_data->data_size() - new_data->data_start();
			destroy_sub_tree();
			return (EC_NO_MEM);
		}
	}

	return (0);
}

int TreeData::decode_tree_row(RowValue &stRow, unsigned char &uchRowFlags,
			      int iDecodeFlag)
{
	row_offset_ = offset_;

	GET_TREE_VALUE(uchRowFlags, unsigned char);
	for (int j = 1; j <= stRow.num_fields(); j++) {
		if (stRow.table_definition()->is_discard(j))
			continue;
		if (j == m_iLAId)
			m_uiLAOffset = offset_;
		switch (stRow.field_type(j)) {
		case DField::Signed:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(int32_t))) {
				GET_TREE_VALUE(stRow.field_value(j)->s64,
					       int64_t);
			} else {
				GET_TREE_VALUE(stRow.field_value(j)->s64,
					       int32_t);
			}
			break;

		case DField::Unsigned:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(uint32_t))) {
				GET_TREE_VALUE(stRow.field_value(j)->u64,
					       uint64_t);
			} else {
				GET_TREE_VALUE(stRow.field_value(j)->u64,
					       uint32_t);
			}
			break;

		case DField::Float:
			if (likely(stRow.field_size(j) > (int)sizeof(float))) {
				GET_TREE_VALUE(stRow.field_value(j)->flt,
					       double);
			} else {
				GET_TREE_VALUE(stRow.field_value(j)->flt,
					       float);
			}
			break;

		case DField::String:
		case DField::Binary:
		default: {
			GET_TREE_VALUE(stRow.field_value(j)->bin.len, int);
			stRow.field_value(j)->bin.ptr = p_content_ + offset_;
			SKIP_TREE_SIZE((uint32_t)stRow.field_value(j)->bin.len);
			break;
		}
		} //end of switch
	}
	return (0);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "get value error");
	return (-100);
}

int TreeData::compare_tree_data(RowValue *stpNodeRow)
{
	uint32_t rowCnt = p_tree_root_->node_count_;
	if (rowCnt == 0) {
		return 1;
	}

	const uint8_t *ufli = p_table_->uniq_fields_list();
	for (int i = 0;
	     !index_part_of_uniq_field_ && i < p_table_->uniq_fields(); i++) {
		if (ufli[i] == TTREE_INDEX_POS) {
			index_part_of_uniq_field_ = true;
			break;
		}
	}

	if (index_part_of_uniq_field_) {
		MEM_HANDLE_T *pRecord = NULL;
		RowValue stOldRow(p_table_);
		char *indexKey = reinterpret_cast<char *>(
			stpNodeRow->field_value(TTREE_INDEX_POS));
		CmpCookie cookie(p_table_, TTREE_INDEX_POS);
		int iRet =
			t_tree_.do_find(indexKey, &cookie, KeyCompare, pRecord);
		if (iRet == -100)
			return iRet;
		if (pRecord != NULL) {
			p_content_ = Pointer<char>(*pRecord);
			uint32_t rows = *(uint32_t *)(p_content_ +
						      sizeof(unsigned char) +
						      sizeof(uint32_t));
			offset_ = sizeof(unsigned char) + sizeof(uint32_t) * 2;
			size_ = mallocator_->chunk_size(*pRecord);

			for (uint32_t j = 0; j < rows; j++) {
				stOldRow.default_value();
				unsigned char uchRowFlags;
				if (decode_tree_row(stOldRow, uchRowFlags, 0) !=
				    0) {
					return (-2);
				}
				if (stpNodeRow->Compare(
					    stOldRow,
					    p_table_->uniq_fields_list(),
					    p_table_->uniq_fields()) == 0) {
					p_record_ = *pRecord;
					return 0;
				}
			}
		}
	} else {
		pResCookie resCookie;
		MEM_HANDLE_T pCookie[rowCnt];
		resCookie.p_handle = pCookie;
		resCookie.need_find_node_count = 0;
		if (t_tree_.traverse_forward(Visit, &resCookie) != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 " traverse tree-data rows error");
			return (-1);
		}

		RowValue stOldRow(p_table_);
		for (uint32_t i = 0; i < resCookie.has_got_node_count;
		     i++) { //逐行拷贝数据
			p_content_ = Pointer<char>(pCookie[i]);
			uint32_t rows = *(uint32_t *)(p_content_ +
						      sizeof(unsigned char) +
						      sizeof(uint32_t));
			offset_ = sizeof(unsigned char) + sizeof(uint32_t) * 2;
			size_ = mallocator_->chunk_size(pCookie[i]);

			for (uint32_t j = 0; j < rows; j++) {
				stOldRow.default_value();
				unsigned char uchRowFlags;
				if (decode_tree_row(stOldRow, uchRowFlags, 0) !=
				    0) {
					return (-2);
				}
				if (stpNodeRow->Compare(
					    stOldRow,
					    p_table_->uniq_fields_list(),
					    p_table_->uniq_fields()) == 0) {
					p_record_ = pCookie[i];
					return 0;
				}
			}
		}
	}

	return 1;
}

int TreeData::replace_tree_data(DTCJobOperation &job_op, Node *p_node,
				RawData *affected_data, bool async,
				unsigned char &RowFlag, bool setrows)
{
	int iRet;
	unsigned int uiTotalRows = 0;
	uint32_t iDelete = 0;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	stpNodeTab = p_table_;
	stpTaskTab = job_op.table_definition();
	RowValue stNewRow(stpTaskTab);
	RowValue stNewNodeRow(stpNodeTab);
	affected_rows_ = 0;

	stpTaskRow = &stNewRow;
	stpNodeRow = &stNewNodeRow;
	if (stpNodeTab == stpTaskTab)
		stpNodeRow = stpTaskRow;

	stNewRow.default_value();
	job_op.update_row(*stpTaskRow);

	if (stpNodeTab != stpTaskTab)
		stpNodeRow->Copy(stpTaskRow);
	else
		stpNodeRow = stpTaskRow;

	iRet = compare_tree_data(stpNodeRow);
	if (iRet < 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "compare tree data error:%d", iRet);
		return iRet;
	} else if (iRet == 0) {
		DTCValue new_value = (*stpTaskRow)[TTREE_INDEX_POS];
		char *NewIndex = reinterpret_cast<char *>(&new_value);
		CmpCookie cookie(p_table_, TTREE_INDEX_POS);
		if (KeyCompare(NewIndex, &cookie, *mallocator_, p_record_) !=
		    0) //Index字段变更
		{
			char *tmp_pchContent = p_content_;
			uint32_t tmp_size = size_;
			ALLOC_SIZE_T tmp_uiOffset = offset_;
			iRet = insert_row(*stpTaskRow, KeyCompare, m_async);
			p_content_ = tmp_pchContent;
			size_ = tmp_size;
			offset_ = tmp_uiOffset;

			if (iRet == EC_NO_MEM)
				return iRet;
			else if (iRet == 0) {
				offset_ = row_offset_;
				RowValue stOldRow(p_table_);
				stOldRow.default_value();
				unsigned char uchRowFlags;
				if (decode_tree_row(stOldRow, uchRowFlags, 0) !=
				    0) {
					return (-2);
				}
				RowFlag = uchRowFlags;
				uiTotalRows = get_row_count();
				offset_ = row_offset_;
				if (delete_cur_row(stOldRow) == 0)
					iDelete++;

				if (uiTotalRows > 0 && uiTotalRows == iDelete &&
				    get_row_count() ==
					    0) //RowFormat上的内容已删光
				{
					//删除tree node
					bool isFreeNode = false;
					DTCValue value = (stOldRow)
						[TTREE_INDEX_POS]; //for轮询的最后一行数据
					char *indexKey =
						reinterpret_cast<char *>(
							&value);
					CmpCookie cookie(p_table_,
							 TTREE_INDEX_POS);
					int iret = t_tree_.Delete(indexKey,
								  &cookie,
								  KeyCompare,
								  isFreeNode);
					if (iret != 0) {
						snprintf(
							err_message_,
							sizeof(err_message_),
							"delete stTree failed:%d",
							iret);
						return -4;
					}
					if (isFreeNode)
						p_tree_root_->tree_size_ -=
							sizeof(TtreeNode);
					p_tree_root_->tree_size_ -= size_;
					p_tree_root_->node_count_--;
					p_tree_root_->root_handle_ =
						t_tree_.Root();
					//释放handle
					mallocator_->Free(p_record_);
				}
			}
		} else //Index字段不变
		{
			MEM_HANDLE_T *pRawHandle = NULL;
			int iRet = do_find(TTREE_INDEX_POS, *stpNodeRow,
					   KeyCompare, pRawHandle);
			if (iRet == -100 || iRet == 0)
				return iRet;

			iRet = replace_cur_row(*stpNodeRow, m_async,
					       pRawHandle); // 加进cache
			if (iRet == EC_NO_MEM) {
				return iRet;
			}
			if (iRet != 0) {
				/*标记加入黑名单*/
				job_op.push_black_list_size(need_size());
				return (-6);
			}
		}
		affected_rows_ = 2;
	}
	return 0;
}

int TreeData::replace_sub_raw_data(DTCJobOperation &job_op,
				   MEM_HANDLE_T hRecord)
{
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow, *stpCurRow;

	stpNodeTab = p_table_;
	stpTaskTab = job_op.table_definition();
	RowValue stNewRow(stpTaskTab);
	RowValue stNewNodeRow(stpNodeTab);
	RowValue stCurRow(stpNodeTab);

	stpTaskRow = &stNewRow;
	stpNodeRow = &stNewNodeRow;
	stpCurRow = &stCurRow;
	if (stpNodeTab == stpTaskTab)
		stpNodeRow = stpTaskRow;

	p_content_ = Pointer<char>(hRecord);
	unsigned int uiTotalRows = get_row_count();
	offset_ = sizeof(unsigned char) +
		  sizeof(uint32_t) * 2; //offset DataType + data_size + RowCount
	size_ = mallocator_->chunk_size(hRecord);

	unsigned char uchRowFlags;
	uint32_t iDelete = 0;
	uint32_t iInsert = 0;
	for (unsigned int i = 0; i < uiTotalRows; i++) {
		if (decode_tree_row(*stpNodeRow, uchRowFlags, 0) != 0)
			return (-1);

		if (stpNodeTab != stpTaskTab)
			stpTaskRow->Copy(stpNodeRow);

		stpCurRow->Copy(stpNodeRow);

		//如果不符合查询条件
		if (job_op.compare_row(*stpTaskRow) == 0)
			continue;

		MEM_HANDLE_T *pRawHandle = NULL;
		int iRet = do_find(TTREE_INDEX_POS, *stpCurRow, KeyCompare,
				   pRawHandle);
		if (iRet == -100 || iRet == 0)
			return iRet;

		job_op.update_row(*stpTaskRow); //修改数据

		if (stpNodeTab != stpTaskTab)
			stpNodeRow->Copy(stpTaskRow);

		if (affected_rows_ == 0) {
			iRet = 0;
			DTCValue new_value = (*stpTaskRow)[TTREE_INDEX_POS];
			char *NewIndex = reinterpret_cast<char *>(&new_value);
			CmpCookie cookie(p_table_, TTREE_INDEX_POS);

			if (KeyCompare(NewIndex, &cookie, *mallocator_,
				       hRecord) != 0) //update Index字段
			{
				char *tmp_pchContent = p_content_;
				uint32_t tmp_size = size_;
				ALLOC_SIZE_T tmp_uiOffset = offset_;

				iRet = insert_row(*stpTaskRow, KeyCompare,
						  m_async);

				p_content_ = tmp_pchContent;
				size_ = tmp_size;
				offset_ = tmp_uiOffset;
				if (iRet == EC_NO_MEM) {
					return iRet;
				} else if (iRet == 0) {
					iInsert++;
					offset_ = row_offset_;
					if (delete_cur_row(*stpCurRow) == 0)
						iDelete++;
				}
			} else {
				iRet = replace_cur_row(*stpNodeRow, m_async,
						       pRawHandle); // 加进cache
				if (iRet == EC_NO_MEM) {
					return iRet;
				}
				if (iRet != 0) {
					/*标记加入黑名单*/
					job_op.push_black_list_size(
						need_size());
					return (-6);
				}
			}

			affected_rows_ += 2;
		} else {
			if (delete_cur_row(*stpCurRow) == 0) {
				iDelete++;
				affected_rows_++;
			}
		}
	}

	if (uiTotalRows > 0 &&
	    uiTotalRows - iDelete == 0) //RowFormat上的内容已删光
	{
		//删除tree node
		bool isFreeNode = false;
		DTCValue value =
			(*stpCurRow)[TTREE_INDEX_POS]; //for轮询的最后一行数据
		char *indexKey = reinterpret_cast<char *>(&value);
		CmpCookie cookie(p_table_, TTREE_INDEX_POS);
		int iret = t_tree_.Delete(indexKey, &cookie, KeyCompare,
					  isFreeNode);
		if (iret != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "delete stTree failed:%d", iret);
			return -4;
		}
		if (isFreeNode)
			p_tree_root_->tree_size_ -= sizeof(TtreeNode);
		p_tree_root_->tree_size_ -= size_;
		p_tree_root_->node_count_--;
		p_tree_root_->root_handle_ = t_tree_.Root();
		//释放handle
		mallocator_->Free(hRecord);
	}

	return 0;
}

/*
 * encode到私有内存，防止replace，update引起重新rellocate导致value引用了过期指针
 */
int TreeData::encode_to_private_area(RawData &raw, RowValue &value,
				     unsigned char value_flag)
{
	int ret = raw.do_init(
		key(), raw.calc_row_size(value, p_table_->key_fields() - 1));
	if (0 != ret) {
		log4cplus_error("init raw-data struct error, ret=%d, err=%s",
				ret, raw.get_err_msg());
		return -1;
	}

	ret = raw.insert_row(value, false, false);
	if (0 != ret) {
		log4cplus_error("insert row to raw-data error: ret=%d, err=%s",
				ret, raw.get_err_msg());
		return -2;
	}

	raw.rewind();

	ret = raw.decode_row(value, value_flag, 0);
	if (0 != ret) {
		log4cplus_error("decode raw-data to row error: ret=%d, err=%s",
				ret, raw.get_err_msg());
		return -3;
	}

	return 0;
}

int TreeData::update_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord)
{
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow, *stpCurRow;

	stpNodeTab = p_table_;
	stpTaskTab = job_op.table_definition();
	RowValue stNewRow(stpTaskTab);
	RowValue stNewNodeRow(stpNodeTab);
	RowValue stCurRow(stpNodeTab);

	stpTaskRow = &stNewRow;
	stpNodeRow = &stNewNodeRow;
	stpCurRow = &stCurRow;
	if (stpNodeTab == stpTaskTab)
		stpNodeRow = stpTaskRow;

	p_content_ = Pointer<char>(hRecord);
	unsigned int uiTotalRows = get_row_count();
	offset_ = sizeof(unsigned char) +
		  sizeof(uint32_t) * 2; //offset DataType + data_size + RowCount
	size_ = mallocator_->chunk_size(hRecord);

	unsigned char uchRowFlags;
	uint32_t iDelete = 0;
	uint32_t iInsert = 0;
	for (unsigned int i = 0; i < uiTotalRows; i++) {
		if (decode_tree_row(*stpNodeRow, uchRowFlags, 0) != 0)
			return (-1);

		if (stpNodeTab != stpTaskTab)
			stpTaskRow->Copy(stpNodeRow);

		stpCurRow->Copy(stpNodeRow);

		//如果不符合查询条件
		if (job_op.compare_row(*stpTaskRow) == 0)
			continue;

		MEM_HANDLE_T *pRawHandle = NULL;
		int iRet = do_find(TTREE_INDEX_POS, *stpCurRow, KeyCompare,
				   pRawHandle);
		if (iRet == -100 || iRet == 0)
			return iRet;

		job_op.update_row(*stpTaskRow); //修改数据

		if (stpNodeTab != stpTaskTab)
			stpNodeRow->Copy(stpTaskRow);

		iRet = 0;
		DTCValue new_value = (*stpTaskRow)[TTREE_INDEX_POS];
		char *NewIndex = reinterpret_cast<char *>(&new_value);
		CmpCookie cookie(p_table_, TTREE_INDEX_POS);

		if (KeyCompare(NewIndex, &cookie, *mallocator_, hRecord) !=
		    0) //update Index字段
		{
			char *tmp_pchContent = p_content_;
			uint32_t tmp_size = size_;
			ALLOC_SIZE_T tmp_uiOffset = offset_;

			iRet = insert_row(*stpTaskRow, KeyCompare, m_async);

			p_content_ = tmp_pchContent;
			size_ = tmp_size;
			offset_ = tmp_uiOffset;
			if (iRet == EC_NO_MEM) {
				return iRet;
			} else if (iRet == 0) {
				iInsert++;
				offset_ = row_offset_;
				if (delete_cur_row(*stpCurRow) == 0)
					iDelete++;
			}
		} else {
			// 在私有区间decode
			RawData stTmpRows(&g_stSysMalloc, 1);
			if (encode_to_private_area(stTmpRows, *stpNodeRow,
						   uchRowFlags)) {
				log4cplus_error(
					"encode rowvalue to private rawdata area failed");
				return -3;
			}

			iRet = replace_cur_row(*stpNodeRow, m_async,
					       pRawHandle); // 加进cache
			if (iRet == EC_NO_MEM) {
				return iRet;
			}
			if (iRet != 0) {
				/*标记加入黑名单*/
				job_op.push_black_list_size(need_size());
				return (-6);
			}
		}

		affected_rows_++;
		if (uchRowFlags & OPER_DIRTY)
			dirty_rows_count_--;
		if (m_async)
			dirty_rows_count_++;
	}

	if (uiTotalRows > 0 &&
	    uiTotalRows - iDelete == 0) //RowFormat上的内容已删光
	{
		//删除tree node
		bool isFreeNode = false;
		DTCValue value =
			(*stpCurRow)[TTREE_INDEX_POS]; //for轮询的最后一行数据
		char *indexKey = reinterpret_cast<char *>(&value);
		CmpCookie cookie(p_table_, TTREE_INDEX_POS);
		int iret = t_tree_.Delete(indexKey, &cookie, KeyCompare,
					  isFreeNode);
		if (iret != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "delete stTree failed:%d", iret);
			return -4;
		}
		if (isFreeNode)
			p_tree_root_->tree_size_ -= sizeof(TtreeNode);
		p_tree_root_->tree_size_ -= size_;
		p_tree_root_->node_count_--;
		p_tree_root_->root_handle_ = t_tree_.Root();
		//释放handle
		mallocator_->Free(hRecord);
	}

	if (iInsert != iDelete) {
		snprintf(err_message_, sizeof(err_message_),
			 "update index change error: insert:%d, delete:%d",
			 iInsert, iDelete);
		return (-10);
	}

	return 0;
}

int TreeData::delete_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	stpNodeTab = p_table_;
	stpTaskTab = job_op.table_definition();
	RowValue stNodeRow(stpNodeTab);
	RowValue stTaskRow(stpTaskTab);
	if (stpNodeTab == stpTaskTab) {
		stpNodeRow = &stTaskRow;
		stpTaskRow = &stTaskRow;
	} else {
		stpNodeRow = &stNodeRow;
		stpTaskRow = &stTaskRow;
	}

	unsigned int iAffectRows = 0;
	unsigned char uchRowFlags;

	p_content_ = Pointer<char>(hRecord);
	unsigned int uiTotalRows = get_row_count();
	offset_ = sizeof(unsigned char) +
		  sizeof(uint32_t) * 2; //offset DataType + data_size + RowCount
	size_ = mallocator_->chunk_size(hRecord);

	for (unsigned int i = 0; i < uiTotalRows; i++) {
		if ((decode_tree_row(*stpNodeRow, uchRowFlags, 0)) != 0) {
			return (-2);
		}
		if (stpNodeTab != stpTaskTab) {
			stpTaskRow->Copy(stpNodeRow);
		}
		if (job_op.compare_row(*stpTaskRow) != 0) { //符合del条件
			iRet = delete_cur_row(*stpNodeRow);
			if (iRet != 0) {
				log4cplus_error(
					"tree-data delete row error: %d", iRet);
				return (-5);
			}
			iAffectRows++;
			rows_count_--;
			if (uchRowFlags & OPER_DIRTY)
				dirty_rows_count_--;
		}
	}

	if (iAffectRows > uiTotalRows)
		return (-3);
	else if (iAffectRows == uiTotalRows &&
		 uiTotalRows > 0) //RowFormat上的内容已删光
	{
		//删除tree node
		bool isFreeNode = false;
		DTCValue value =
			(*stpNodeRow)[TTREE_INDEX_POS]; //for轮询的最后一行数据
		char *indexKey = reinterpret_cast<char *>(&value);
		CmpCookie cookie(p_table_, TTREE_INDEX_POS);
		int iret = t_tree_.Delete(indexKey, &cookie, KeyCompare,
					  isFreeNode);
		if (iret != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "delete stTree failed:%d\t%s", iret,
				 t_tree_.get_err_msg());
			return -4;
		}
		if (isFreeNode)
			p_tree_root_->tree_size_ -= sizeof(TtreeNode);
		p_tree_root_->tree_size_ -= size_;
		p_tree_root_->node_count_--;
		p_tree_root_->root_handle_ = t_tree_.Root();
		//释放handle
		mallocator_->Free(hRecord);
	}

	return (0);
}

int TreeData::skip_row(const RowValue &stRow)
{
	if (p_content_ == NULL) {
		snprintf(err_message_, sizeof(err_message_),
			 "rawdata not init yet");
		return (-1);
	}

	offset_ = row_offset_;
	if (offset_ >= get_data_size()) {
		snprintf(err_message_, sizeof(err_message_),
			 "already at end of data");
		return (-2);
	}

	SKIP_TREE_SIZE(sizeof(unsigned char)); // flag

	for (int j = key_index_ + 1; j <= stRow.num_fields();
	     j++) //拷贝一行数据
	{
		//id: bug fix skip discard
		if (stRow.table_definition()->is_discard(j))
			continue;
		int temp = 0;
		switch (stRow.field_type(j)) {
		case DField::Unsigned:
		case DField::Signed:
			GET_TREE_VALUE_AT_OFFSET(temp, int, offset_);

			if (stRow.field_size(j) > (int)sizeof(int32_t))
				SKIP_TREE_SIZE(sizeof(int64_t));
			else
				SKIP_TREE_SIZE(sizeof(int32_t));
			;
			break;

		case DField::Float: //浮点数
			if (stRow.field_size(j) > (int)sizeof(float))
				SKIP_TREE_SIZE(sizeof(double));
			else
				SKIP_TREE_SIZE(sizeof(float));
			break;

		case DField::String: //字符串
		case DField::Binary: //二进制数据
		default: {
			int iLen;
			GET_TREE_VALUE(iLen, int);
			SKIP_TREE_SIZE(iLen);
			break;
		}
		} //end of switch
	}

	return (0);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "skip row error");
	return (-100);
}

int TreeData::replace_cur_row(const RowValue &stRow, bool isDirty,
			      MEM_HANDLE_T *hRecord)
{
	int iRet = 0;
	ALLOC_SIZE_T uiOldOffset;
	ALLOC_SIZE_T uiNextRowsSize;
	ALLOC_SIZE_T uiNewRowSize = 0;
	ALLOC_SIZE_T uiCurRowSize = 0;
	ALLOC_SIZE_T uiNextRowsOffset;
	ALLOC_SIZE_T uiDataSize = get_data_size();

	uiOldOffset = offset_;
	if ((iRet = skip_row(stRow)) != 0) {
		goto ERROR_RET;
	}

	unsigned char uchRowFlag;
	GET_TREE_VALUE_AT_OFFSET(uchRowFlag, unsigned char, row_offset_);
	if (isDirty)
		uchRowFlag = OPER_UPDATE;

	uiNewRowSize = calc_tree_row_size(stRow, key_index_);
	uiCurRowSize = offset_ - row_offset_;
	uiNextRowsOffset = offset_;
	uiNextRowsSize = uiDataSize - offset_;

	if (uiNewRowSize > uiCurRowSize) {
		// enlarge buffer
		MEM_HANDLE_T hTmp = mallocator_->ReAlloc(
			*hRecord, uiDataSize + uiNewRowSize - uiCurRowSize);
		if (hTmp == INVALID_HANDLE) {
			snprintf(err_message_, sizeof(err_message_),
				 "realloc error");
			need_new_bufer_size =
				uiDataSize + uiNewRowSize - uiCurRowSize;
			iRet = EC_NO_MEM;
			goto ERROR_RET;
		}
		p_tree_root_->tree_size_ -= size_;
		*hRecord = hTmp;
		size_ = mallocator_->chunk_size(*hRecord);
		p_tree_root_->tree_size_ += size_;
		p_content_ = Pointer<char>(*hRecord);

		// move data
		if (uiNextRowsSize > 0)
			memmove(p_content_ + uiNextRowsOffset +
					(uiNewRowSize - uiCurRowSize),
				p_content_ + uiNextRowsOffset, uiNextRowsSize);

		// copy new row
		offset_ = row_offset_;
		iRet = encode_tree_row(stRow, uchRowFlag);
		if (iRet != 0) {
			if (uiNextRowsSize > 0)
				memmove(p_content_ + uiNextRowsOffset,
					p_content_ + uiNextRowsOffset +
						(uiNewRowSize - uiCurRowSize),
					uiNextRowsSize);
			iRet = -1;
			goto ERROR_RET;
		}
	} else {
		// back up old row
		void *pTmpBuf = MALLOC(uiCurRowSize);
		if (pTmpBuf == NULL) {
			need_new_bufer_size = uiCurRowSize;
			snprintf(err_message_, sizeof(err_message_),
				 "malloc error: %m");
			return (-ENOMEM);
		}
		memmove(pTmpBuf, p_content_ + row_offset_, uiCurRowSize);

		// copy new row
		offset_ = row_offset_;
		iRet = encode_tree_row(stRow, uchRowFlag);
		if (iRet != 0) {
			memmove(p_content_ + row_offset_, pTmpBuf,
				uiCurRowSize);
			FREE(pTmpBuf);
			iRet = -1;
			goto ERROR_RET;
		}

		// move data
		if (uiNextRowsSize > 0 && offset_ != uiNextRowsOffset)
			memmove(p_content_ + offset_,
				p_content_ + uiNextRowsOffset, uiNextRowsSize);
		FREE(pTmpBuf);

		// shorten buffer
		MEM_HANDLE_T hTmp = mallocator_->ReAlloc(
			*hRecord, uiDataSize + uiNewRowSize - uiCurRowSize);
		if (hTmp != INVALID_HANDLE) {
			p_tree_root_->tree_size_ -= size_;
			*hRecord = hTmp;
			size_ = mallocator_->chunk_size(*hRecord);
			p_tree_root_->tree_size_ += size_;
			p_content_ = Pointer<char>(*hRecord);
		}
	}
	set_data_size(uiDataSize - uiCurRowSize + uiNewRowSize);
	p_tree_root_->total_raw_size_ += (uiNewRowSize - uiCurRowSize);

ERROR_RET:
	offset_ = uiOldOffset + uiNewRowSize - uiCurRowSize;
	return (iRet);
}

int TreeData::delete_cur_row(const RowValue &stRow)
{
	int iRet = 0;
	ALLOC_SIZE_T uiOldOffset;
	ALLOC_SIZE_T uiNextRowsSize;

	uiOldOffset = offset_;
	if ((iRet = skip_row(stRow)) != 0) {
		log4cplus_error("skip error: %d,%s", iRet, get_err_msg());
		goto ERROR_RET;
	}
	uiNextRowsSize = get_data_size() - offset_;

	memmove(p_content_ + row_offset_, p_content_ + offset_, uiNextRowsSize);
	set_row_count(get_row_count() - 1);
	set_data_size(get_data_size() - (offset_ - row_offset_));

	p_tree_root_->row_count_--;
	p_tree_root_->total_raw_size_ -= (offset_ - row_offset_);

	offset_ = row_offset_;
	return (iRet);

ERROR_RET:
	offset_ = uiOldOffset;
	return (iRet);
}

int TreeData::get_sub_raw_data(DTCJobOperation &job_op, MEM_HANDLE_T hRecord)
{
	//	int laid = job_op.flag_no_cache() ? -1 : job_op.table_definition()->lastacc_field_id();

	if (job_op.result_full())
		return 0;

	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;
	stpNodeTab = p_table_;
	stpTaskTab = job_op.table_definition();
	RowValue stNodeRow(stpNodeTab);
	RowValue stTaskRow(stpTaskTab);
	if (stpNodeTab == stpTaskTab) {
		stpNodeRow = &stTaskRow;
		stpTaskRow = &stTaskRow;
	} else {
		stpNodeRow = &stNodeRow;
		stpTaskRow = &stTaskRow;
	}

	p_content_ = Pointer<char>(hRecord);
	uint32_t rows = get_row_count();
	offset_ = sizeof(unsigned char) + sizeof(uint32_t) * 2;
	size_ = mallocator_->chunk_size(hRecord);

	unsigned char uchRowFlags;
	for (unsigned int j = 0; j < rows; j++) {
		job_op.update_key(
			*stpNodeRow); // use stpNodeRow is fine, as just modify key field
		if ((decode_tree_row(*stpNodeRow, uchRowFlags, 0)) != 0) {
			return (-2);
		}
		// this pointer compare is ok, as these two is both come from tabledefmanager. if they mean same, they are same object.
		if (stpNodeTab != stpTaskTab) {
			stpTaskRow->Copy(stpNodeRow);
		}
		if (job_op.compare_row(*stpTaskRow) == 0) //如果不符合查询条件
			continue;

		if (stpTaskTab->expire_time_field_id() > 0)
			stpTaskRow->update_expire_time();
		//当前行添加到task中
		log4cplus_debug("append_row flag");
		job_op.append_row(stpTaskRow);

		if (job_op.all_rows() && job_op.result_full()) {
			job_op.set_total_rows((int)rows);
			break;
		}
	}
	return 0;
}

int TreeData::get_sub_raw(DTCJobOperation &job_op, unsigned int nodeCnt,
			  bool isAsc, SubRowProcess subRowProc)
{
	pResCookie resCookie;
	MEM_HANDLE_T pCookie[nodeCnt];
	resCookie.p_handle = pCookie;

	if (job_op.all_rows() &&
	    job_op.requestInfo.limit_count() >
		    0) //condition: ONLY `LIMIT` without `WHERE`
		resCookie.need_find_node_count =
			job_op.requestInfo.limit_start() +
			job_op.requestInfo.limit_count();
	else
		resCookie.need_find_node_count = 0;

	t_tree_.traverse_forward(Visit, &resCookie);

	if (isAsc) //升序
	{
		for (int i = 0; i < (int)resCookie.has_got_node_count; i++) {
			int iRet = (this->*subRowProc)(job_op, pCookie[i]);
			if (iRet != 0)
				return iRet;
		}
	} else //降序
	{
		for (int i = (int)resCookie.has_got_node_count - 1; i >= 0;
		     i--) {
			int iRet = (this->*subRowProc)(job_op, pCookie[i]);
			if (iRet != 0)
				return iRet;
		}
	}

	return 0;
}

int TreeData::match_index_condition(DTCJobOperation &job_op,
				    unsigned int NodeCnt,
				    SubRowProcess subRowProc)
{
	const DTCFieldValue *condition = job_op.request_condition();
	int numfields = 0; //条件字段个数
	bool isAsc = !(p_table_->is_desc_order(TTREE_INDEX_POS));

	if (condition)
		numfields = condition->num_fields();

	int indexIdArr[numfields]; //开辟空间比实际使用的大
	int indexCount = 0; //条件索引个数
	int firstEQIndex = -1; //第一个EQ在indexIdArr中的位置

	for (int i = 0; i < numfields; i++) {
		if (condition->field_id(i) == TTREE_INDEX_POS) {
			if (firstEQIndex == -1 &&
			    condition->field_operation(i) == DField::EQ)
				firstEQIndex = i;
			indexIdArr[indexCount++] = i;
		}
	}

	if (indexCount == 0 ||
	    (indexCount == 1 && condition->field_operation(indexIdArr[0]) ==
					DField::NE)) { //平板类型
		int iret = get_sub_raw(job_op, NodeCnt, isAsc, subRowProc);
		if (iret != 0)
			return iret;
	} else if (firstEQIndex != -1) //有至少一个EQ条件
	{
		MEM_HANDLE_T *pRecord = NULL;

		char *indexKey = reinterpret_cast<char *>(
			condition->field_value(firstEQIndex));
		CmpCookie cookie(p_table_, TTREE_INDEX_POS);
		int iRet =
			t_tree_.do_find(indexKey, &cookie, KeyCompare, pRecord);
		if (iRet == -100)
			return iRet;
		if (pRecord != NULL) {
			iRet = (this->*subRowProc)(job_op, *pRecord);
			if (iRet != 0)
				return iRet;
		}
	} else {
		int leftId = -1;
		int rightId = -1;

		for (int i = 0; i < indexCount; i++) {
			switch (condition->field_operation(indexIdArr[i])) {
			case DField::LT:
			case DField::LE:
				if (rightId == -1)
					rightId = indexIdArr[i];
				break;

			case DField::GT:
			case DField::GE:
				if (leftId == -1)
					leftId = indexIdArr[i];
				break;

			default:
				break;
			}
		}

		if (leftId != -1 && rightId == -1) //GE
		{
			pResCookie resCookie;
			MEM_HANDLE_T pCookie[NodeCnt];
			resCookie.p_handle = pCookie;
			resCookie.need_find_node_count = 0;
			char *indexKey = reinterpret_cast<char *>(
				condition->field_value(leftId));
			CmpCookie cookie(p_table_, TTREE_INDEX_POS);

			if (t_tree_.traverse_forward(indexKey, &cookie,
						     KeyCompare, Visit,
						     &resCookie) != 0) {
				snprintf(err_message_, sizeof(err_message_),
					 " traverse tree-data rows error");
				return (-1);
			}

			if (isAsc) {
				for (int i = 0;
				     i < (int)resCookie.has_got_node_count;
				     i++) {
					int iRet = (this->*subRowProc)(
						job_op, pCookie[i]);
					if (iRet != 0)
						return iRet;
				}
			} else {
				for (int i = (int)resCookie.has_got_node_count -
					     1;
				     i >= 0; i--) {
					int iRet = (this->*subRowProc)(
						job_op, pCookie[i]);
					if (iRet != 0)
						return iRet;
				}
			}
		} else if (leftId == -1 && rightId != -1) //LE
		{
			pResCookie resCookie;
			MEM_HANDLE_T pCookie[NodeCnt];
			resCookie.p_handle = pCookie;
			resCookie.need_find_node_count = NodeCnt;
			char *indexKey = reinterpret_cast<char *>(
				condition->field_value(rightId));
			CmpCookie cookie(p_table_, TTREE_INDEX_POS);

			if (t_tree_.traverse_backward(indexKey, &cookie,
						      KeyCompare, Visit,
						      &resCookie) != 0) {
				snprintf(err_message_, sizeof(err_message_),
					 " traverse tree-data rows error");
				return (-1);
			}

			if (isAsc) {
				for (int i = (int)resCookie.has_got_node_count -
					     1;
				     i >= 0; i--) {
					int iRet = (this->*subRowProc)(
						job_op, pCookie[i]);
					if (iRet != 0)
						return iRet;
				}
			} else {
				for (int i = 0;
				     i < (int)resCookie.has_got_node_count;
				     i++) {
					int iRet = (this->*subRowProc)(
						job_op, pCookie[i]);
					if (iRet != 0)
						return iRet;
				}
			}
		} else if (leftId != -1 && rightId != -1) //range
		{
			pResCookie resCookie;
			MEM_HANDLE_T pCookie[NodeCnt];
			resCookie.p_handle = pCookie;
			resCookie.need_find_node_count = 0;
			char *beginKey = reinterpret_cast<char *>(
				condition->field_value(leftId));
			char *endKey = reinterpret_cast<char *>(
				condition->field_value(rightId));
			CmpCookie cookie(p_table_, TTREE_INDEX_POS);

			if (t_tree_.traverse_forward(beginKey, endKey, &cookie,
						     KeyCompare, Visit,
						     &resCookie) != 0) {
				snprintf(err_message_, sizeof(err_message_),
					 " traverse tree-data rows error");
				return (-1);
			}

			if (isAsc) {
				for (int i = 0;
				     i < (int)resCookie.has_got_node_count;
				     i++) {
					int iRet = (this->*subRowProc)(
						job_op, pCookie[i]);
					if (iRet != 0)
						return iRet;
				}
			} else {
				for (int i = (int)resCookie.has_got_node_count -
					     1;
				     i >= 0; i--) {
					int iRet = (this->*subRowProc)(
						job_op, pCookie[i]);
					if (iRet != 0)
						return iRet;
				}
			}
		} else //may all NE, raw data process
		{
			int iret =
				get_sub_raw(job_op, NodeCnt, isAsc, subRowProc);
			if (iret != 0)
				return iret;
		}
	}

	return 0;
}

int TreeData::get_dirty_row_count()
{
	unsigned int uiTotalNodes = p_tree_root_->node_count_;
	int dirty_rows = 0;
	pResCookie resCookie;
	MEM_HANDLE_T pCookie[uiTotalNodes];
	resCookie.p_handle = pCookie;
	resCookie.need_find_node_count = 0;

	RowValue stRow(p_table_);

	t_tree_.traverse_forward(Visit, &resCookie);

	for (int i = 0; i < (int)resCookie.has_got_node_count; i++) {
		p_content_ = Pointer<char>(pCookie[i]);
		uint32_t rows = get_row_count();
		offset_ = sizeof(unsigned char) + sizeof(uint32_t) * 2;
		size_ = mallocator_->chunk_size(pCookie[i]);

		unsigned char uchRowFlags;
		for (unsigned int j = 0; j < rows; j++) {
			if (decode_tree_row(stRow, uchRowFlags, 0) != 0) {
				log4cplus_error(
					"subraw-data decode row error: %s",
					get_err_msg());
				return (-1);
			}

			if (uchRowFlags & OPER_DIRTY)
				dirty_rows++;
		}
	}

	return dirty_rows;
}

int TreeData::flush_tree_data(DTCFlushRequest *flush_req, Node *p_node,
			      unsigned int &affected_count)
{
	unsigned int uiTotalNodes = p_tree_root_->node_count_;

	affected_count = 0;
	DTCValue astKey[p_table_->key_fields()];
	TaskPackedKey::unpack_key(p_table_, key(), astKey);
	RowValue stRow(p_table_); //一行数据
	for (int i = 0; i < p_table_->key_fields(); i++)
		stRow[i] = astKey[i];

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	pResCookie resCookie;
	MEM_HANDLE_T pCookie[uiTotalNodes];
	resCookie.p_handle = pCookie;
	resCookie.need_find_node_count = 0;

	t_tree_.traverse_forward(Visit, &resCookie);

	for (int i = 0; i < (int)resCookie.has_got_node_count; i++) {
		p_content_ = Pointer<char>(pCookie[i]);
		uint32_t rows = get_row_count();
		offset_ = sizeof(unsigned char) + sizeof(uint32_t) * 2;
		size_ = mallocator_->chunk_size(pCookie[i]);

		unsigned char uchRowFlags;
		for (unsigned int j = 0; j < rows; j++) {
			if (decode_tree_row(stRow, uchRowFlags, 0) != 0) {
				log4cplus_error(
					"subraw-data decode row error: %s",
					get_err_msg());
				return (-1);
			}

			if ((uchRowFlags & OPER_DIRTY) == false)
				continue;

			if (flush_req && flush_req->flush_row(stRow) != 0) {
				log4cplus_error(
					"do_flush() invoke flushRow() failed.");
				return (-2);
			}
			set_cur_row_flag(uchRowFlags & ~OPER_DIRTY);
			dirty_rows_count_--;
			affected_count++;
		}
	}

	return 0;
}

int TreeData::get_tree_data(DTCJobOperation &job_op)
{
	uint32_t rowCnt = p_tree_root_->row_count_;
	if (rowCnt == 0) {
		return 0;
	}

	job_op.prepare_result(); //准备返回结果对象
	if (job_op.all_rows() &&
	    (job_op.count_only() || !job_op.in_range((int)rowCnt, 0))) {
		if (job_op.is_batch_request()) {
			if ((int)rowCnt > 0)
				job_op.add_total_rows((int)rowCnt);
		} else {
			job_op.set_total_rows((int)rowCnt);
		}
	} else {
		int iret =
			match_index_condition(job_op, p_tree_root_->node_count_,
					      &TreeData::get_sub_raw_data);
		if (iret != 0)
			return iret;
	}

	return 0;
}

int TreeData::update_tree_data(DTCJobOperation &job_op, Node *p_node,
			       RawData *affected_data, bool async, bool setrows)
{
	uint32_t rowCnt = p_tree_root_->node_count_;
	if (rowCnt == 0) {
		return 0;
	}

	m_pstNode = p_node;
	m_async = async;
	dirty_rows_count_ = 0;

	return match_index_condition(job_op, rowCnt,
				     &TreeData::update_sub_raw_data);
}

int TreeData::delete_tree_data(DTCJobOperation &job_op)
{
	uint32_t rowCnt = p_tree_root_->node_count_;
	if (rowCnt == 0) {
		return 0;
	}

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	job_op.prepare_result(); //准备返回结果对象
	if (job_op.all_rows() &&
	    (job_op.count_only() || !job_op.in_range((int)rowCnt, 0))) {
		if (job_op.is_batch_request()) {
			if ((int)rowCnt > 0)
				job_op.add_total_rows((int)rowCnt);
		} else {
			job_op.set_total_rows((int)rowCnt);
		}
	} else {
		int iret = match_index_condition(
			job_op, rowCnt, &TreeData::delete_sub_raw_data);
		if (iret != 0)
			return iret;
	}

	return 0;
}

int TreeData::get_expire_time(DTCTableDefinition *t, uint32_t &expire)
{
	expire = 0;
	if (unlikely(handle_ == INVALID_HANDLE)) {
		snprintf(err_message_, sizeof(err_message_),
			 "root tree data not init yet");
		return (-1);
	}
	if (expire_id_ == -1) {
		expire = 0;
		return 0;
	}

	MEM_HANDLE_T root = get_tree_root();
	if (unlikely(root == INVALID_HANDLE)) {
		snprintf(err_message_, sizeof(err_message_),
			 "root tree data not init yet");
		return (-1);
	}

	MEM_HANDLE_T firstHanle = t_tree_.first_node();
	if (unlikely(firstHanle == INVALID_HANDLE)) {
		snprintf(err_message_, sizeof(err_message_),
			 "root tree data not init yet");
		return (-1);
	}

	offset_ = 0;
	size_ = mallocator_->chunk_size(firstHanle);
	p_content_ = Pointer<char>(firstHanle);

	SKIP_TREE_SIZE(sizeof(unsigned char));

	for (int j = key_index_ + 1; j <= p_table_->num_fields(); j++) {
		if (j == expire_id_) {
			expire = *((uint32_t *)(p_content_ + offset_));
			break;
		}

		switch (p_table_->field_type(j)) {
		case DField::Unsigned:
		case DField::Signed:
			if (p_table_->field_size(j) > (int)sizeof(int32_t))
				SKIP_TREE_SIZE(sizeof(int64_t));
			else
				SKIP_TREE_SIZE(sizeof(int32_t));
			;
			break;

		case DField::Float:
			if (p_table_->field_size(j) > (int)sizeof(float))
				SKIP_TREE_SIZE(sizeof(double));
			else
				SKIP_TREE_SIZE(sizeof(float));
			break;

		case DField::String:
		case DField::Binary:
		default:
			uint32_t iLen = 0;
			GET_TREE_VALUE(iLen, int);
			SKIP_TREE_SIZE(iLen);
			break;
		} //end of switch
	}
	return 0;

	offset_ = 0;
	size_ = 0;
	p_content_ = NULL;

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "get expire error");
	return (-100);
}

ALLOC_SIZE_T TreeData::calc_tree_row_size(const RowValue &stRow, int keyIdx)
{
	if (keyIdx == -1)
		log4cplus_error("TreeData may not init yet...");
	ALLOC_SIZE_T tSize = 1; // flag
	for (int j = keyIdx + 1; j <= stRow.num_fields(); j++) //¿½±´Ò»ÐÐÊý¾Ý
	{
		if (stRow.table_definition()->is_discard(j))
			continue;
		switch (stRow.field_type(j)) {
		case DField::Signed:
		case DField::Unsigned:
			tSize += unlikely(stRow.field_size(j) >
					  (int)sizeof(int32_t)) ?
					 sizeof(int64_t) :
					 sizeof(int32_t);
			break;

		case DField::Float: //¸¡µãÊý
			tSize += likely(stRow.field_size(j) >
					(int)sizeof(float)) ?
					 sizeof(double) :
					 sizeof(float);
			break;

		case DField::String: //×Ö·û´®
		case DField::Binary: //¶þ½øÖÆÊý¾Ý
		default: {
			tSize += sizeof(int);
			tSize += stRow.field_value(j)->bin.len;
			break;
		}
		} //end of switch
	}
	if (tSize < 2)
		log4cplus_info("key_index_:%d, stRow.num_fields():%d tSize:%d",
			       keyIdx, stRow.num_fields(), tSize);

	return (tSize);
}

int TreeData::destroy_sub_tree()
{
	t_tree_.destory();
	p_tree_root_->row_count_ = 0;
	p_tree_root_->root_handle_ = INVALID_HANDLE;
	p_tree_root_->tree_size_ = 0;
	p_tree_root_->total_raw_size_ = 0;
	p_tree_root_->node_count_ = 0;
	return 0;
}

unsigned int TreeData::get_row_count()
{
	return *(uint32_t *)(p_content_ + sizeof(unsigned char) +
			     sizeof(uint32_t));
}

unsigned int TreeData::get_data_size()
{
	return *(uint32_t *)(p_content_ + sizeof(unsigned char));
}

int TreeData::set_row_count(unsigned int count)
{
	SET_TREE_VALUE_AT_OFFSET(count, uint32_t,
				 sizeof(unsigned char) + sizeof(uint32_t));

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "set data rowcount error");
	return (-100);
}

int TreeData::set_data_size(unsigned int data_size)
{
	SET_TREE_VALUE_AT_OFFSET(data_size, uint32_t, sizeof(unsigned char));

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "set data size error");
	return (-100);
}

int TreeData::set_cur_row_flag(unsigned char uchFlag)
{
	if (row_offset_ >= get_data_size()) {
		snprintf(err_message_, sizeof(err_message_), "no more rows");
		return (-1);
	}
	*(unsigned char *)(p_content_ + row_offset_) = uchFlag;

	return (0);
}
