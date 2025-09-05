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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "raw_data.h"
#include "global.h"
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

#define GET_VALUE(x, t)                                                        \
	do {                                                                   \
		if (unlikely(offset_ + sizeof(t) > size_))                     \
			goto ERROR_RET;                                        \
		x = (typeof(x)) * (t *)(p_content_ + offset_);                 \
		offset_ += sizeof(t);                                          \
	} while (0)

#define GET_VALUE_AT_OFFSET(x, t, offset)                                      \
	do {                                                                   \
		if (unlikely(offset + sizeof(t) > size_))                      \
			goto ERROR_RET;                                        \
		x = (typeof(x)) * (t *)(p_content_ + offset);                  \
	} while (0)

#define SET_VALUE(x, t)                                                        \
	do {                                                                   \
		if (unlikely(offset_ + sizeof(t) > size_))                     \
			goto ERROR_RET;                                        \
		*(t *)(p_content_ + offset_) = x;                              \
		offset_ += sizeof(t);                                          \
	} while (0)

#define SET_VALUE_AT_OFFSET(x, t, offset)                                      \
	do {                                                                   \
		if (unlikely(offset + sizeof(t) > size_))                      \
			goto ERROR_RET;                                        \
		*(t *)(p_content_ + offset) = x;                               \
	} while (0)

#define SET_BIN_VALUE(p, len)                                                  \
	do {                                                                   \
		if (unlikely(offset_ + sizeof(int) + len > size_))             \
			goto ERROR_RET;                                        \
		*(int *)(p_content_ + offset_) = len;                          \
		offset_ += sizeof(int);                                        \
		if (likely(len != 0))                                          \
			memcpy(p_content_ + offset_, p, len);                  \
		offset_ += len;                                                \
	} while (0)

#define CHECK_SIZE(s)                                                          \
	do {                                                                   \
		if (unlikely(offset_ + s > size_))                             \
			goto ERROR_RET;                                        \
	} while (0)

#define SKIP_SIZE(s)                                                           \
	do {                                                                   \
		if (unlikely(offset_ + s > size_))                             \
			goto ERROR_RET;                                        \
		offset_ += s;                                                  \
	} while (0)
const int BTYE_MAX_VALUE = 255;
RawData::RawData(MallocBase *pstMalloc, int iAutoDestroy)
{
	data_size_ = 0;
	row_count_ = 0;
	key_size_ = 0;
	m_iLAId = -1;
	expire_id_ = -1;
	table_index_ = -1;
	key_start_ = 0;
	data_start_ = 0;
	offset_ = 0;
	m_uiLAOffset = 0;
	row_offset_ = 0;
	get_request_count_offset_ = 0;
	time_stamp_offset_ = 0;
	get_request_count_ = 0;
	create_time_ = 0;
	latest_request_time_ = 0;
	latest_update_time_ = 0;
	key_index_ = -1;
	p_content_ = NULL;
	need_new_bufer_size = 0;
	mallocator_ = pstMalloc;
	handle_ = INVALID_HANDLE;
	auto_destory_ = iAutoDestroy;
	size_ = 0;
	p_reference_ = NULL;
	memset(err_message_, 0, sizeof(err_message_));
}

RawData::~RawData()
{
	if (auto_destory_) {
		destory();
	}
	handle_ = INVALID_HANDLE;
	size_ = 0;
}

int RawData::init(uint8_t uchKeyIdx, int iKeySize, const char *pchKey,
		  ALLOC_SIZE_T uiDataSize, int laId, int expireId, int nodeIdx)
{
	int ks = iKeySize != 0 ? iKeySize : 1 + *(unsigned char *)pchKey;

	/*|1 byte:type|4 bytes:data size|4 bytes: row count| 1 byte : Get count| 2 bytes: last access time| 2 bytes : last update time|2 bytes: last creation time |key|*/
	uiDataSize += 2 + sizeof(uint32_t) * 2 + sizeof(uint16_t) * 3 + ks;

	handle_ = INVALID_HANDLE;
	size_ = 0;

	handle_ = mallocator_->Malloc(uiDataSize);
	if (handle_ == INVALID_HANDLE) {
		snprintf(err_message_, sizeof(err_message_), "malloc error");
		need_new_bufer_size = uiDataSize;
		return (EC_NO_MEM);
	}
	size_ = mallocator_->chunk_size(handle_);

	data_size_ = 2 + sizeof(uint32_t) * 2 + sizeof(uint16_t) * 3 + ks;
	row_count_ = 0;
	key_index_ = uchKeyIdx;
	key_size_ = iKeySize;
	m_iLAId = laId;
	expire_id_ = expireId;

	p_content_ = Pointer<char>();
	offset_ = 0;
	m_uiLAOffset = 0;
	if (nodeIdx != -1) {
		table_index_ = nodeIdx;
	}
	if (table_index_ != 0 && table_index_ != 1) {
		snprintf(err_message_, sizeof(err_message_), "node idx error");
		return -100;
	}
	SET_VALUE(((table_index_ << 7) & 0x80) + DATA_TYPE_RAW, unsigned char);
	SET_VALUE(data_size_, uint32_t);
	SET_VALUE(row_count_, uint32_t);

	get_request_count_offset_ = offset_;
	get_request_count_ = 1;
	SET_VALUE(get_request_count_, uint8_t);
	time_stamp_offset_ = offset_;
	init_timp_stamp();
	SKIP_SIZE(3 * sizeof(uint16_t));
	key_start_ = offset_;
	if (iKeySize != 0) {
		memcpy(p_content_ + offset_, pchKey, iKeySize);
		offset_ += iKeySize;
	} else {
		memcpy(p_content_ + offset_, pchKey, ks);
		offset_ += ks;
	}
	data_start_ = offset_;
	row_offset_ = data_start_;

	return (0);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "set value error");
	return (-100);
}

int RawData::do_init(const char *pchKey, ALLOC_SIZE_T uiDataSize)
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
	table_definition_ =
		TableDefinitionManager::instance()->get_table_def_by_idx(
			table_index_);
	if (table_definition_ == NULL) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach error, tabledef[NULL]");
		return -1;
	}

	return init(table_definition_->key_fields() - 1,
		    table_definition_->key_format(), pchKey, uiDataSize,
		    table_definition_->lastacc_field_id(),
		    table_definition_->expire_time_field_id());
}

int RawData::do_attach(MEM_HANDLE_T hHandle)
{
	handle_ = hHandle;
	char *p = Pointer<char>();
	table_index_ = (*p >> 7) & 0x01;
	if (table_index_ != 0 && table_index_ != 1) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach error, nodeIdx[%d] error", table_index_);
		return -1;
	}
	table_definition_ =
		TableDefinitionManager::instance()->get_table_def_by_idx(
			table_index_);
	if (table_definition_ == NULL) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach error, tabledef[NULL]");
		return -1;
	}
	return do_attach(hHandle, table_definition_->key_fields() - 1,
			 table_definition_->key_format(),
			 table_definition_->lastacc_field_id(),
			 table_definition_->lastcmod_field_id(),
			 table_definition_->expire_time_field_id());
}

/* this function belive that inputted raw data is formatted correclty, but it's not the case sometimes */
int RawData::do_attach(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
		       int laid, int lcmodid, int expireid)
{
	int ks = 0;

	size_ = mallocator_->chunk_size(hHandle);
	if (unlikely(size_ == 0)) {
		snprintf(err_message_, sizeof(err_message_), "attach error: %s",
			 mallocator_->get_err_msg());
		return (-1);
	}
	handle_ = hHandle;

	p_content_ = Pointer<char>();
	offset_ = 0;
	m_uiLAOffset = 0;
	unsigned char uchType;
	GET_VALUE(uchType, unsigned char);
	if (unlikely((uchType & 0x7f) != DATA_TYPE_RAW)) {
		snprintf(err_message_, sizeof(err_message_),
			 "invalid data type: %u", uchType);
		return (-2);
	}

	GET_VALUE(data_size_, uint32_t);
	GET_VALUE(row_count_, uint32_t);
	get_request_count_offset_ = offset_;
	GET_VALUE(get_request_count_, uint8_t);
	time_stamp_offset_ = offset_;
	attach_time_stamp();
	SKIP_SIZE(3 * sizeof(uint16_t));
	if (unlikely(data_size_ > size_)) {
		snprintf(
			err_message_, sizeof(err_message_),
			"raw-data handle[" UINT64FMT
			"] data size[%u] error, large than chunk size[" UINT64FMT
			"]",
			hHandle, data_size_, size_);
		return (-3);
	}

	key_index_ = uchKeyIdx;
	key_start_ = offset_;
	key_size_ = iKeySize;
	m_iLAId = laid;
	m_iLCmodId = lcmodid;
	expire_id_ = expireid;

	ks = iKeySize != 0 ? iKeySize :
			     1 + *(unsigned char *)(p_content_ + key_start_);
	SKIP_SIZE(ks);
	data_start_ = offset_;
	row_offset_ = data_start_;

	return (0);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "get value error");
	return (-100);
}

int RawData::destory()
{
	if (handle_ == INVALID_HANDLE) {
		size_ = 0;
		return 0;
	}

	int iRet = mallocator_->Free(handle_);
	handle_ = INVALID_HANDLE;
	size_ = 0;
	return (iRet);
}

int RawData::check_size(MEM_HANDLE_T hHandle, uint8_t uchKeyIdx, int iKeySize,
			int size)
{
	size_ = mallocator_->chunk_size(hHandle);
	if (unlikely(size_ == 0)) {
		snprintf(err_message_, sizeof(err_message_), "attach error: %s",
			 mallocator_->get_err_msg());
		return (-1);
	}
	handle_ = hHandle;

	p_content_ = Pointer<char>();
	offset_ = 0;
	m_uiLAOffset = 0;
	unsigned char uchType;
	GET_VALUE(uchType, unsigned char);
	if (unlikely(uchType != DATA_TYPE_RAW)) {
		snprintf(err_message_, sizeof(err_message_),
			 "invalid data type: %u", uchType);
		return (-2);
	}

	GET_VALUE(data_size_, uint32_t);
	if (data_size_ != (unsigned int)size) {
		snprintf(err_message_, sizeof(err_message_),
			 "invalid data type: %u", uchType);
		return -1;
	}

	return 0;
ERROR_RET:
	return -1;
}

int RawData::strip_mem()
{
	ALLOC_HANDLE_T hTmp = mallocator_->ReAlloc(handle_, data_size_);
	if (hTmp == INVALID_HANDLE) {
		snprintf(err_message_, sizeof(err_message_), "realloc error");
		need_new_bufer_size = data_size_;
		return (EC_NO_MEM);
	}
	handle_ = hTmp;
	size_ = mallocator_->chunk_size(handle_);
	p_content_ = Pointer<char>();

	return (0);
}

int RawData::decode_row(RowValue &stRow, unsigned char &uchRowFlags,
			int iDecodeFlag)
{
	if (unlikely(handle_ == INVALID_HANDLE || p_content_ == NULL)) {
		snprintf(err_message_, sizeof(err_message_),
			 "rawdata not init yet");
		return (-1);
	}

	ALLOC_SIZE_T uiOldOffset = offset_;
	ALLOC_SIZE_T uiOldRowOffset = row_offset_;
	m_uiLAOffset = 0;
	row_offset_ = offset_;
	GET_VALUE(uchRowFlags, unsigned char);

	for (int j = key_index_ + 1; j <= stRow.num_fields();
	     j++) //copy row data
	{
		if (stRow.table_definition()->is_discard(j))
			continue;
		if (j == m_iLAId)
			m_uiLAOffset = offset_;
		switch (stRow.field_type(j)) {
		case DField::Signed:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(int32_t))) {
				GET_VALUE(stRow.field_value(j)->s64, int64_t);
			} else {
				GET_VALUE(stRow.field_value(j)->s64, int32_t);
			}
			break;

		case DField::Unsigned:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(uint32_t))) {
				GET_VALUE(stRow.field_value(j)->u64, uint64_t);
			} else {
				GET_VALUE(stRow.field_value(j)->u64, uint32_t);
			}
			break;

		case DField::Float: //float type
			if (likely(stRow.field_size(j) > (int)sizeof(float))) {
				GET_VALUE(stRow.field_value(j)->flt, double);
			} else {
				GET_VALUE(stRow.field_value(j)->flt, float);
			}
			break;

		case DField::String: //string type
		case DField::Binary: //binary data
		default: {
			GET_VALUE(stRow.field_value(j)->bin.len, int);
			stRow.field_value(j)->bin.ptr = p_content_ + offset_;
			SKIP_SIZE(stRow.field_value(j)->bin.len);
			break;
		}
		} //end of switch
	}

	if (unlikely(iDecodeFlag & PRE_DECODE_ROW)) {
		offset_ = uiOldOffset;
		row_offset_ = uiOldRowOffset;
	}

	return (0);

ERROR_RET:
	if (unlikely(iDecodeFlag & PRE_DECODE_ROW)) {
		offset_ = uiOldOffset;
		row_offset_ = uiOldRowOffset;
	}
	snprintf(err_message_, sizeof(err_message_), "get value error");
	return (-100);
}

int RawData::get_expire_time(DTCTableDefinition *t, uint32_t &expire)
{
	expire = 0;
	if (unlikely(handle_ == INVALID_HANDLE || p_content_ == NULL)) {
		snprintf(err_message_, sizeof(err_message_),
			 "rawdata not init yet");
		return (-1);
	}
	if (expire_id_ == -1) {
		expire = 0;
		return 0;
	}
	SKIP_SIZE(sizeof(unsigned char)); //skip flag
	// the first field should be expire time
	for (int j = key_index_ + 1; j <= table_definition_->num_fields();
	     j++) { //copy row data
		if (j == expire_id_) {
			expire = *((uint32_t *)(p_content_ + offset_));
			break;
		}

		switch (table_definition_->field_type(j)) {
		case DField::Unsigned:
		case DField::Signed:
			if (table_definition_->field_size(j) >
			    (int)sizeof(int32_t))
				SKIP_SIZE(sizeof(int64_t));
			else
				SKIP_SIZE(sizeof(int32_t));
			;
			break;

		case DField::Float: //float type
			if (table_definition_->field_size(j) >
			    (int)sizeof(float))
				SKIP_SIZE(sizeof(double));
			else
				SKIP_SIZE(sizeof(float));
			break;

		case DField::String: //string type
		case DField::Binary: //binary data
		default:
			int iLen = 0;
			GET_VALUE(iLen, int);
			SKIP_SIZE(iLen);
			break;
		} //end of switch
	}
	return 0;

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "get expire error");
	return (-100);
}

int RawData::get_lastcmod(uint32_t &lastcmod)
{
	lastcmod = 0;
	if (unlikely(handle_ == INVALID_HANDLE || p_content_ == NULL)) {
		snprintf(err_message_, sizeof(err_message_),
			 "rawdata not init yet");
		return (-1);
	}

	row_offset_ = offset_;
	SKIP_SIZE(sizeof(unsigned char)); //skip flag

	for (int j = key_index_ + 1; j <= table_definition_->num_fields();
	     j++) //copy row data
	{
		//id: bug fix skip discard
		if (table_definition_->is_discard(j))
			continue;
		if (j == m_iLCmodId)
			lastcmod = *((uint32_t *)(p_content_ + offset_));

		switch (table_definition_->field_type(j)) {
		case DField::Unsigned:
		case DField::Signed:
			if (table_definition_->field_size(j) >
			    (int)sizeof(int32_t))
				SKIP_SIZE(sizeof(int64_t));
			else
				SKIP_SIZE(sizeof(int32_t));
			;
			break;

		case DField::Float: //float type
			if (table_definition_->field_size(j) >
			    (int)sizeof(float))
				SKIP_SIZE(sizeof(double));
			else
				SKIP_SIZE(sizeof(float));
			break;

		case DField::String: //string type
		case DField::Binary: //binary data
		default: {
			int iLen = 0;
			GET_VALUE(iLen, int);
			SKIP_SIZE(iLen);
			break;
		}
		} //end of switch
	}
	return (0);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "get timecmod error");
	return (-100);
}

int RawData::set_data_size()
{
	SET_VALUE_AT_OFFSET(data_size_, uint32_t, 1);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "set data size error");
	return (-100);
}

int RawData::set_row_count()
{
	SET_VALUE_AT_OFFSET(row_count_, uint32_t, 5);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "set row count error");
	return (-100);
}

int RawData::expand_chunk(ALLOC_SIZE_T expand_size)
{
	if (handle_ == INVALID_HANDLE) {
		snprintf(err_message_, sizeof(err_message_),
			 "data not init yet");
		return (-1);
	}

	if (data_size_ + expand_size > size_) {
		ALLOC_HANDLE_T hTmp =
			mallocator_->ReAlloc(handle_, data_size_ + expand_size);
		if (hTmp == INVALID_HANDLE) {
			snprintf(err_message_, sizeof(err_message_),
				 "realloc error[%s]",
				 mallocator_->get_err_msg());
			need_new_bufer_size = data_size_ + expand_size;
			return (EC_NO_MEM);
		}
		handle_ = hTmp;
		size_ = mallocator_->chunk_size(handle_);
		p_content_ = Pointer<char>();
	}

	return (0);
}

int RawData::re_alloc_chunk(ALLOC_SIZE_T tSize)
{
	if (tSize > size_) {
		ALLOC_HANDLE_T hTmp = mallocator_->ReAlloc(handle_, tSize);
		if (hTmp == INVALID_HANDLE) {
			snprintf(err_message_, sizeof(err_message_),
				 "realloc error");
			need_new_bufer_size = tSize;
			return (EC_NO_MEM);
		}
		handle_ = hTmp;
		size_ = mallocator_->chunk_size(handle_);
		p_content_ = Pointer<char>();
	}

	return (0);
}

ALLOC_SIZE_T RawData::calc_row_size(const RowValue &stRow, int keyIdx)
{
	if (keyIdx == -1)
		log4cplus_error("RawData may not init yet...");
	ALLOC_SIZE_T tSize = 1; // flag
	for (int j = keyIdx + 1; j <= stRow.num_fields(); j++) //copy row data
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

		case DField::Float: //float type
			tSize += likely(stRow.field_size(j) >
					(int)sizeof(float)) ?
					 sizeof(double) :
					 sizeof(float);
			break;

		case DField::String: //string type
		case DField::Binary: //binary data
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

int RawData::encode_row(const RowValue &stRow, unsigned char uchOp,
			bool expendBuf)
{
	int iRet;

	ALLOC_SIZE_T tSize;
	tSize = calc_row_size(stRow, key_index_);

	if (unlikely(expendBuf)) {
		iRet = expand_chunk(tSize);
		if (unlikely(iRet != 0))
			return (iRet);
	}

	SET_VALUE(uchOp, unsigned char);

	for (int j = key_index_ + 1; j <= stRow.num_fields();
	     j++) //copy row data
	{
		if (stRow.table_definition()->is_discard(j))
			continue;
		const DTCValue *const v = stRow.field_value(j);
		switch (stRow.field_type(j)) {
		case DField::Signed:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(int32_t)))
				SET_VALUE(v->s64, int64_t);
			else
				SET_VALUE(v->s64, int32_t);
			break;

		case DField::Unsigned:
			if (unlikely(stRow.field_size(j) >
				     (int)sizeof(uint32_t)))
				SET_VALUE(v->u64, uint64_t);
			else
				SET_VALUE(v->u64, uint32_t);
			break;

		case DField::Float: //float type
			if (likely(stRow.field_size(j) > (int)sizeof(float)))
				SET_VALUE(v->flt, double);
			else
				SET_VALUE(v->flt, float);
			break;

		case DField::String: //string type
		case DField::Binary: //binary data
		default: {
			SET_BIN_VALUE(v->bin.ptr, v->bin.len);
			break;
		}
		} //end of switch
	}

	data_size_ += tSize;
	set_data_size();
	row_count_++;
	set_row_count();

	return 0;

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "encode row error");
	return (-100);
}

int RawData::insert_row_flag(const RowValue &stRow, bool byFirst,
			     unsigned char uchOp)
{
	uint32_t uiOldSize = data_size_;

	offset_ = data_size_;
	int iRet = encode_row(stRow, uchOp);
	uint32_t uiNewRowSize = data_size_ - uiOldSize;
	if (iRet == 0 && byFirst == true && uiNewRowSize > 0 &&
	    (uiOldSize - data_start_) > 0) {
		void *pBuf = MALLOC(uiNewRowSize);
		if (pBuf == NULL) {
			snprintf(err_message_, sizeof(err_message_),
				 "malloc error: %m");
			return (-ENOMEM);
		}
		char *pchDataStart = p_content_ + data_start_;
		// save last row
		memmove(pBuf, p_content_ + uiOldSize, uiNewRowSize);
		// move buf up sz bytes
		memmove(pchDataStart + uiNewRowSize, pchDataStart,
			uiOldSize - data_start_);
		// last row as first row
		memcpy(pchDataStart, pBuf, uiNewRowSize);
		FREE(pBuf);
	}

	return (iRet);
}

int RawData::insert_row(const RowValue &stRow, bool byFirst, bool isDirty)
{
	return insert_row_flag(stRow, byFirst,
			       isDirty ? OPER_INSERT : OPER_SELECT);
}

int RawData::insert_n_rows(unsigned int uiNRows, const RowValue *pstRow,
			   bool byFirst, bool isDirty)
{
	int iRet;
	unsigned int i;
	ALLOC_SIZE_T tSize;

	tSize = 0;
	for (i = 0; i < uiNRows; i++)
		tSize += calc_row_size(pstRow[i], key_index_);

	iRet = expand_chunk(tSize); // expand buffer first to avoid rollback if insert fails later
	if (iRet != 0)
		return (iRet);

	uint32_t uiOldSize = data_size_;
	offset_ = data_size_;
	for (i = 0; i < uiNRows; i++) {
		iRet = encode_row(pstRow[i],
				  isDirty ? OPER_INSERT : OPER_SELECT);
		if (iRet != 0) {
			return (iRet);
		}
	}

	uint32_t uiNewRowSize = data_size_ - uiOldSize;
	if (byFirst == true && uiNewRowSize > 0 &&
	    (uiOldSize - data_start_) > 0) {
		void *pBuf = MALLOC(uiNewRowSize);
		if (pBuf == NULL) {
			snprintf(err_message_, sizeof(err_message_),
				 "malloc error: %m");
			return (-ENOMEM);
		}
		char *pchDataStart = p_content_ + data_start_;
		// save last row
		memmove(pBuf, p_content_ + uiOldSize, uiNewRowSize);
		// move buf up sz bytes
		memmove(pchDataStart + uiNewRowSize, pchDataStart,
			uiOldSize - data_start_);
		// last row as first row
		memcpy(pchDataStart, pBuf, uiNewRowSize);
		FREE(pBuf);
	}

	return (0);
}

int RawData::skip_row(const RowValue &stRow)
{
	if (handle_ == INVALID_HANDLE || p_content_ == NULL) {
		snprintf(err_message_, sizeof(err_message_),
			 "rawdata not init yet");
		return (-1);
	}

	offset_ = row_offset_;
	if (offset_ >= data_size_) {
		snprintf(err_message_, sizeof(err_message_),
			 "already at end of data");
		return (-2);
	}

	SKIP_SIZE(sizeof(unsigned char)); // flag

	for (int j = key_index_ + 1; j <= stRow.num_fields();
	     j++) //copy row data
	{
		//id: bug fix skip discard
		if (stRow.table_definition()->is_discard(j))
			continue;

		switch (stRow.field_type(j)) {
		case DField::Unsigned:
		case DField::Signed:
			if (stRow.field_size(j) > (int)sizeof(int32_t))
				SKIP_SIZE(sizeof(int64_t));
			else
				SKIP_SIZE(sizeof(int32_t));
			;
			break;

		case DField::Float: //float type
			if (stRow.field_size(j) > (int)sizeof(float))
				SKIP_SIZE(sizeof(double));
			else
				SKIP_SIZE(sizeof(float));
			break;

		case DField::String: //string type
		case DField::Binary: //binary data
		default: {
			int iLen;
			GET_VALUE(iLen, int);
			SKIP_SIZE(iLen);
			break;
		}
		} //end of switch
	}

	return (0);

ERROR_RET:
	snprintf(err_message_, sizeof(err_message_), "skip row error");
	return (-100);
}

int RawData::replace_cur_row(const RowValue &stRow, bool isDirty)
{
	int iRet = 0;
	ALLOC_SIZE_T uiOldOffset;
	ALLOC_SIZE_T uiNewRowSize;
	ALLOC_SIZE_T uiCurRowSize;
	ALLOC_SIZE_T uiNextRowsOffset;
	ALLOC_SIZE_T uiNextRowsSize;

	uiOldOffset = offset_;
	if ((iRet = skip_row(stRow)) != 0) {
		goto ERROR_RET;
	}

	unsigned char uchRowFlag;
	GET_VALUE_AT_OFFSET(uchRowFlag, unsigned char, row_offset_);
	if (isDirty)
		uchRowFlag = OPER_UPDATE;

	uiNewRowSize = calc_row_size(stRow, key_index_);
	uiCurRowSize = offset_ - row_offset_;
	uiNextRowsOffset = offset_;
	uiNextRowsSize = data_size_ - offset_;

	if (uiNewRowSize > uiCurRowSize) {
		// enlarge buffer
		MEM_HANDLE_T hTmp = mallocator_->ReAlloc(
			handle_, data_size_ + uiNewRowSize - uiCurRowSize);
		if (hTmp == INVALID_HANDLE) {
			snprintf(err_message_, sizeof(err_message_),
				 "realloc error");
			need_new_bufer_size =
				data_size_ + uiNewRowSize - uiCurRowSize;
			iRet = EC_NO_MEM;
			goto ERROR_RET;
		}
		handle_ = hTmp;
		size_ = mallocator_->chunk_size(handle_);
		p_content_ = Pointer<char>();

		// move data
		if (uiNextRowsSize > 0)
			memmove(p_content_ + uiNextRowsOffset +
					(uiNewRowSize - uiCurRowSize),
				p_content_ + uiNextRowsOffset, uiNextRowsSize);

		// copy new row
		offset_ = row_offset_;
		iRet = encode_row(stRow, uchRowFlag, false);
		if (iRet != 0) {
			if (uiNextRowsSize > 0)
				memmove(p_content_ + uiNextRowsOffset,
					p_content_ + uiNextRowsOffset +
						(uiNewRowSize - uiCurRowSize),
					uiNextRowsSize);
			iRet = -1;
			goto ERROR_RET;
		}

		row_count_--;
		data_size_ -= uiCurRowSize;
	} else {
		// back up old row
		void *pTmpBuf = MALLOC(uiCurRowSize);
		if (pTmpBuf == NULL) {
			snprintf(err_message_, sizeof(err_message_),
				 "malloc error: %m");
			return (-ENOMEM);
		}
		memmove(pTmpBuf, p_content_ + row_offset_, uiCurRowSize);

		// copy new row
		offset_ = row_offset_;
		iRet = encode_row(stRow, uchRowFlag, false);
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
			handle_, data_size_ + uiNewRowSize - uiCurRowSize);
		if (hTmp != INVALID_HANDLE) {
			handle_ = hTmp;
			size_ = mallocator_->chunk_size(handle_);
			p_content_ = Pointer<char>();
		}

		row_count_--;
		data_size_ -= uiCurRowSize;
	}

	set_data_size();
	set_row_count();

	return (0);

ERROR_RET:
	offset_ = uiOldOffset;
	return (iRet);
}

int RawData::delete_cur_row(const RowValue &stRow)
{
	int iRet = 0;
	ALLOC_SIZE_T uiOldOffset;
	ALLOC_SIZE_T uiNextRowsSize;

	uiOldOffset = offset_;
	if ((iRet = skip_row(stRow)) != 0) {
		goto ERROR_RET;
	}
	uiNextRowsSize = data_size_ - offset_;

	memmove(p_content_ + row_offset_, p_content_ + offset_, uiNextRowsSize);
	data_size_ -= (offset_ - row_offset_);
	row_count_--;
	set_data_size();
	set_row_count();

	offset_ = row_offset_;
	return (iRet);

ERROR_RET:
	offset_ = uiOldOffset;
	return (iRet);
}

int RawData::delete_all_rows()
{
	data_size_ = data_start_;
	row_offset_ = data_start_;
	row_count_ = 0;
	offset_ = data_size_;

	set_data_size();
	set_row_count();

	need_new_bufer_size = 0;

	return (0);
}

int RawData::set_cur_row_flag(unsigned char uchFlag)
{
	if (row_offset_ >= data_size_) {
		snprintf(err_message_, sizeof(err_message_), "no more rows");
		return (-1);
	}
	*(unsigned char *)(p_content_ + row_offset_) = uchFlag;

	return (0);
}

int RawData::copy_row()
{
	int iRet;
	ALLOC_SIZE_T uiSize = p_reference_->offset_ - p_reference_->row_offset_;
	if ((iRet = expand_chunk(uiSize)) != 0)
		return (iRet);

	memcpy(p_content_ + offset_,
	       p_reference_->p_content_ + p_reference_->row_offset_, uiSize);
	offset_ += uiSize;
	data_size_ += uiSize;
	row_count_++;

	set_data_size();
	set_row_count();

	return (0);
}

int RawData::copy_all()
{
	int iRet;
	ALLOC_SIZE_T uiSize = p_reference_->data_size_;
	if ((iRet = re_alloc_chunk(uiSize)) != 0)
		return (iRet);

	memcpy(p_content_, p_reference_->p_content_, uiSize);

	if ((iRet = do_attach(handle_)) != 0)
		return (iRet);

	return (0);
}

int RawData::append_n_records(unsigned int uiNRows, const char *pchData,
			      const unsigned int uiLen)
{
	int iRet;

	iRet = expand_chunk(uiLen);
	if (iRet != 0)
		return (iRet);

	memcpy(p_content_ + data_size_, pchData, uiLen);
	data_size_ += uiLen;
	row_count_ += uiNRows;

	set_data_size();
	set_row_count();

	return (0);
}

void RawData::init_timp_stamp()
{
	if (unlikely(NULL == p_content_)) {
		return;
	}

	if (unlikely(offset_ + 3 * sizeof(uint16_t) > size_)) {
		return;
	}
	uint16_t dwCurHour = RELATIVE_HOUR_CALCULATOR->get_relative_hour();

	latest_request_time_ = dwCurHour;
	latest_update_time_ = dwCurHour;
	create_time_ = dwCurHour;

	*(uint16_t *)(p_content_ + time_stamp_offset_) = dwCurHour;
	*(uint16_t *)(p_content_ + time_stamp_offset_ + sizeof(uint16_t)) =
		dwCurHour;
	*(uint16_t *)(p_content_ + time_stamp_offset_ + 2 * sizeof(uint16_t)) =
		dwCurHour;
}

void RawData::attach_time_stamp()
{
	if (unlikely(NULL == p_content_)) {
		return;
	}
	if (unlikely(time_stamp_offset_ + 3 * sizeof(uint16_t) > size_)) {
		return;
	}
	latest_request_time_ = *(uint16_t *)(p_content_ + time_stamp_offset_);
	latest_update_time_ = *(uint16_t *)(p_content_ + time_stamp_offset_ +
					    sizeof(uint16_t));
	create_time_ = *(uint16_t *)(p_content_ + time_stamp_offset_ +
				     2 * sizeof(uint16_t));
}
void RawData::update_last_access_time_by_hour()
{
	if (unlikely(NULL == p_content_)) {
		return;
	}
	if (unlikely(time_stamp_offset_ + sizeof(uint16_t) > size_)) {
		return;
	}
	latest_request_time_ = RELATIVE_HOUR_CALCULATOR->get_relative_hour();
	*(uint16_t *)(p_content_ + time_stamp_offset_) = latest_request_time_;
}
void RawData::update_last_update_time_by_hour()
{
	if (unlikely(NULL == p_content_)) {
		return;
	}
	if (unlikely(time_stamp_offset_ + 2 * sizeof(uint16_t) > size_)) {
		return;
	}
	latest_update_time_ = RELATIVE_HOUR_CALCULATOR->get_relative_hour();
	*(uint16_t *)(p_content_ + time_stamp_offset_ + sizeof(uint16_t)) =
		latest_update_time_;
}
uint32_t RawData::get_create_time_by_hour()
{
	return create_time_;
}
uint32_t RawData::get_last_access_time_by_hour()
{
	return latest_request_time_;
}

uint32_t RawData::get_last_update_time_by_hour()
{
	return latest_update_time_;
}
uint32_t RawData::get_select_op_count()
{
	return get_request_count_;
}

void RawData::inc_select_count()
{
	if (unlikely(get_request_count_ >= BTYE_MAX_VALUE)) {
		return;
	}
	if (unlikely(get_request_count_offset_ + sizeof(uint8_t) > size_)) {
		return;
	}
	get_request_count_++;
	*(uint8_t *)(p_content_ + get_request_count_offset_) =
		get_request_count_;
}

DTCTableDefinition *RawData::get_node_table_def()
{
	return table_definition_;
}
