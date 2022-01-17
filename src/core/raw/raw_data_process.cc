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

#include "raw_data_process.h"
#include "global.h"
#include "log/log.h"
#include "sys_malloc.h"
#include "task/task_pkey.h"
#include "buffer_flush.h"
#include "algorithm/relative_hour_calculator.h"

DTC_USING_NAMESPACE

RawDataProcess::RawDataProcess(MallocBase *pstMalloc,
			       DTCTableDefinition *p_table_definition_,
			       BufferPond *pstPool,
			       const UpdateMode *pstUpdateMode)
	: raw_data_(pstMalloc), p_table_(p_table_definition_),
	  p_mallocator_(pstMalloc), p_buffer_pond_(pstPool)
{
	memcpy(&update_mode_, pstUpdateMode, sizeof(update_mode_));
	nodeSizeLimit = 0;
	history_datasize = g_stat_mgr.get_sample(DATA_SIZE_HISTORY_STAT);
	history_rowsize = g_stat_mgr.get_sample(ROW_SIZE_HISTORY_STAT);
}

RawDataProcess::~RawDataProcess()
{
}

int RawDataProcess::init_data(Node *p_node, RawData *affected_data,
			      const char *ptrKey)
{
	int iRet;

	iRet = raw_data_.do_init(ptrKey, 0);
	if (iRet != 0) {
		log4cplus_error("raw-data init error: %d,%s", iRet,
				raw_data_.get_err_msg());
		return (-1);
	}
	p_node->vd_handle() = raw_data_.get_handle();

	if (affected_data != NULL) {
		iRet = affected_data->do_init(ptrKey, 0);
		if (iRet != 0) {
			log4cplus_error("raw-data init error: %d,%s", iRet,
					affected_data->get_err_msg());
			return (-2);
		}
	}

	return DTC_CODE_SUCCESS;
}

int RawDataProcess::attach_data(Node *p_node, RawData *affected_data)
{
	int iRet;

	iRet = raw_data_.do_attach(p_node->vd_handle());
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("raw-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				raw_data_.get_err_msg());
		return (-1);
	}

	if (affected_data != NULL) {
		iRet = affected_data->do_init(raw_data_.key(), 0);
		if (iRet != DTC_CODE_SUCCESS) {
			log4cplus_error("raw-data init error: %d,%s", iRet,
					affected_data->get_err_msg());
			return (-2);
		}
	}

	return DTC_CODE_SUCCESS;
}

int RawDataProcess::get_node_all_rows_count(Node *p_node, RawData *pstRows)
{
	int iRet;

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = attach_data(p_node, pstRows);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("attach data error: %d", iRet);
		return (-1);
	}

	pstRows->set_refrence(&raw_data_);
	if (pstRows->copy_all() != 0) {
		log4cplus_error("copy data error: %d,%s", iRet,
				pstRows->get_err_msg());
		return (-2);
	}

	return DTC_CODE_SUCCESS;
}

int RawDataProcess::expand_node(DTCJobOperation &job_op, Node *p_node)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	// no need to check expand status as checked in CCacheProces

	// save node to stack as new version
	iRet = attach_data(p_node, NULL);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("attach data error: %d", iRet);
		return -1;
	}
	unsigned int uiTotalRows = raw_data_.total_rows();
	stpNodeTab = raw_data_.get_node_table_def();
	stpTaskTab = TableDefinitionManager::instance()->get_new_table_def();
	if (stpTaskTab == stpNodeTab) {
		log4cplus_info(
			"expand one node which is already new version, pay attention, treat as success");
		return DTC_CODE_SUCCESS;
	}
	RowValue stNewRow(stpTaskTab);
	RowValue stNewNodeRow(stpNodeTab);
	stpTaskRow = &stNewRow;
	stpNodeRow = &stNewNodeRow;
	RawData stNewTmpRawData(&g_stSysMalloc, 1);
	iRet = stNewTmpRawData.do_init(raw_data_.key(), raw_data_.data_size());
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error(
			"init raw-data struct error, ret = %d, err = %s", iRet,
			stNewTmpRawData.get_err_msg());
		return -2;
	}
	for (unsigned int i = 0; i < uiTotalRows; ++i) {
		unsigned char uchRowFlags;
		if (raw_data_.decode_row(*stpNodeRow, uchRowFlags, 0) != 0) {
			log4cplus_error("raw-data decode row error: %d, %s",
					iRet, raw_data_.get_err_msg());
			return -1;
		}
		stpTaskRow->default_value();
		stpTaskRow->Copy(stpNodeRow);
		iRet = stNewTmpRawData.insert_row(
			*stpTaskRow,
			update_mode_.m_uchInsertOrder ? true : false, false);
		if (0 != iRet) {
			log4cplus_error(
				"insert row to raw-data error: ret = %d, err = %s",
				iRet, stNewTmpRawData.get_err_msg());
			return -2;
		}
	}

	// allocate new with new version
	RawData stTmpRawData(p_mallocator_);
	iRet = stTmpRawData.do_init(stNewTmpRawData.key(),
				    stNewTmpRawData.data_size());
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(stTmpRawData.need_size(),
						   *p_node) == 0)
			iRet = stTmpRawData.do_init(
				stNewTmpRawData.key(),
				stNewTmpRawData.data_size() -
					stNewTmpRawData.data_start());
	}

	if (iRet != DTC_CODE_SUCCESS) {
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data init error: %s", stTmpRawData.get_err_msg());
		stTmpRawData.destory();
		return -3;
	}

	stTmpRawData.set_refrence(&stNewTmpRawData);
	iRet = stTmpRawData.copy_all();
	if (iRet != DTC_CODE_SUCCESS) {
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data init error: %s", stTmpRawData.get_err_msg());
		stTmpRawData.destory();
		return -3;
	}

	// purge old
	raw_data_.destory();
	p_node->vd_handle() = stTmpRawData.get_handle();
	return DTC_CODE_SUCCESS;
}

int RawDataProcess::destroy_data(Node *p_node)
{
	int iRet;

	iRet = raw_data_.do_attach(p_node->vd_handle());
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("raw-data attach error: %d,%s", iRet,
				raw_data_.get_err_msg());
		return DTC_CODE_FAILED;
	}
	rows_count_ += 0LL - raw_data_.total_rows();

	raw_data_.destory();
	p_node->vd_handle() = INVALID_HANDLE;

	return DTC_CODE_SUCCESS;
}

int RawDataProcess::do_replace_all(Node *p_node, RawData *new_data)
{
	int iRet;

	log4cplus_debug("do_replace_all start ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	RawData tmpRawData(p_mallocator_);

	iRet = tmpRawData.do_init(new_data->key(),
				  new_data->data_size() -
					  new_data->data_start());
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(tmpRawData.need_size(),
						   *p_node) == 0)
			iRet = tmpRawData.do_init(
				new_data->key(),
				new_data->data_size() - new_data->data_start());
	}

	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data init error: %s", tmpRawData.get_err_msg());
		tmpRawData.destory();
		return (-2);
	}

	tmpRawData.set_refrence(new_data);
	iRet = tmpRawData.copy_all();
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data init error: %s", tmpRawData.get_err_msg());
		tmpRawData.destory();
		return (-3);
	}

	if (p_node->vd_handle() != INVALID_HANDLE)
		destroy_data(p_node);
	p_node->vd_handle() = tmpRawData.get_handle();
	rows_count_ += new_data->total_rows();
	if (tmpRawData.total_rows() > 0) {
		log4cplus_debug(
			"do_replace_all,  stat history datasize, size is %u",
			tmpRawData.data_size());
		history_datasize.push(tmpRawData.data_size());
		history_rowsize.push(tmpRawData.total_rows());
	}
	return DTC_CODE_SUCCESS;
}

int RawDataProcess::get_expire_time(DTCTableDefinition *t, Node *p_node,
				    uint32_t &expire)
{
	int iRet = DTC_CODE_SUCCESS;

	iRet = attach_data(p_node, NULL);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("attach data error: %d", iRet);
		return iRet;
	}
	iRet = raw_data_.get_expire_time(t, expire);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("raw data get expire time error: %d", iRet);
		return iRet;
	}
	return DTC_CODE_SUCCESS;
}

void RawDataProcess::change_mallocator(MallocBase *pstMalloc)
{
	log4cplus_debug("oring mallc: %p, new mallc: %p", p_mallocator_,
			pstMalloc);
	p_mallocator_ = pstMalloc;
	raw_data_.change_mallocator(pstMalloc);
}

int RawDataProcess::get_dirty_row_count(DTCJobOperation &job_op, Node *p_node)
{
	int iRet = 0;
	int dirty_rows = 0;

	iRet = attach_data(p_node, NULL);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("attach data error: %d", iRet);
		return iRet;
	}

	unsigned char uchRowFlags;
	unsigned int uiTotalRows = raw_data_.total_rows();

	DTCTableDefinition *t = raw_data_.get_node_table_def();
	RowValue stRow(t);
	for (unsigned int i = 0; i < uiTotalRows; i++) {
		iRet = raw_data_.decode_row(stRow, uchRowFlags, 0);
		if (iRet != 0) {
			log4cplus_error("raw-data decode row error: %d,%s",
					iRet, raw_data_.get_err_msg());
			return (-4);
		}

		if (uchRowFlags & OPER_DIRTY)
			dirty_rows++;
	}

	return dirty_rows;
}

// affected_data is always NULL
int RawDataProcess::do_delete(DTCJobOperation &job_op, Node *p_node,
			      RawData *affected_data)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	log4cplus_debug("do_delete start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = attach_data(p_node, affected_data);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("attach data error: %d", iRet);
		return (iRet);
	}

	if (affected_data != NULL)
		affected_data->set_refrence(&raw_data_);

	stpNodeTab = raw_data_.get_node_table_def();
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

	int iAffectRows = 0;
	unsigned char uchRowFlags;
	unsigned int uiTotalRows = raw_data_.total_rows();
	for (unsigned int i = 0; i < uiTotalRows; i++) {
		iRet = raw_data_.decode_row(*stpNodeRow, uchRowFlags, 0);
		if (iRet != DTC_CODE_SUCCESS) {
			log4cplus_error("raw-data decode row error: %d,%s",
					iRet, raw_data_.get_err_msg());
			return (-4);
		}
		if (stpNodeTab != stpTaskTab) {
			stpTaskRow->Copy(stpNodeRow);
		}
		if (job_op.compare_row(*stpTaskRow) != 0) { //符合del条件
			if (affected_data != NULL) { // copy row
				iRet = affected_data->copy_row();
				if (iRet != 0) {
					log4cplus_error(
						"raw-data copy row error: %d,%s",
						iRet,
						affected_data->get_err_msg());
				}
			}
			iRet = raw_data_.delete_cur_row(*stpNodeRow);
			if (iRet != EC_NO_MEM)
				p_node->vd_handle() = raw_data_.get_handle();
			if (iRet != 0) {
				log4cplus_error(
					"raw-data delete row error: %d,%s",
					iRet, raw_data_.get_err_msg());
				return (-5);
			}
			iAffectRows++;
			rows_count_--;
			if (uchRowFlags & OPER_DIRTY)
				dirty_rows_count_--;
		}
	}
	if (iAffectRows > 0) {
		if (job_op.resultInfo.affected_rows() == 0 ||
		    (job_op.request_condition() &&
		     job_op.request_condition()->has_type_timestamp())) {
			job_op.resultInfo.set_affected_rows(iAffectRows);
		}
		raw_data_.strip_mem();
	}

	if (raw_data_.total_rows() > 0) {
		log4cplus_debug("stat history datasize, size is %u",
				raw_data_.data_size());
		history_datasize.push(raw_data_.data_size());
		history_rowsize.push(raw_data_.total_rows());
		raw_data_.update_last_access_time_by_hour();
		raw_data_.update_last_update_time_by_hour();
	}
	return DTC_CODE_SUCCESS;
}

int RawDataProcess::do_get(DTCJobOperation &job_op, Node *p_node)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	log4cplus_debug("do_get start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;
	int laid = job_op.flag_no_cache() ?
			   -1 :
			   job_op.table_definition()->lastacc_field_id();

	iRet = raw_data_.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		log4cplus_error("raw-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				raw_data_.get_err_msg());
		return (-1);
	}

	unsigned int uiTotalRows = raw_data_.total_rows();
	job_op.prepare_result(); //准备返回结果对象
	if (job_op.all_rows() &&
	    (job_op.count_only() || !job_op.in_range((int)uiTotalRows, 0))) {
		if (job_op.is_batch_request()) {
			if ((int)uiTotalRows > 0)
				job_op.add_total_rows((int)uiTotalRows);
		} else {
			job_op.set_total_rows((int)uiTotalRows);
		}
	} else {
		stpNodeTab = raw_data_.get_node_table_def();
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
		unsigned char uchRowFlags;
		for (unsigned int i = 0; i < uiTotalRows; i++) //逐行拷贝数据
		{
			job_op.update_key(
				*stpNodeRow); // use stpNodeRow is fine, as just modify key field
			if ((iRet = raw_data_.decode_row(
				     *stpNodeRow, uchRowFlags, 0)) != 0) {
				log4cplus_error(
					"raw-data decode row error: %d,%s",
					iRet, raw_data_.get_err_msg());
				return (-2);
			}
			// this pointer compare is ok, as these two is both come from tabledefmanager. if they mean same, they are same object.
			if (stpNodeTab != stpTaskTab) {
				stpTaskRow->Copy(stpNodeRow);
			}
			if (job_op.compare_row(*stpTaskRow) ==
			    0) //如果不符合查询条件
				continue;

			if (stpTaskTab->expire_time_field_id() > 0)
				stpTaskRow->update_expire_time();
			//当前行添加到task中
			log4cplus_debug("append_row flag");
			if (job_op.append_row(stpTaskRow) > 0 && laid > 0) {
				raw_data_.update_lastacc(job_op.Timestamp());
			}
			if (job_op.all_rows() && job_op.result_full()) {
				job_op.set_total_rows((int)uiTotalRows);
				break;
			}
		}
	}
	/*更新访问时间和查找操作计数*/
	raw_data_.update_last_access_time_by_hour();
	raw_data_.inc_select_count();
	log4cplus_debug(
		"node[id:%u] ,Get Count is %d, last_access_time is %d, create_time is %d",
		p_node->node_id(), raw_data_.get_select_op_count(),
		raw_data_.get_last_access_time_by_hour(),
		raw_data_.get_create_time_by_hour());
	return DTC_CODE_SUCCESS;
}

// affected_data is always NULL
int RawDataProcess::do_append(DTCJobOperation &job_op, Node *p_node,
			      RawData *affected_data, bool isDirty,
			      bool setrows)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	iRet = attach_data(p_node, affected_data);
	if (iRet != DTC_CODE_SUCCESS) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach data error");
		log4cplus_warning("attach data error: %d", iRet);
		return (iRet);
	}

	stpNodeTab = raw_data_.get_node_table_def();
	stpTaskTab = job_op.table_definition();
	RowValue stTaskRow(stpTaskTab);
	RowValue stNodeRow(stpNodeTab);
	stpTaskRow = &stTaskRow;
	stpTaskRow->default_value();
	job_op.update_row(*stpTaskRow);

	if (stpTaskTab->auto_increment_field_id() >= stpTaskTab->key_fields() &&
	    job_op.resultInfo.insert_id()) {
		const int iFieldID = stpTaskTab->auto_increment_field_id();
		const uint64_t iVal = job_op.resultInfo.insert_id();
		stpTaskRow->field_value(iFieldID)->Set(iVal);
	}

	if (stpNodeTab == stpTaskTab) {
		stpNodeRow = stpTaskRow;
	} else {
		stpNodeRow = &stNodeRow;
		stpNodeRow->default_value();
		stpNodeRow->Copy(stpTaskRow);
	}

	log4cplus_debug("do_append start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	unsigned int uiTotalRows = raw_data_.total_rows();
	if (uiTotalRows > 0) {
		if ((isDirty || setrows) &&
		    job_op.table_definition()->key_as_uniq_field()) {
			snprintf(err_message_, sizeof(err_message_),
				 "duplicate key error");
			return (-1062);
		}
		RowValue stOldRow(stpNodeTab); //一行数据
		if (setrows &&
		    job_op.table_definition()->key_part_of_uniq_field()) {
			for (unsigned int i = 0; i < uiTotalRows;
			     i++) { //逐行拷贝数据
				unsigned char uchRowFlags;
				if (raw_data_.decode_row(stOldRow, uchRowFlags,
							 0) != 0) {
					log4cplus_error(
						"raw-data decode row error: %d,%s",
						iRet, raw_data_.get_err_msg());
					return (-1);
				}

				if (stpNodeRow->Compare(
					    stOldRow,
					    stpNodeTab->uniq_fields_list(),
					    stpNodeTab->uniq_fields()) == 0) {
					snprintf(err_message_,
						 sizeof(err_message_),
						 "duplicate key error");
					return (-1062);
				}
			}
		}
	}

	if (affected_data != NULL &&
	    affected_data->insert_row(*stpNodeRow, false, isDirty) != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data insert row error: %s",
			 affected_data->get_err_msg());
		return (-1);
	}

	// insert clean row
	iRet = raw_data_.insert_row(
		*stpNodeRow, update_mode_.m_uchInsertOrder ? true : false,
		isDirty);
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(raw_data_.need_size(),
						   *p_node) == 0)
			iRet = raw_data_.insert_row(
				*stpNodeRow,
				update_mode_.m_uchInsertOrder ? true : false,
				isDirty);
	}
	if (iRet != EC_NO_MEM)
		p_node->vd_handle() = raw_data_.get_handle();
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data insert row error: %s",
			 raw_data_.get_err_msg());
		/*标记加入黑名单*/
		job_op.push_black_list_size(raw_data_.need_size());
		return (-2);
	}

	if (job_op.resultInfo.affected_rows() == 0 || setrows == true)
		job_op.resultInfo.set_affected_rows(1);
	rows_count_++;
	if (isDirty)
		dirty_rows_count_++;
	log4cplus_debug("stat history datasize, size is %u",
			raw_data_.data_size());
	history_datasize.push(raw_data_.data_size());
	history_rowsize.push(raw_data_.total_rows());
	raw_data_.update_last_access_time_by_hour();
	raw_data_.update_last_update_time_by_hour();
	log4cplus_debug(
		"node[id:%u] ，Get Count is %d, create_time is %d, last_access_time is %d, last_update_time is %d ",
		p_node->node_id(), raw_data_.get_select_op_count(),
		raw_data_.get_create_time_by_hour(),
		raw_data_.get_last_access_time_by_hour(),
		raw_data_.get_last_update_time_by_hour());
	return DTC_CODE_SUCCESS;
}

int RawDataProcess::do_replace_all(DTCJobOperation &job_op, Node *p_node)
{
	log4cplus_debug("do_replace_all start! ");
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow;

	int iRet;
	int try_purge_count = 0;
	uint64_t all_rows_size = 0;
	int laid = job_op.flag_no_cache() || job_op.count_only() ?
			   -1 :
			   job_op.table_definition()->lastacc_field_id();
	int matchedCount = 0;
	int limitStart = 0;
	int limitStop = 0x10000000;

	stpTaskTab = job_op.table_definition();
	if (DTCColExpand::instance()->is_expanding())
		stpNodeTab =
			TableDefinitionManager::instance()->get_new_table_def();
	else
		stpNodeTab =
			TableDefinitionManager::instance()->get_cur_table_def();
	RowValue stNodeRow(stpNodeTab);
	stpNodeRow = &stNodeRow;
	stpNodeRow->default_value();

	if (laid > 0 && job_op.requestInfo.limit_count() > 0) {
		limitStart = job_op.requestInfo.limit_start();
		if (job_op.requestInfo.limit_start() > 0x10000000) {
			laid = -1;
		} else if (job_op.requestInfo.limit_count() < 0x10000000) {
			limitStop =
				limitStart + job_op.requestInfo.limit_count();
		}
	}

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	if (p_node->vd_handle() != INVALID_HANDLE) {
		iRet = destroy_data(p_node);
		if (iRet != 0)
			return (-1);
	}

	iRet = raw_data_.do_init(job_op.packed_key(), 0);
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(raw_data_.need_size(),
						   *p_node) == 0)
			iRet = raw_data_.init(p_table_->key_fields() - 1,
					      p_table_->key_format(),
					      job_op.packed_key(), 0);
	}
	if (iRet != EC_NO_MEM)
		p_node->vd_handle() = raw_data_.get_handle();

	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data init error: %s", raw_data_.get_err_msg());
		/*标记加入黑名单*/
		job_op.push_black_list_size(raw_data_.need_size());
		p_buffer_pond_->purge_node(job_op.packed_key(), *p_node);
		return (-2);
	}

	if (job_op.result != NULL) {
		ResultSet *pstResultSet = job_op.result;
		for (int i = 0; i < pstResultSet->total_rows(); i++) {
			RowValue *pstRow = pstResultSet->_fetch_row();
			if (pstRow == NULL) {
				log4cplus_debug("%s!",
						"call fetch_row func error");
				p_buffer_pond_->purge_node(job_op.packed_key(),
							   *p_node);
				raw_data_.destory();
				return (-3);
			}

			if (laid > 0 && job_op.compare_row(*pstRow)) {
				if (matchedCount >= limitStart &&
				    matchedCount < limitStop) {
					(*pstRow)[laid].s64 =
						job_op.Timestamp();
				}
				matchedCount++;
			}

			if (stpTaskTab != stpNodeTab) {
				stpNodeRow->Copy(pstRow);
			} else {
				stpNodeRow = pstRow;
			}

			/* 插入当前行 */
			iRet = raw_data_.insert_row(*stpNodeRow, false, false);

			/* 如果内存空间不足，尝试扩大最多两次 */
			if (iRet == EC_NO_MEM) {
				/* 预测整个Node的数据大小 */
				all_rows_size = raw_data_.need_size() -
						raw_data_.data_start();
				all_rows_size *= pstResultSet->total_rows();
				all_rows_size /= (i + 1);
				all_rows_size += raw_data_.data_start();

				if (try_purge_count >= 2) {
					goto ERROR_PROCESS;
				}

				/* 尝试次数 */
				++try_purge_count;
				if (p_buffer_pond_->try_purge_size(
					    (size_t)all_rows_size, *p_node) ==
				    0)
					iRet = raw_data_.insert_row(
						*stpNodeRow, false, false);
			}
			if (iRet != EC_NO_MEM)
				p_node->vd_handle() = raw_data_.get_handle();

			/* 当前行操作成功 */
			if (0 == iRet)
				continue;
		ERROR_PROCESS:
			snprintf(
				err_message_, sizeof(err_message_),
				"raw-data insert row error: ret=%d,err=%s, cnt=%d",
				iRet, raw_data_.get_err_msg(), try_purge_count);
			/*标记加入黑名单*/
			job_op.push_black_list_size(all_rows_size);
			p_buffer_pond_->purge_node(job_op.packed_key(),
						   *p_node);
			raw_data_.destory();
			return (-4);
		}

		rows_count_ += pstResultSet->total_rows();
	}

	raw_data_.update_last_access_time_by_hour();
	raw_data_.update_last_update_time_by_hour();
	log4cplus_debug(
		"node[id:%u], handle[" UINT64FMT
		"] ,data-size[%u],  Get Count is %d, create_time is %d, last_access_time is %d, Update time is %d",
		p_node->node_id(), p_node->vd_handle(), raw_data_.data_size(),
		raw_data_.get_select_op_count(),
		raw_data_.get_create_time_by_hour(),
		raw_data_.get_last_access_time_by_hour(),
		raw_data_.get_last_update_time_by_hour());

	history_datasize.push(raw_data_.data_size());
	history_rowsize.push(raw_data_.total_rows());
	return DTC_CODE_SUCCESS;
}

// The correct replace behavior:
// 	If conflict rows found, delete them all
// 	Insert new row
// 	Affected rows is total deleted and inserted rows
// Implementation hehavior:
// 	If first conflict row found, update it, and increase affected rows to 2 (1 delete + 1 insert)
// 	delete other fonflict row, increase affected 1 per row
// 	If no rows found, insert it and set affected rows to 1
int RawDataProcess::do_replace(DTCJobOperation &job_op, Node *p_node,
			       RawData *affected_data, bool async, bool setrows)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	log4cplus_debug("do_replace start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	if (p_node->vd_handle() == INVALID_HANDLE) {
		iRet = init_data(p_node, affected_data, job_op.packed_key());
		if (iRet != DTC_CODE_SUCCESS) {
			log4cplus_error("init data error: %d", iRet);
			if (p_node->vd_handle() == INVALID_HANDLE)
				p_buffer_pond_->purge_node(job_op.packed_key(),
							   *p_node);
			return (iRet);
		}
	} else {
		iRet = attach_data(p_node, affected_data);
		if (iRet != DTC_CODE_SUCCESS) {
			log4cplus_error("attach data error: %d", iRet);
			return (iRet);
		}
	}

	unsigned char uchRowFlags;
	uint64_t ullAffectedrows = 0;
	unsigned int uiTotalRows = raw_data_.total_rows();
	if (affected_data != NULL)
		affected_data->set_refrence(&raw_data_);

	stpNodeTab = raw_data_.get_node_table_def();
	stpTaskTab = job_op.table_definition();
	RowValue stNewRow(stpTaskTab);
	RowValue stNewNodeRow(stpNodeTab);
	stNewRow.default_value();
	stpTaskRow = &stNewRow;
	stpNodeRow = &stNewNodeRow;
	job_op.update_row(*stpTaskRow); //获取Replace的行
	if (stpNodeTab != stpTaskTab)
		stpNodeRow->Copy(stpTaskRow);
	else
		stpNodeRow = stpTaskRow;

	RowValue stRow(stpNodeTab); //一行数据
	for (unsigned int i = 0; i < uiTotalRows; i++) { //逐行拷贝数据
		if (raw_data_.decode_row(stRow, uchRowFlags, 0) != 0) {
			log4cplus_error("raw-data decode row error: %d,%s",
					iRet, raw_data_.get_err_msg());
			return (-1);
		}

		if (job_op.table_definition()->key_as_uniq_field() == false &&
		    stNewRow.Compare(
			    stRow,
			    job_op.table_definition()->uniq_fields_list(),
			    job_op.table_definition()->uniq_fields()) != 0)
			continue;

		if (ullAffectedrows == 0) {
			if (affected_data != NULL &&
			    affected_data->insert_row(*stpNodeRow, false,
						      async) != 0) {
				log4cplus_error(
					"raw-data copy row error: %d,%s", iRet,
					affected_data->get_err_msg());
				return (-2);
			}

			ullAffectedrows = 2;
			iRet = raw_data_.replace_cur_row(*stpNodeRow,
							 async); // 加进cache
		} else {
			ullAffectedrows++;
			iRet = raw_data_.delete_cur_row(
				*stpNodeRow); // 加进cache
		}
		if (iRet == EC_NO_MEM) {
			if (p_buffer_pond_->try_purge_size(
				    raw_data_.need_size(), *p_node) == 0)
				iRet = raw_data_.replace_cur_row(*stpNodeRow,
								 async);
		}
		if (iRet != EC_NO_MEM)
			p_node->vd_handle() = raw_data_.get_handle();
		if (iRet != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "raw-data replace row error: %d, %s", iRet,
				 raw_data_.get_err_msg());
			/*标记加入黑名单*/
			job_op.push_black_list_size(raw_data_.need_size());
			return (-3);
		}
		if (uchRowFlags & OPER_DIRTY)
			dirty_rows_count_--;
		if (async)
			dirty_rows_count_++;
	}

	if (ullAffectedrows == 0) { // 找不到匹配的行，insert一行
		iRet = raw_data_.insert_row(*stpNodeRow, false,
					    async); // 加进cache
		if (iRet == EC_NO_MEM) {
			if (p_buffer_pond_->try_purge_size(
				    raw_data_.need_size(), *p_node) == 0)
				iRet = raw_data_.insert_row(*stpNodeRow, false,
							    async);
		}
		if (iRet != EC_NO_MEM)
			p_node->vd_handle() = raw_data_.get_handle();

		if (iRet != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "raw-data replace row error: %d, %s", iRet,
				 raw_data_.get_err_msg());
			/*标记加入黑名单*/
			job_op.push_black_list_size(raw_data_.need_size());
			return (-3);
		}
		rows_count_++;
		ullAffectedrows++;
		if (async)
			dirty_rows_count_++;
	}

	if (async == true || setrows == true) {
		job_op.resultInfo.set_affected_rows(ullAffectedrows);
	} else if (ullAffectedrows != job_op.resultInfo.affected_rows()) {
		//如果cache更新纪录数和helper更新的纪录数不相等
		log4cplus_debug(
			"unequal affected rows, cache[%lld], helper[%lld]",
			(long long)ullAffectedrows,
			(long long)job_op.resultInfo.affected_rows());
	}

	log4cplus_debug("stat history datasize, size is %u",
			raw_data_.data_size());
	history_datasize.push(raw_data_.data_size());
	history_rowsize.push(raw_data_.total_rows());
	raw_data_.update_last_access_time_by_hour();
	raw_data_.update_last_update_time_by_hour();
	log4cplus_debug(
		"node[id:%u], create_time is %d, last_access_time is %d, Update Time is %d ",
		p_node->node_id(), raw_data_.get_create_time_by_hour(),
		raw_data_.get_last_access_time_by_hour(),
		raw_data_.get_last_update_time_by_hour());
	return DTC_CODE_SUCCESS;
}

/*
 * encode到私有内存，防止replace，update引起重新rellocate导致value引用了过期指针
 */
int RawDataProcess::encode_to_private_area(RawData &raw, RowValue &value,
					   unsigned char value_flag)
{
	int ret = raw.do_init(raw_data_.key(),
			      raw.calc_row_size(value,
						p_table_->key_fields() - 1));
	if (DTC_CODE_SUCCESS != ret) {
		log4cplus_error("init raw-data struct error, ret=%d, err=%s",
				ret, raw.get_err_msg());
		return -1;
	}

	ret = raw.insert_row(value, false, false);
	if (DTC_CODE_SUCCESS != ret) {
		log4cplus_error("insert row to raw-data error: ret=%d, err=%s",
				ret, raw.get_err_msg());
		return -2;
	}

	raw.rewind();

	ret = raw.decode_row(value, value_flag, 0);
	if (DTC_CODE_SUCCESS != ret) {
		log4cplus_error("decode raw-data to row error: ret=%d, err=%s",
				ret, raw.get_err_msg());
		return -3;
	}

	return DTC_CODE_SUCCESS;
}

int RawDataProcess::do_update(DTCJobOperation &job_op, Node *p_node,
			      RawData *affected_data, bool async, bool setrows)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	log4cplus_debug("do_update start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = attach_data(p_node, affected_data);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("attach data error: %d", iRet);
		return (iRet);
	}

	unsigned char uchRowFlags;
	uint64_t ullAffectedrows = 0;
	unsigned int uiTotalRows = raw_data_.total_rows();
	if (affected_data != NULL)
		affected_data->set_refrence(&raw_data_);

	RowValue stRow(job_op.table_definition()); //一行数据

	stpNodeTab = raw_data_.get_node_table_def();
	stpTaskTab = job_op.table_definition();
	RowValue stNewRow(stpTaskTab);
	RowValue stNewNodeRow(stpNodeTab);
	stpTaskRow = &stNewRow;
	stpNodeRow = &stNewNodeRow;
	if (stpNodeTab == stpTaskTab)
		stpNodeRow = stpTaskRow;

	for (unsigned int i = 0; i < uiTotalRows; i++) { //逐行拷贝数据
		if (raw_data_.decode_row(*stpNodeRow, uchRowFlags, 0) != 0) {
			log4cplus_error("raw-data decode row error: %d,%s",
					iRet, raw_data_.get_err_msg());
			return (-1);
		}

		if (stpNodeTab != stpTaskTab)
			stpTaskRow->Copy(stpNodeRow);

		//如果不符合查询条件
		if (job_op.compare_row(*stpTaskRow) == 0)
			continue;

		job_op.update_row(*stpTaskRow); //修改数据
		ullAffectedrows++;

		if (stpNodeTab != stpTaskTab)
			stpNodeRow->Copy(stpTaskRow);

		if (affected_data != NULL &&
		    affected_data->insert_row(*stpNodeRow, false, async) != 0) {
			log4cplus_error("raw-data copy row error: %d,%s", iRet,
					affected_data->get_err_msg());
			return (-2);
		}

		// 在私有区间decode
		RawData stTmpRows(&g_stSysMalloc, 1);
		if (encode_to_private_area(stTmpRows, *stpNodeRow,
					   uchRowFlags)) {
			log4cplus_error(
				"encode rowvalue to private rawdata area failed");
			return -3;
		}

		iRet = raw_data_.replace_cur_row(*stpNodeRow,
						 async); // 加进cache
		if (iRet == EC_NO_MEM) {
			if (p_buffer_pond_->try_purge_size(
				    raw_data_.need_size(), *p_node) == 0)
				iRet = raw_data_.replace_cur_row(*stpNodeRow,
								 async);
		}
		if (iRet != EC_NO_MEM)
			p_node->vd_handle() = raw_data_.get_handle();
		if (iRet != 0) {
			snprintf(err_message_, sizeof(err_message_),
				 "raw-data replace row error: %d, %s", iRet,
				 raw_data_.get_err_msg());
			/*标记加入黑名单*/
			job_op.push_black_list_size(raw_data_.need_size());
			return (-6);
		}

		if (uchRowFlags & OPER_DIRTY)
			dirty_rows_count_--;
		if (async)
			dirty_rows_count_++;
	}

	if (async == true || setrows == true) {
		job_op.resultInfo.set_affected_rows(ullAffectedrows);
	} else if (ullAffectedrows != job_op.resultInfo.affected_rows()) {
		//如果cache更新纪录数和helper更新的纪录数不相等
		log4cplus_debug(
			"unequal affected rows, cache[%lld], helper[%lld]",
			(long long)ullAffectedrows,
			(long long)job_op.resultInfo.affected_rows());
	}
	log4cplus_debug("stat history datasize, size is %u",
			raw_data_.data_size());
	history_datasize.push(raw_data_.data_size());
	history_rowsize.push(raw_data_.total_rows());
	raw_data_.update_last_access_time_by_hour();
	raw_data_.update_last_update_time_by_hour();
	log4cplus_debug(
		"node[id:%u], create_time is %d, last_access_time is %d, UpdateTime is %d",
		p_node->node_id(), raw_data_.get_create_time_by_hour(),
		raw_data_.get_last_access_time_by_hour(),
		raw_data_.get_last_update_time_by_hour());
	return DTC_CODE_SUCCESS;
}

int RawDataProcess::do_flush(DTCFlushRequest *flush_req, Node *p_node,
			     unsigned int &affected_count)
{
	int iRet;

	log4cplus_debug("do_flush start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = attach_data(p_node, NULL);
	if (iRet != DTC_CODE_SUCCESS) {
		log4cplus_error("attach data error: %d", iRet);
		return (iRet);
	}

	unsigned char uchRowFlags;
	unsigned int uiTotalRows = raw_data_.total_rows();

	affected_count = 0;
	DTCValue astKey[p_table_->key_fields()];
	TaskPackedKey::unpack_key(p_table_, raw_data_.key(), astKey);
	RowValue stRow(p_table_); //一行数据
	for (int i = 0; i < p_table_->key_fields(); i++)
		stRow[i] = astKey[i];

	for (unsigned int i = 0; p_node->is_dirty() && i < uiTotalRows;
	     i++) { //逐行拷贝数据
		if (raw_data_.decode_row(stRow, uchRowFlags, 0) != 0) {
			log4cplus_error("raw-data decode row error: %d,%s",
					iRet, raw_data_.get_err_msg());
			return (-1);
		}

		if ((uchRowFlags & OPER_DIRTY) == false)
			continue;

		if (flush_req && flush_req->flush_row(stRow) != 0) {
			log4cplus_error("do_flush() invoke flushRow() failed.");
			return (-2);
		}
		raw_data_.set_cur_row_flag(uchRowFlags & ~OPER_DIRTY);
		dirty_rows_count_--;
		affected_count++;
	}

	return DTC_CODE_SUCCESS;
}

int RawDataProcess::do_purge(DTCFlushRequest *flush_req, Node *p_node,
			     unsigned int &affected_count)
{
	int iRet = DTC_CODE_SUCCESS;

	log4cplus_debug("do_purge start! ");

	iRet = do_flush(flush_req, p_node, affected_count);
	if (iRet != 0) {
		return (iRet);
	}
	rows_count_ = 0LL - raw_data_.total_rows();

	return DTC_CODE_SUCCESS;
}
