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

#include "tree_data_process.h"
#include "global.h"
#include "log/log.h"
#include "sys_malloc.h"

DTC_USING_NAMESPACE

TreeDataProcess::TreeDataProcess(MallocBase *pstMalloc,
				 DTCTableDefinition *p_table_definition_,
				 BufferPond *pstPool,
				 const UpdateMode *pstUpdateMode)
	: m_stTreeData(pstMalloc), p_table_(p_table_definition_),
	  p_mallocator_(pstMalloc), p_buffer_pond_(pstPool)
{
	memcpy(&update_mode_, pstUpdateMode, sizeof(update_mode_));
	nodeSizeLimit = 0;
	history_rowsize = g_stat_mgr.get_sample(ROW_SIZE_HISTORY_STAT);
}

TreeDataProcess::~TreeDataProcess()
{
}

int TreeDataProcess::get_expire_time(DTCTableDefinition *t, Node *p_node,
				     uint32_t &expire)
{
	int iRet = 0;

	iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach data error");
		log4cplus_error("tree-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return (iRet);
	}

	iRet = m_stTreeData.get_expire_time(t, expire);
	if (iRet != 0) {
		log4cplus_error("tree data get expire time error: %d", iRet);
		return iRet;
	}
	return 0;
}

int TreeDataProcess::do_replace_all(Node *p_node, RawData *new_data)
{
	int iRet;

	log4cplus_debug("Replace TreeData start ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	TreeData tmpTreeData(p_mallocator_);

	iRet = tmpTreeData.do_init(new_data->key());
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(tmpTreeData.need_size(),
						   *p_node) == 0)
			iRet = tmpTreeData.do_init(new_data->key());
	}

	if (iRet != 0) {
		const char* err_msg = tmpTreeData.get_err_msg();
		size_t available_space = sizeof(err_message_) - 23; // Reserve space for prefix
		snprintf(err_message_, sizeof(err_message_),
			 "root-data init error: %.*s",
			 (int)available_space, err_msg ? err_msg : "unknown error");
		tmpTreeData.destory();
		return (-2);
	}

	iRet = tmpTreeData.copy_tree_all(new_data);
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(tmpTreeData.need_size(),
						   *p_node) == 0)
			iRet = tmpTreeData.copy_tree_all(new_data);
	}

	if (iRet != 0) {
		const char* err_msg = tmpTreeData.get_err_msg();
		size_t available_space = sizeof(err_message_) - 23; // Reserve space for prefix
		snprintf(err_message_, sizeof(err_message_),
			 "root-data init error: %.*s",
			 (int)available_space, err_msg ? err_msg : "unknown error");
		tmpTreeData.destory();
		return (-2);
	}

	if (p_node->vd_handle() != INVALID_HANDLE)
		destroy_data(p_node);
	p_node->vd_handle() = tmpTreeData.get_handle();

	if (tmpTreeData.total_rows() > 0) {
		history_rowsize.push(tmpTreeData.total_rows());
	}
	return (0);
}

int TreeDataProcess::do_append(DTCJobOperation &job_op, Node *p_node,
			       RawData *affected_data, bool isDirty,
			       bool setrows)
{
	int iRet;
	DTCTableDefinition *stpNodeTab, *stpTaskTab;
	RowValue *stpNodeRow, *stpTaskRow;

	iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach data error");
		log4cplus_error("tree-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return (iRet);
	}

	stpNodeTab = m_stTreeData.get_node_table_def();
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

	log4cplus_debug("AppendTreeData start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	unsigned int uiTotalRows = m_stTreeData.total_rows();
	if (uiTotalRows > 0) {
		if ((isDirty || setrows) &&
		    job_op.table_definition()->key_as_uniq_field()) {
			snprintf(err_message_, sizeof(err_message_),
				 "duplicate key error");
			return (-1062);
		}
		if (setrows &&
		    job_op.table_definition()->key_part_of_uniq_field()) {
			iRet = m_stTreeData.compare_tree_data(stpNodeRow);
			if (iRet < 0) {
				log4cplus_error(
					"tree-data decode row error: %d,%s",
					iRet, m_stTreeData.get_err_msg());
				return iRet;
			} else if (iRet == 0) {
				snprintf(err_message_, sizeof(err_message_),
					 "duplicate key error");
				return (-1062);
			}
		}
	}

	// insert clean row
	iRet = m_stTreeData.insert_row(*stpNodeRow, KeyCompare, isDirty);
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(m_stTreeData.need_size(),
						   *p_node) == 0)
			iRet = m_stTreeData.insert_row(*stpNodeRow, KeyCompare,
						       isDirty);
	}
	if (iRet != EC_NO_MEM)
		p_node->vd_handle() = m_stTreeData.get_handle();
	if (iRet != 0) {
		const char* err_msg = m_stTreeData.get_err_msg();
		size_t available_space = sizeof(err_message_) - 32; // Reserve space for prefix and error code
		snprintf(err_message_, sizeof(err_message_),
			 "tree-data insert row error: %.*s,%d",
			 (int)available_space, err_msg ? err_msg : "unknown error", iRet);
		/* Mark to add to blacklist */
		job_op.push_black_list_size(m_stTreeData.need_size());
		return (-2);
	}

	if (job_op.resultInfo.affected_rows() == 0 || setrows == true)
		job_op.resultInfo.set_affected_rows(1);
	rows_count_++;
	if (isDirty)
		dirty_rows_count_++;
	history_rowsize.push(m_stTreeData.total_rows());
	return (0);
}

int TreeDataProcess::do_get(DTCJobOperation &job_op, Node *p_node)
{
	int iRet;
	log4cplus_debug("Get TreeData start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach data error");
		log4cplus_error("tree-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return (-1);
	}

	iRet = m_stTreeData.get_tree_data(job_op);
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "get tree data error");
		log4cplus_error("tree-data get[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return iRet;
	}

	/*Update access time and query operation count*/
	log4cplus_debug("node[id:%u] ,Get Count is %d", p_node->node_id(),
			m_stTreeData.total_rows());
	return (0);
}

int TreeDataProcess::expand_node(DTCJobOperation &job_op, Node *p_node)
{
	return 0;
}

int TreeDataProcess::get_dirty_row_count(DTCJobOperation &job_op, Node *p_node)
{
	int iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach data error");
		log4cplus_error("tree-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return (-1);
	}

	return m_stTreeData.get_dirty_row_count();
}

int TreeDataProcess::attach_data(Node *p_node, RawData *affected_data)
{
	int iRet;

	iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		log4cplus_error("tree-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return (-1);
	}

	if (affected_data != NULL) {
		iRet = affected_data->do_init(m_stTreeData.key(), 0);
		if (iRet != 0) {
			log4cplus_error("tree-data init error: %d,%s", iRet,
					affected_data->get_err_msg());
			return (-2);
		}
	}

	return (0);
}

int TreeDataProcess::get_node_all_rows_count(Node *p_node, RawData *pstRows)
{
	int iRet = 0;

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = attach_data(p_node, pstRows);
	if (iRet != 0) {
		log4cplus_error("attach data error: %d", iRet);
		return (-1);
	}

	iRet = m_stTreeData.copy_raw_all(pstRows);
	if (iRet != 0) {
		log4cplus_error("copy data error: %d,%s", iRet,
				m_stTreeData.get_err_msg());
		return (-2);
	}

	return (0);
}

int TreeDataProcess::do_delete(DTCJobOperation &job_op, Node *p_node,
			       RawData *affected_data)
{
	int iRet;
	log4cplus_debug("Delete TreeData start! ");

	iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach data error");
		log4cplus_error("tree-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return (-1);
	}

	int start = m_stTreeData.total_rows();

	iRet = m_stTreeData.delete_tree_data(job_op);
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "get tree data error");
		log4cplus_error("tree-data get[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return iRet;
	}

	int iAffectRows = start - m_stTreeData.total_rows();
	if (iAffectRows > 0) {
		if (job_op.resultInfo.affected_rows() == 0 ||
		    (job_op.request_condition() &&
		     job_op.request_condition()->has_type_timestamp())) {
			job_op.resultInfo.set_affected_rows(iAffectRows);
		}
	}

	rows_count_ = m_stTreeData.get_increase_row_count();
	dirty_rows_count_ = m_stTreeData.get_increase_dirty_row_count();

	log4cplus_debug("node[id:%u] ,Get Count is %d", p_node->node_id(),
			m_stTreeData.total_rows());
	return (0);
}

int TreeDataProcess::do_replace_all(DTCJobOperation &job_op, Node *p_node)
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

	iRet = m_stTreeData.do_init(job_op.packed_key());
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(m_stTreeData.need_size(),
						   *p_node) == 0)
			iRet = m_stTreeData.do_init(p_table_->key_fields() - 1,
						    p_table_->key_format(),
						    job_op.packed_key());
	}
	if (iRet != EC_NO_MEM)
		p_node->vd_handle() = m_stTreeData.get_handle();

	if (iRet != 0) {
		const char* err_msg = m_stTreeData.get_err_msg();
		size_t available_space = sizeof(err_message_) - 22; // Reserve space for prefix
		snprintf(err_message_, sizeof(err_message_),
			 "raw-data init error: %.*s",
			 (int)available_space, err_msg ? err_msg : "unknown error");
		/* Mark to add to blacklist */
		job_op.push_black_list_size(m_stTreeData.need_size());
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
				m_stTreeData.destory();
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

			/* Insert current row */
			iRet = m_stTreeData.insert_row(*stpNodeRow, KeyCompare,
						       false);

			/* If insufficient memory space, try to expand up to two times */
			if (iRet == EC_NO_MEM) {
				if (try_purge_count >= 2) {
					goto ERROR_PROCESS;
				}

				/* Number of attempts */
				++try_purge_count;
				if (p_buffer_pond_->try_purge_size(
					    m_stTreeData.need_size(),
					    *p_node) == 0)
					iRet = m_stTreeData.insert_row(
						*stpNodeRow, KeyCompare, false);
			}
			if (iRet != EC_NO_MEM)
				p_node->vd_handle() = m_stTreeData.get_handle();

			/* Current row operation successful */
			if (0 == iRet)
				continue;
		ERROR_PROCESS:
			const char* err_msg = m_stTreeData.get_err_msg();
			size_t available_space = sizeof(err_message_) - 50; // Reserve space for prefix and numbers
			snprintf(
				err_message_, sizeof(err_message_),
				"raw-data insert row error: ret=%d,err=%.*s, cnt=%d",
				iRet, (int)available_space, err_msg ? err_msg : "unknown error",
				try_purge_count);
			/* Mark to add to blacklist */
			job_op.push_black_list_size(all_rows_size);
			p_buffer_pond_->purge_node(job_op.packed_key(),
						   *p_node);
			m_stTreeData.destory();
			return (-4);
		}

		rows_count_ += pstResultSet->total_rows();
	}

	history_rowsize.push(m_stTreeData.total_rows());

	return (0);
}

int TreeDataProcess::do_replace(DTCJobOperation &job_op, Node *p_node,
				RawData *affected_data, bool async,
				bool setrows = false)
{
	int iRet;
	log4cplus_debug("Replace TreeData start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	if (p_node) {
		iRet = m_stTreeData.do_attach(p_node->vd_handle());
		if (iRet != 0) {
			log4cplus_error("attach tree data error: %d", iRet);
			return (iRet);
		}
	} else {
		iRet = m_stTreeData.do_init(job_op.packed_key());
		if (iRet == EC_NO_MEM) {
			if (p_buffer_pond_->try_purge_size(
				    m_stTreeData.need_size(), *p_node) == 0)
				iRet = m_stTreeData.do_init(
					job_op.packed_key());
		}

		if (iRet != 0) {
			log4cplus_error("tree-data replace[handle:" UINT64FMT
					"] error: %d,%s",
					p_node->vd_handle(), iRet,
					m_stTreeData.get_err_msg());
			return iRet;
		}

		p_node->vd_handle() = m_stTreeData.get_handle();
	}

	unsigned char uchRowFlags;
	iRet = m_stTreeData.replace_tree_data(job_op, p_node, affected_data,
					      async, uchRowFlags, setrows);
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(m_stTreeData.need_size(),
						   *p_node) == 0)
			iRet = m_stTreeData.replace_tree_data(
				job_op, p_node, affected_data, async,
				uchRowFlags, setrows);
	}

	if (iRet != 0) {
		log4cplus_error("tree-data replace[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return iRet;
	}

	if (uchRowFlags & OPER_DIRTY)
		dirty_rows_count_--;
	if (async)
		dirty_rows_count_++;

	uint64_t ullAffectedRows = m_stTreeData.get_affectedrows();
	if (ullAffectedRows == 0) //insert
	{
		DTCTableDefinition *stpTaskTab;
		RowValue *stpNewRow;
		stpTaskTab = job_op.table_definition();
		RowValue stNewRow(stpTaskTab);
		stNewRow.default_value();
		stpNewRow = &stNewRow;
		job_op.update_row(*stpNewRow); // Get Replace row
		iRet = m_stTreeData.insert_row(*stpNewRow, KeyCompare,
					       async); // Add to cache
		if (iRet == EC_NO_MEM) {
			if (p_buffer_pond_->try_purge_size(
				    m_stTreeData.need_size(), *p_node) == 0)
				iRet = m_stTreeData.insert_row(
					*stpNewRow, KeyCompare, async);
		}
		if (iRet != EC_NO_MEM)
			p_node->vd_handle() = m_stTreeData.get_handle();

		if (iRet != 0) {
			const char* err_msg = m_stTreeData.get_err_msg();
			size_t available_space = sizeof(err_message_) - 35; // Reserve space for prefix and error code
			snprintf(err_message_, sizeof(err_message_),
				 "raw-data replace row error: %d, %.*s", iRet,
				 (int)available_space, err_msg ? err_msg : "unknown error");
			/* Mark to add to blacklist */
			job_op.push_black_list_size(m_stTreeData.need_size());
			return (-3);
		}
		rows_count_++;
		ullAffectedRows++;
		if (async)
			dirty_rows_count_++;
	}
	if (async == true || setrows == true) {
		job_op.resultInfo.set_affected_rows(ullAffectedRows);
	} else if (ullAffectedRows != job_op.resultInfo.affected_rows()) {
		// If the number of cache updated records is not equal to the number of helper updated records
		log4cplus_debug(
			"unequal affected rows, cache[%lld], helper[%lld]",
			(long long)ullAffectedRows,
			(long long)job_op.resultInfo.affected_rows());
	}

	return 0;
}

int TreeDataProcess::do_update(DTCJobOperation &job_op, Node *p_node,
			       RawData *affected_data, bool async,
			       bool setrows = false)
{
	int iRet;
	log4cplus_debug("Update TreeData start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		log4cplus_error("attach tree data error: %d", iRet);
		return (iRet);
	}

	m_stTreeData.set_affected_rows(0);

	iRet = m_stTreeData.update_tree_data(job_op, p_node, affected_data,
					     async, setrows);
	if (iRet == EC_NO_MEM) {
		if (p_buffer_pond_->try_purge_size(m_stTreeData.need_size(),
						   *p_node) == 0)
			iRet = m_stTreeData.update_tree_data(
				job_op, p_node, affected_data, async, setrows);
	}

	if (iRet != 0) {
		log4cplus_error("tree-data update[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return iRet;
	}

	uint64_t ullAffectedRows = m_stTreeData.get_affectedrows();
	dirty_rows_count_ = m_stTreeData.get_increase_dirty_row_count();

	if (async == true || setrows == true) {
		job_op.resultInfo.set_affected_rows(ullAffectedRows);
	} else if (ullAffectedRows != job_op.resultInfo.affected_rows()) {
		// If the number of cache updated records is not equal to the number of helper updated records
		log4cplus_debug(
			"unequal affected rows, cache[%lld], helper[%lld]",
			(long long)ullAffectedRows,
			(long long)job_op.resultInfo.affected_rows());
	}

	return (0);
}

int TreeDataProcess::do_flush(DTCFlushRequest *flush_req, Node *p_node,
			      unsigned int &affected_count)
{
	int iRet;

	log4cplus_debug("do_flush start! ");

	rows_count_ = 0;
	dirty_rows_count_ = 0;

	iRet = m_stTreeData.do_attach(p_node->vd_handle());
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "attach data error");
		log4cplus_error("tree-data attach[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return (-1);
	}

	iRet = m_stTreeData.flush_tree_data(flush_req, p_node, affected_count);
	if (iRet != 0) {
		snprintf(err_message_, sizeof(err_message_),
			 "flush tree data error");
		log4cplus_error("tree-data flush[handle:" UINT64FMT
				"] error: %d,%s",
				p_node->vd_handle(), iRet,
				m_stTreeData.get_err_msg());
		return iRet;
	}

	dirty_rows_count_ = m_stTreeData.get_increase_dirty_row_count();

	return (0);
}

int TreeDataProcess::do_purge(DTCFlushRequest *flush_req, Node *p_node,
			      unsigned int &affected_count)
{
	int iRet;

	log4cplus_debug("do_purge start! ");

	iRet = do_flush(flush_req, p_node, affected_count);
	if (iRet != 0) {
		return (iRet);
	}
	rows_count_ = 0LL - m_stTreeData.total_rows();

	return 0;
}

int TreeDataProcess::destroy_data(Node *p_node)
{
	if (p_node->vd_handle() == INVALID_HANDLE)
		return 0;
	TreeData treeData(p_mallocator_);
	treeData.do_attach(p_node->vd_handle());
	treeData.destory();
	p_node->vd_handle() = INVALID_HANDLE;
	return 0;
}