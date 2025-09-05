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

#ifndef RAW_DATA_PROCESS_H
#define RAW_DATA_PROCESS_H

#include "buffer_def.h"
#include "protocol.h"
#include "value.h"
#include "field/field.h"
#include "section.h"
#include "table/table_def.h"
#include "task/task_request.h"
#include "stat_dtc.h"
#include "raw_data.h"
#include "node.h"
#include "data_process.h"
#include "buffer_pond.h"
#include "namespace.h"
#include "stat_manager.h"

DTC_BEGIN_NAMESPACE

class DTCJobOperation;
class DTCFlushRequest;

class RawDataProcess : public DataProcess {
    private:
	RawData raw_data_;
	DTCTableDefinition *p_table_;
	MallocBase *p_mallocator_;
	BufferPond *p_buffer_pond_;
	UpdateMode update_mode_;
	int64_t rows_count_;
	int64_t dirty_rows_count_;
	char err_message_[4096];

	unsigned int nodeSizeLimit; // -DEBUG-

	/*Sampling statistics for historical node data, placed in high-end memory operation management for easier convergence of statistical points, modify by tomchen 2014.08.27*/
	StatSample history_datasize;
	StatSample history_rowsize;

    protected:
	int init_data(Node *p_node, RawData *affected_data, const char *ptrKey);
	int attach_data(Node *p_node, RawData *affected_data);
	int destroy_data(Node *p_node);

    private:
	int encode_to_private_area(RawData &, RowValue &, unsigned char);

    public:
	RawDataProcess(MallocBase *pstMalloc,
		       DTCTableDefinition *p_table_definition_,
		       BufferPond *pstPool, const UpdateMode *pstUpdateMode);
	~RawDataProcess();

	void set_limit_node_size(int node_size)
	{
		nodeSizeLimit = node_size;
	} // -DEBUG-

	const char *get_err_msg()
	{
		return err_message_;
	}
	void set_insert_mode(EUpdateMode iMode)
	{
		update_mode_.m_iInsertMode = iMode;
	}
	void set_insert_order(int iOrder)
	{
		update_mode_.m_uchInsertOrder = iOrder;
	}
	void change_mallocator(MallocBase *pstMalloc);

	// expire time for cache only dtc mode
	int get_expire_time(DTCTableDefinition *t, Node *node,
			    uint32_t &expire);
	// count dirty row, cache process will use it when buffer_delete_rows in job->all_rows case
	int get_dirty_row_count(DTCJobOperation &job_op, Node *node);
	int64_t get_increase_row_count()
	{
		return rows_count_;
	}
	int64_t get_increase_dirty_row_count()
	{
		return dirty_rows_count_;
	}
	int get_node_all_rows_count(Node *p_node, RawData *pstRows);
	int expand_node(DTCJobOperation &job_op, Node *p_node);

	int do_replace_all(DTCJobOperation &job_op, Node *p_node);
	int do_replace_all(Node *p_node, RawData *new_data);
	int do_replace(DTCJobOperation &job_op, Node *p_node,
		       RawData *affected_data, bool async,
		       bool setrows = false);
	int do_delete(DTCJobOperation &job_op, Node *p_node,
		      RawData *affected_data);
	int do_get(DTCJobOperation &job_op, Node *p_node);
	int do_append(DTCJobOperation &job_op, Node *p_node,
		      RawData *affected_data, bool isDirty, bool uniq);
	int do_update(DTCJobOperation &job_op, Node *p_node,
		      RawData *affected_data, bool async, bool setrows = false);
	int do_flush(DTCFlushRequest *flush_req, Node *p_node,
		     unsigned int &affected_count);
	int do_purge(DTCFlushRequest *flush_req, Node *p_node,
		     unsigned int &affected_count);
};

DTC_END_NAMESPACE

#endif
