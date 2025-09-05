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

#ifndef TREE_DATA_PROCESS_H
#define TREE_DATA_PROCESS_H

#include "buffer_def.h"
#include "protocol.h"
#include "value.h"
#include "field/field.h"
#include "section.h"
#include "table/table_def.h"
#include "task/task_request.h"
#include "stat_dtc.h"
#include "tree_data.h"
#include "node.h"
#include "data_process.h"
#include "buffer_pond.h"
#include "namespace.h"
#include "stat_manager.h"
#include "data_chunk.h"

DTC_BEGIN_NAMESPACE

class DTCJobOperation;
class DTCFlushRequest;

class TreeDataProcess : public DataProcess {
    private:
	TreeData m_stTreeData;
	DTCTableDefinition *p_table_;
	MallocBase *p_mallocator_;
	BufferPond *p_buffer_pond_;
	UpdateMode update_mode_;
	int64_t rows_count_;
	int64_t dirty_rows_count_;
	char err_message_[4096];

	unsigned int nodeSizeLimit; // -DEBUG-

	StatSample history_datasize;
	StatSample history_rowsize;

    protected:
	int attach_data(Node *p_node, RawData *affected_data);

    public:
	void change_mallocator(MallocBase *pstMalloc)
	{
		log4cplus_debug("oring mallc: %p, new mallc: %p", p_mallocator_,
				pstMalloc);
		p_mallocator_ = pstMalloc;
		m_stTreeData.change_mallocator(pstMalloc);
	}

	TreeDataProcess(MallocBase *pstMalloc,
			DTCTableDefinition *p_table_definition_,
			BufferPond *pstPool, const UpdateMode *pstUpdateMode);
	~TreeDataProcess();

	const char *get_err_msg()
	{
		return err_message_;
	}
	void set_insert_mode(EUpdateMode iMode)
	{
	}
	void set_insert_order(int iOrder)
	{
	}

	/*************************************************
    Description: get expire time
    Output:   
    *************************************************/
	int get_expire_time(DTCTableDefinition *t, Node *p_node,
			    uint32_t &expire);

	/*************************************************
    Description: 
    Output:   
    *************************************************/
	int expand_node(DTCJobOperation &job_op, Node *p_node);

	/*************************************************
    Description: 
    Output:   
    *************************************************/
	int get_dirty_row_count(DTCJobOperation &job_op, Node *p_node);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int64_t get_increase_row_count()
	{
		return rows_count_;
	};

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int64_t get_increase_dirty_row_count()
	{
		return dirty_rows_count_;
	}

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int get_node_all_rows_count(Node *p_node, RawData *pstRows);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int do_delete(DTCJobOperation &job_op, Node *p_node,
		      RawData *affected_data);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int do_replace_all(DTCJobOperation &job_op, Node *p_node);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int do_replace(DTCJobOperation &job_op, Node *p_node,
		       RawData *affected_data, bool async, bool setrows);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int do_update(DTCJobOperation &job_op, Node *p_node,
		      RawData *affected_data, bool async, bool setrows);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int do_flush(DTCFlushRequest *flush_req, Node *p_node,
		     unsigned int &affected_count);

	/*************************************************
    Description: 
    Output: 
    *************************************************/
	int do_purge(DTCFlushRequest *flush_req, Node *p_node,
		     unsigned int &affected_count);

	/*************************************************
    Description: append data in t-tree
    Output:   
    *************************************************/
	int do_append(DTCJobOperation &job_op, Node *p_node,
		      RawData *affected_data, bool isDirty, bool setrows);

	/*************************************************
    Description: replace data in t-tree
    Output:   
    *************************************************/
	int do_replace_all(Node *p_node, RawData *new_data);

	/*************************************************
    Description: get data in t-tree
    Output:   
    *************************************************/
	int do_get(DTCJobOperation &job_op, Node *p_node);

	/*************************************************
    Description: destroy t-tree
    Output:   
    *************************************************/
	int destroy_data(Node *p_node);
};

DTC_END_NAMESPACE

#endif