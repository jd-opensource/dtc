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

#ifndef DATA_PROCESS_H
#define DATA_PROCESS_H

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

#include "namespace.h"
DTC_BEGIN_NAMESPACE

enum EUpdateMode { MODE_SYNC = 0, MODE_ASYNC, MODE_FLUSH };

typedef struct {
	EUpdateMode m_iAsyncServer;
	EUpdateMode m_iUpdateMode;
	EUpdateMode m_iInsertMode;
	unsigned char m_uchInsertOrder;
} UpdateMode;

class DTCFlushRequest;
class DataProcess {
    public:
	DataProcess()
	{
	}
	virtual ~DataProcess()
	{
	}

	virtual const char *get_err_msg() = 0;
	virtual void set_insert_mode(EUpdateMode iMode) = 0;
	virtual void set_insert_order(int iOrder) = 0;

	virtual int64_t get_increase_row_count() = 0;
	virtual int64_t get_increase_dirty_row_count() = 0;
	virtual int get_node_all_rows_count(Node *p_node, RawData *pstRows) = 0;
	virtual int get_dirty_row_count(DTCJobOperation &job_op,
					Node *node) = 0;
	virtual int get_expire_time(DTCTableDefinition *t, Node *p_node,
				    uint32_t &expire) = 0;

	//affected_data: save data which was deleted just now temporarily.
	virtual int do_delete(DTCJobOperation &job_op, Node *p_node,
			      RawData *affected_data) = 0;
	virtual int do_get(DTCJobOperation &job_op, Node *p_node) = 0;
	virtual int do_append(DTCJobOperation &job_op, Node *p_node,
			      RawData *affected_data, bool isDirty,
			      bool uniq) = 0;
	virtual int do_replace_all(Node *p_node, RawData *new_data) = 0;
	virtual int do_replace_all(DTCJobOperation &job_op, Node *new_data) = 0;
	virtual int do_replace(DTCJobOperation &job_op, Node *p_node,
			       RawData *affected_data, bool async,
			       bool setrows = false) = 0;
	virtual int do_update(DTCJobOperation &job_op, Node *p_node,
			      RawData *affected_data, bool async,
			      bool setrows = false) = 0;
	//flush request is composed of dirty data.
	virtual int do_flush(DTCFlushRequest *flush_req, Node *p_node,
			     unsigned int &affected_count) = 0;
	virtual int do_purge(DTCFlushRequest *flush_req, Node *p_node,
			     unsigned int &affected_count) = 0;

	virtual int expand_node(DTCJobOperation &job_op, Node *p_node) = 0;
	virtual void change_mallocator(MallocBase *pstMalloc) = 0;
};

DTC_END_NAMESPACE

#endif
