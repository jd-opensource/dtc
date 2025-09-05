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

#include <string.h>
#include <unistd.h>

#include "col_expand.h"
#include "table/table_def_manager.h"

DTC_USING_NAMESPACE

extern DTCConfig *g_dtc_config;

DTCColExpand::DTCColExpand() : col_expand_(NULL)
{
	memset(errmsg_, 0, sizeof(errmsg_));
}

DTCColExpand::~DTCColExpand()
{
}

int DTCColExpand::initialization()
{
	// alloc mem
	size_t size = sizeof(COL_EXPAND_T);
	MEM_HANDLE_T v = M_CALLOC(size);
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "init column expand failed, %s", M_ERROR());
		return DTC_CODE_FAILED;
	}
	col_expand_ = M_POINTER(COL_EXPAND_T, v);
	col_expand_->expanding = false;
	col_expand_->curTable = 0;
	memset(col_expand_->tableBuff, 0, sizeof(col_expand_->tableBuff));
	// copy file's table.conf to shm
	if (strlen(TableDefinitionManager::instance()->table_file_buffer()) >
	    COL_EXPAND_BUFF_SIZE) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "table buf size bigger than %d", COL_EXPAND_BUFF_SIZE);
		return DTC_CODE_FAILED;
	}
	strcpy(col_expand_
		       ->tableBuff[col_expand_->curTable % COL_EXPAND_BUFF_NUM],
	       TableDefinitionManager::instance()->table_file_buffer());
	// use file's tabledef
	DTCTableDefinition *t =
		TableDefinitionManager::instance()->table_file_table_def();
	TableDefinitionManager::instance()->set_cur_table_def(
		t, col_expand_->curTable % COL_EXPAND_BUFF_NUM);
	log4cplus_debug("init col expand with curTable: %d, tableBuff: %s",
			col_expand_->curTable,
			col_expand_->tableBuff[col_expand_->curTable %
					       COL_EXPAND_BUFF_NUM]);
	return DTC_CODE_SUCCESS;
}

int DTCColExpand::reload_table()
{
	log4cplus_debug("reload table entry");
	if (TableDefinitionManager::instance()->get_cur_table_idx() ==
	    col_expand_->curTable)
		return DTC_CODE_SUCCESS;

	DTCTableDefinition *t =
		TableDefinitionManager::instance()->load_buffered_table(
			col_expand_->tableBuff[col_expand_->curTable %
					       COL_EXPAND_BUFF_NUM]);
	if (!t) {
		log4cplus_error("load shm table.yaml error, buf: %s",
				col_expand_->tableBuff[col_expand_->curTable %
						       COL_EXPAND_BUFF_NUM]);
		return DTC_CODE_FAILED;
	}
	TableDefinitionManager::instance()->set_cur_table_def(
		t, col_expand_->curTable);

	log4cplus_debug("reload table leave");
	return DTC_CODE_SUCCESS;
}

int DTCColExpand::attach(MEM_HANDLE_T handle, int forceFlag)
{
	if (INVALID_HANDLE == handle) {
		log4cplus_error("attch col expand error, handle = 0");
		return DTC_CODE_FAILED;
	}
	col_expand_ = M_POINTER(COL_EXPAND_T, handle);
	// 1) force update shm mem, 2)replace shm mem by dumped mem
	if (forceFlag) {
		log4cplus_debug("force use table.yaml, not use shm conf");
		if (strlen(TableDefinitionManager::instance()
				   ->table_file_buffer()) >
		    COL_EXPAND_BUFF_SIZE) {
			log4cplus_error(
				"table.yaml to long while force update shm");
			return DTC_CODE_FAILED;
		}
		if (col_expand_->expanding) {
			log4cplus_error(
				"col expanding, can't force update table.yaml, delete shm and try again");
			return DTC_CODE_FAILED;
		}
		strcpy(col_expand_->tableBuff[col_expand_->curTable %
					      COL_EXPAND_BUFF_NUM],
		       TableDefinitionManager::instance()->table_file_buffer());
		DTCTableDefinition *t = TableDefinitionManager::instance()
						->table_file_table_def();
		TableDefinitionManager::instance()->set_cur_table_def(
			t, col_expand_->curTable);
		return DTC_CODE_SUCCESS;
	}
	// parse shm table.conf
	DTCTableDefinition *t, *tt = NULL;
	t = TableDefinitionManager::instance()->load_buffered_table(
		col_expand_->tableBuff[col_expand_->curTable %
				       COL_EXPAND_BUFF_NUM]);
	if (!t) {
		log4cplus_error("load shm table.yaml error, buf: %s",
				col_expand_->tableBuff[col_expand_->curTable %
						       COL_EXPAND_BUFF_NUM]);
		return DTC_CODE_FAILED;
	}
	if (col_expand_->expanding) {
		tt = TableDefinitionManager::instance()->load_buffered_table(
			col_expand_->tableBuff[(col_expand_->curTable + 1) %
					       COL_EXPAND_BUFF_NUM]);
		if (!tt) {
			log4cplus_error(
				"load shm col expand new table.yaml error, buf: %s",
				col_expand_->tableBuff[(col_expand_->curTable +
							1) %
						       COL_EXPAND_BUFF_NUM]);
			return DTC_CODE_FAILED;
		}
	}
	// compare
	// if not same
	// 		log4cplus_error
	if (!t->is_same_table(
		    TableDefinitionManager::instance()
			    ->table_file_table_def())) { // same with hash_equal
		DTCTableDefinition *tt = TableDefinitionManager::instance()
						 ->table_file_table_def();
		log4cplus_error("table.yaml is not same to shm's");
		log4cplus_error("shm table, name: %s, hash: %s",
				t->table_name(), t->table_hash());
		log4cplus_error("file table, name: %s, hash: %s",
				tt->table_name(), tt->table_hash());
	} else {
		log4cplus_debug("table.yaml is same to shm's");
	}
	// use shm's
	TableDefinitionManager::instance()->set_cur_table_def(
		t, col_expand_->curTable);
	if (col_expand_->expanding)
		TableDefinitionManager::instance()->set_new_table_def(
			tt, col_expand_->curTable + 1);
	return DTC_CODE_SUCCESS;
}

bool DTCColExpand::is_expanding()
{
	return col_expand_->expanding;
}

bool DTCColExpand::expand(const char *table, int len)
{
	col_expand_->expanding = true;
	memcpy(col_expand_->tableBuff[(col_expand_->curTable + 1) %
				      COL_EXPAND_BUFF_NUM],
	       table, len);
	return true;
}

int DTCColExpand::try_expand(const char *table, int len)
{
	if (col_expand_->expanding || len > COL_EXPAND_BUFF_SIZE ||
	    col_expand_->curTable > 255)
		return DTC_CODE_FAILED;
	return DTC_CODE_SUCCESS;
}

bool DTCColExpand::expand_done()
{
	++col_expand_->curTable;
	col_expand_->expanding = false;
	return true;
}

int DTCColExpand::cur_table_idx()
{
	return col_expand_->curTable;
}
