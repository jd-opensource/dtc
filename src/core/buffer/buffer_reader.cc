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

#include "task/task_pkey.h"
#include "buffer_reader.h"
#include "log/log.h"
#include "sys_malloc.h"

BufferReader::BufferReader(void) : BufferPond(NULL)
{
	pstItem = NULL;
	pstDataProcess = NULL;
	iInDirtyLRU = 1;
	notFetch = 1;
}

BufferReader::~BufferReader(void)
{
	if (pstItem != NULL)
		delete pstItem;
	pstItem = NULL;
}

int BufferReader::cache_open(int shmKey, int keySize,
			     DTCTableDefinition *p_table_definition_)
{
	int iRet;

	BlockProperties stInfo;
	memset(&stInfo, 0, sizeof(stInfo));
	stInfo.ipcMemKey = shmKey;
	stInfo.keySize = keySize;
	stInfo.readOnly = 1;

	iRet = BufferPond::cache_open(&stInfo);
	if (iRet != E_OK)
		return -1;

	pstItem = new RawData(&g_stSysMalloc, 1);
	if (pstItem == NULL) {
		snprintf(error_message, sizeof(error_message),
			 "new RawData error: %m");
		return -1;
	}

	UpdateMode stUpdateMod;
	stUpdateMod.m_iAsyncServer = MODE_SYNC;
	stUpdateMod.m_iUpdateMode = MODE_SYNC;
	stUpdateMod.m_iInsertMode = MODE_SYNC;
	stUpdateMod.m_uchInsertOrder = 0;

	if (p_table_definition_->index_fields() > 0) {
#if HAS_TREE_DATA
		pstDataProcess = new TreeDataProcess(PtMalloc::instance(),
						     p_table_definition_, this,
						     &stUpdateMod);
#else
		log4cplus_error("tree index not supported, index field num[%d]",
				p_table_definition_->index_fields());
		return -1;
#endif
	} else
		pstDataProcess = new RawDataProcess(PtMalloc::instance(),
						    p_table_definition_, this,
						    &stUpdateMod);
	if (pstDataProcess == NULL) {
		log4cplus_error("create %s error: %m",
				p_table_definition_->index_fields() > 0 ?
					"TreeDataProcess" :
					"RawDataProcess");
		return -1;
	}

	return 0;
}

int BufferReader::begin_read()
{
	stDirtyHead = dirty_lru_head();
	stClrHead = clean_lru_head();
	if (!dirty_lru_empty()) {
		iInDirtyLRU = 1;
		stCurNode = stDirtyHead;
	} else {
		iInDirtyLRU = 0;
		stCurNode = stClrHead;
	}
	return 0;
}

int BufferReader::fetch_node()
{
	pstItem->destory();
	if (!stCurNode) {
		snprintf(error_message, sizeof(error_message),
			 "begin read first!");
		return -1;
	}
	if (end()) {
		snprintf(error_message, sizeof(error_message),
			 "reach end of cache");
		return -2;
	}
	notFetch = 0;

	curRowIdx = 0;
	if (iInDirtyLRU) {
		while (stCurNode != stDirtyHead && is_time_marker(stCurNode))
			stCurNode = stCurNode.Next();
		if (stCurNode != stDirtyHead && !is_time_marker(stCurNode)) {
			if (pstDataProcess->get_node_all_rows_count(
				    &stCurNode, pstItem) != 0) {
				snprintf(error_message, sizeof(error_message),
					 "get node's data error");
				return -3;
			}
			return (0);
		}

		iInDirtyLRU = 0;
		stCurNode = stClrHead.Next();
	}

	stCurNode = stCurNode.Next();
	if (stCurNode != stClrHead) {
		if (pstDataProcess->get_node_all_rows_count(&stCurNode,
							    pstItem) != 0) {
			snprintf(error_message, sizeof(error_message),
				 "get node's data error");
			return -3;
		}
	} else {
		snprintf(error_message, sizeof(error_message),
			 "reach end of cache");
		return -2;
	}

	return (0);
}

int BufferReader::num_rows()
{
	if (pstItem == NULL)
		return (-1);

	return pstItem->total_rows();
}

int BufferReader::read_row(RowValue &row)
{
	while (notFetch || curRowIdx >= (int)pstItem->total_rows()) {
		if (fetch_node() != 0)
			return -1;
	}

	TaskPackedKey::unpack_key(row.table_definition(), pstItem->key(),
				  row.field_value(0));

	if (pstItem->decode_row(row, uchRowFlags, 0) != 0)
		return -2;

	curRowIdx++;

	return 0;
}
