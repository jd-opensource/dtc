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

#ifndef __CACHE_READER_H
#define __CACHE_READER_H

#include "reader_interface.h"
#include "buffer_pond.h"
#include "table/table_def.h"
#include "raw_data_process.h"

class BufferReader : public ReaderInterface, public BufferPond {
    private:
	Node stClrHead;
	Node stDirtyHead;
	int iInDirtyLRU;
	Node stCurNode;
	unsigned char uchRowFlags;
	RawData *pstItem;
	DataProcess *pstDataProcess;
	int notFetch;
	int curRowIdx;
	char error_message[200];

    public:
	BufferReader(void);
	~BufferReader(void);

	int cache_open(int shmKey, int keySize,
		       DTCTableDefinition *p_table_definition_);

	const char *err_msg()
	{
		return error_message;
	}
	int begin_read();
	int read_row(RowValue &row);
	int end();
	int key_flags(void) const
	{
		return stCurNode.is_dirty();
	}
	int key_flag_dirty(void) const
	{
		return stCurNode.is_dirty();
	}
	int row_flags(void) const
	{
		return uchRowFlags;
	}
	int row_flag_dirty(void) const
	{
		return uchRowFlags & OPER_DIRTY;
	}
	int fetch_node();
	int num_rows();
};

inline int BufferReader::end()
{
	return (iInDirtyLRU == 0) && (notFetch == 0) &&
	       (stCurNode == stClrHead);
}

#endif
