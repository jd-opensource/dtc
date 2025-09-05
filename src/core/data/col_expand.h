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

#ifndef __DTC_COL_EXPAND_H_
#define __DTC_COL_EXPAND_H_

#include "namespace.h"
#include "global.h"
#include "algorithm/singleton.h"

DTC_BEGIN_NAMESPACE

#define COL_EXPAND_BUFF_SIZE (1024 * 1024)
#define COL_EXPAND_BUFF_NUM 2

struct _col_expand {
	bool expanding;
	unsigned char curTable;
	char tableBuff[COL_EXPAND_BUFF_NUM][COL_EXPAND_BUFF_SIZE];
};
typedef struct _col_expand COL_EXPAND_T;

class DTCColExpand {
    public:
	DTCColExpand();
	~DTCColExpand();

	static DTCColExpand *instance()
	{
		return Singleton<DTCColExpand>::instance();
	}
	static void destroy()
	{
		Singleton<DTCColExpand>::destory();
	}

	int initialization();
	int attach(MEM_HANDLE_T handle, int forceFlag);

	bool is_expanding();
	bool expand(const char *table, int len);
	int try_expand(const char *table, int len);
	bool expand_done();
	int cur_table_idx();
	int reload_table();

	const MEM_HANDLE_T get_handle() const
	{
		return M_HANDLE(col_expand_);
	}
	const char *error() const
	{
		return errmsg_;
	}

    private:
	COL_EXPAND_T *col_expand_;
	char errmsg_[256];
};

DTC_END_NAMESPACE

#endif
