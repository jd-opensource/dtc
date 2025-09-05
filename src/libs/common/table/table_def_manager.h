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
#ifndef __DTC_TABLEDEF_MANAGER_H_
#define __DTC_TABLEDEF_MANAGER_H_

#include "algorithm/singleton.h"
#include "../table/table_def.h"
#include "config/dbconfig.h"

class TableDefinitionManager {
    public:
	TableDefinitionManager();
	~TableDefinitionManager();

	static TableDefinitionManager *instance()
	{
		return Singleton<TableDefinitionManager>::instance();
	}
	static void destory()
	{
		Singleton<TableDefinitionManager>::destory();
	}

	bool set_new_table_def(DTCTableDefinition *t, int idx);
	bool set_cur_table_def(DTCTableDefinition *t, int idx);
	bool renew_cur_table_def();
	bool save_new_table_conf();

	DTCTableDefinition *get_cur_table_def();
	DTCTableDefinition *get_new_table_def();
	DTCTableDefinition *get_old_table_def();
	int get_cur_table_idx();
	DTCTableDefinition *get_table_def_by_idx(int idx);

	bool build_hot_backup_table_def();
	DTCTableDefinition *get_hot_backup_table_def();

	bool save_db_config();
	DTCTableDefinition *load_buffered_table(const char *buff);
	DTCTableDefinition *load_table(const char *file);

	DTCTableDefinition *table_file_table_def();
	const char *table_file_buffer();
	bool release_table_file_def_and_buffer();
	bool renew_table_file_def(const char *buf, int len);
	const DbConfig* get_db_config() const {
		return _dbconfig;
	};

    private:
	int _cur;
	int _new;
	DTCTableDefinition *_def[2];
	DTCTableDefinition *_hotbackup;
	// for cold start
	char *_buf;
	DTCTableDefinition *_table;
	DbConfig *_dbconfig;
	DbConfig *_save_dbconfig;
};

#endif
