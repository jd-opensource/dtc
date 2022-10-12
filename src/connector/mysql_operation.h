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
* 
*/
#ifndef __HELPER_PROCESS_H__
#define __HELPER_PROCESS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
// local include files
#include "database_connection.h"
// common include files
#include "task/task_base.h"
#include "config/dbconfig.h"
#include "buffer.h"

class ConnectorProcess {
    private:
	int error_no;
	CDBConn db_conn;

	class buffer sql;
	class buffer esc;

	char left_quote;
	char right_quote;
	char DBName[40];
	char table_name[40];

	char name[16];
	char title[80];
	int title_prefix_size;

	DTCTableDefinition *table_def;
	int self_group_id;
	const DbConfig *dbConfig;
	DBHost db_host_conf;

	unsigned long *_lengths;
	time_t last_access;
	int ping_timeout;
	unsigned int proc_timeout;

    protected:
	/* 将字符串printf在原来字符串的后面，如果buffer不够大会自动重新分配buffer */

	void init_table_name(const DTCValue *key, int field_type);
	void init_sql_buffer(void);
	void sql_printf(const char *Format, ...)
		__attribute__((format(printf, 2, 3)));
	void sql_append_string(const char *str, int len = 0);
#define sql_append_const(x) sql_append_string(x, sizeof(x) - 1)
	void sql_append_table(void);
	void sql_append_field(int fid);
	void sql_append_comparator(uint8_t op);

	int config_db_by_struct(const DbConfig *do_config);
	int machine_init(int GroupID, int bSlave);

	int select_field_concate(const DTCFieldSet *Needed);
	inline int format_sql_value(const DTCValue *Value, int iFieldType);
	inline int set_default_value(int field_type, DTCValue &Value);
	inline int str_to_value(char *Str, int fieldid, int field_type,
				DTCValue &Value);
	std::string value_to_str(const DTCValue *value, int fieldType);
	int condition_concate(const DTCFieldValue *Condition);
	int update_field_concate(const DTCFieldValue *UpdateInfo);
	int default_value_concate(const DTCFieldValue *UpdateInfo);
	int save_row(RowValue *Row, DtcJob *Task);

	
	int process_select(DtcJob *Task);
	int process_insert(DtcJob *Task);
	int process_insert_rb(DtcJob *Task);
	int process_update(DtcJob *Task);
	int process_update_rb(DtcJob *Task);
	int process_delete(DtcJob *Task);
	int process_delete_rb(DtcJob *Task);
	int process_replace(DtcJob *Task);
 	int process_reload_config(DtcJob *Task);
public:
	ConnectorProcess();

	void use_matched_rows(void)
	{
		db_conn.use_matched_rows();
	}
	int do_init(int GroupID, const DbConfig *do_config,
		    DTCTableDefinition *tdef, int slave);

	int try_ping(void);
	int create_tab_if_not_exist();

	void init_ping_timeout(void);
	int check_table();

	int do_process(DtcJob *Task);

	void init_title(int m, int t);
	void set_title(const char *status);
	const char *get_name(void)
	{
		return name;
	}
	void set_proc_timeout(unsigned int secs)
	{
		proc_timeout = secs;
	}

	int process_statement_query(const DTCValue* key, std::string& s_sql);
	~ConnectorProcess();
};

#endif
