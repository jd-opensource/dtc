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

#ifndef DB_CONN_H
#define DB_CONN_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <stdint.h>
// mysql include files
#define list_add my_list_add
#include "mysql.h"
#undef list_add

struct DBHost {
	char Host[110];
	int Port;
	char User[64];
	char Password[128];
	unsigned int ConnTimeout;
	char OptionFile[256];
};

class CDBConn {
    private:
	DBHost DBConfig;
	int Connected;
	MYSQL Mysql;
	char achErr[400];
	int db_err;
	int use_matched;
	std::string s_charac_set;

    public:
	MYSQL_RES *Res;
	MYSQL_ROW Row;
	int res_num;
	int need_free;

    protected:
	int Connect(const char *DBName);

    public:
	CDBConn();
	CDBConn(const DBHost *Host);

	static int get_client_version(void);
	void do_config(const DBHost *Host);
	void use_matched_rows(void);
	const char *get_err_msg();
	int get_err_no();
	int get_raw_err_no();

	int Open();
	int Open(const char *DBName);
	int Close();

	int do_ping(void);
	int do_query(const char *SQL); // connect db if needed
	int do_query(const char *DBName,
		     const char *SQL); // connect db if needed
	int begin_work();
	int do_commit();
	int roll_back();
	int64_t affected_rows();
	const char *result_info();
	uint64_t insert_id();
	uint32_t escape_string(char To[], const char *From);
	uint32_t escape_string(char To[], const char *From, int Len);
	int64_t get_variable(const char *v);

	int use_result();
	int fetch_row();
	int free_result();

	inline unsigned long *get_lengths(void)
	{
		return mysql_fetch_lengths(Res);
	}
	const std::string& GetCharacSet() const { return s_charac_set;};

	~CDBConn();
};

#endif
