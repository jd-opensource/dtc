/* Copyright (c) 2015, 2021, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */
#ifndef PLUGIN_PROTOCOL_INCLUDED
#define PLUGIN_PROTOCOL_INCLUDED

#ifndef MYSQL_ABI_CHECK
//#include "field_types.h" /* enum_field_types */
//#include "mysql_com.h"   /* mysql_enum_shutdown_level */
#endif

#define NULL_LENGTH ((unsigned long)~0) /* For net_store_length */
#define MYSQL_STMT_HEADER 4
#define MYSQL_LONG_DATA_HEADER 6

#define HOSTNAME_LENGTH 60
#define SYSTEM_CHARSET_MBMAXLEN 3
#define NAME_CHAR_LEN 64 /* Field/table name length */
#define USERNAME_CHAR_LENGTH 16
#define NAME_LEN (NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN)
#define USERNAME_LENGTH (USERNAME_CHAR_LENGTH * SYSTEM_CHARSET_MBMAXLEN)

/**
  @file include/mysql/com_data.h
  Definition of COM_DATA to be used with the Command service as data input
  structure.
*/

struct COM_INIT_DB_DATA {
	const char *db_name;
	unsigned long length;
};

struct COM_REFRESH_DATA {
	unsigned char options;
};

struct COM_KILL_DATA {
	unsigned long id;
};

struct COM_SET_OPTION_DATA {
	unsigned int opt_command;
};

struct COM_STMT_EXECUTE_DATA {
	unsigned long stmt_id;
	/** This holds the flags, as defined in @ref enum_cursor_type */
	unsigned long open_cursor;
	unsigned long parameter_count;
	unsigned char has_new_types;
};

struct COM_STMT_FETCH_DATA {
	unsigned long stmt_id;
	unsigned long num_rows;
};

struct COM_STMT_SEND_LONG_DATA_DATA {
	unsigned long stmt_id;
	unsigned int param_number;
	unsigned char *longdata;
	unsigned long length;
};

struct COM_STMT_PREPARE_DATA {
	const char *query;
	unsigned int length;
};

struct COM_STMT_CLOSE_DATA {
	unsigned int stmt_id;
};

struct COM_STMT_RESET_DATA {
	unsigned int stmt_id;
};

struct COM_QUERY_DATA {
	const char *query;
	unsigned int length;
	unsigned long parameter_count;
};

struct COM_FIELD_LIST_DATA {
	unsigned char *table_name;
	unsigned int table_name_length;
	const unsigned char *query;
	unsigned int query_length;
};

union COM_DATA {
	struct COM_INIT_DB_DATA com_init_db;
	struct COM_REFRESH_DATA com_refresh;
	struct COM_KILL_DATA com_kill;
	struct COM_SET_OPTION_DATA com_set_option;
	struct COM_STMT_EXECUTE_DATA com_stmt_execute;
	struct COM_STMT_FETCH_DATA com_stmt_fetch;
	struct COM_STMT_SEND_LONG_DATA_DATA com_stmt_send_long_data;
	struct COM_STMT_PREPARE_DATA com_stmt_prepare;
	struct COM_STMT_CLOSE_DATA com_stmt_close;
	struct COM_STMT_RESET_DATA com_stmt_reset;
	struct COM_QUERY_DATA com_query;
	struct COM_FIELD_LIST_DATA com_field_list;
};

#endif /* PLUGIN_PROTOCOL_INCLUDED */
