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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#ifndef LLONG_MAX
#define LLONG_MAX LONG_LONG_MAX
#endif
#ifndef ULLONG_MAX
#define ULLONG_MAX ULONG_LONG_MAX
#endif
// local include files
#include "database_connection.h"
// mysql include files
#include "errmsg.h"
#include "m_ctype.h"

#define STRCPY(a, s)                                                           \
    do {                                                                   \
        strncpy(a, s, sizeof(a) - 1);                                  \
        a[sizeof(a) - 1] = 0;                                          \
    } while (0)

CDBConn::CDBConn()
    : s_charac_set("")
{
    Connected = 0;
    need_free = 0;
    memset(achErr, 0, sizeof(achErr));
    db_err = 0;
    memset(&DBConfig, 0, sizeof(DBConfig));
    use_matched = 0;

    if (mysql_init(&Mysql) == NULL) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1, "mysql init error: %s",
             mysql_error(&Mysql));
    }
}

void CDBConn::use_matched_rows()
{
    use_matched = 1;
}

CDBConn::CDBConn(const DBHost *Host)
{
    Connected = 0;
    need_free = 0;
    db_err = 0;
    memset(achErr, 0, sizeof(achErr));
    memset(&DBConfig, 0, sizeof(DBConfig));
    STRCPY(DBConfig.Host, Host->Host);
    DBConfig.Port = Host->Port;
    STRCPY(DBConfig.User, Host->User);
    STRCPY(DBConfig.Password, Host->Password);
    DBConfig.ConnTimeout = Host->ConnTimeout;
    STRCPY(DBConfig.OptionFile, Host->OptionFile);

    if (mysql_init(&Mysql) == NULL) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1, "mysql init error: %s",
             mysql_error(&Mysql));
    }
}

void CDBConn::do_config(const DBHost *Host)
{
    Close();

    Connected = 0;
    db_err = 0;
    memset(achErr, 0, sizeof(achErr));
    memset(&DBConfig, 0, sizeof(DBConfig));
    STRCPY(DBConfig.Host, Host->Host);
    DBConfig.Port = Host->Port;
    STRCPY(DBConfig.User, Host->User);
    STRCPY(DBConfig.Password, Host->Password);
    DBConfig.ConnTimeout = Host->ConnTimeout;
    STRCPY(DBConfig.OptionFile, Host->OptionFile);
}

int CDBConn::get_client_version(void)
{
    return mysql_get_client_version();
}

int CDBConn::get_err_no()
{
    if (db_err >= 2000)
        return -(db_err - 100);
    return -db_err;
}

int CDBConn::get_raw_err_no()
{
    return db_err;
}

const char *CDBConn::get_err_msg()
{
    return achErr;
}

int64_t CDBConn::get_variable(const char *var)
{
    int64_t ret = -1;
    char buf[100];
    snprintf(buf, sizeof(buf), "SHOW VARIABLES LIKE '%s'", var);

    if (do_query(buf) == 0 &&
        use_result() == 0) { // query succ and got result
        if (fetch_row() == 0 &&
            !strcasecmp(Row[0], var)) { // got one row and var is match
            ret = atoll(Row[1]);
        }
        free_result();
    }

    return ret;
}

int CDBConn::Connect(const char *DBName)
{
    if (!Connected) {
        if (mysql_init(&Mysql) == NULL) {
            db_err = mysql_errno(&Mysql);
            snprintf(achErr, sizeof(achErr) - 1,
                 "mysql init error: %s", mysql_error(&Mysql));
            return (-1);
        }
        if (DBConfig.ConnTimeout != 0) {
            mysql_options(&Mysql, MYSQL_OPT_CONNECT_TIMEOUT,
                      (const char *)&(DBConfig.ConnTimeout));
        }
        int isunix = DBConfig.Host[0] == '/';
        if (DBConfig.OptionFile[0] != '\0' &&
            mysql_options(&Mysql, MYSQL_READ_DEFAULT_FILE,
                  DBConfig.OptionFile) != 0) {
            db_err = mysql_errno(&Mysql);
            snprintf(achErr, sizeof(achErr) - 1,
                 "mysql_options error: %s",
                 mysql_error(&Mysql));
            return (-2);
        }

        if (mysql_real_connect(&Mysql, isunix ? NULL : DBConfig.Host,
                       DBConfig.User, DBConfig.Password, NULL,
                       isunix ? 0 : DBConfig.Port,
                       isunix ? DBConfig.Host : NULL,
                       use_matched ? CLIENT_FOUND_ROWS : 0) ==
            NULL) {
            db_err = mysql_errno(&Mysql);
            snprintf(achErr, sizeof(achErr) - 1,
                 "mysql connect error: %s",
                 mysql_error(&Mysql));
            return (-2);
        }

        s_charac_set = (Mysql.charset != NULL && Mysql.charset->csname != NULL) 
                        ? Mysql.charset->csname : "utf8";

        Connected = 1;
    }

    if (DBName != NULL && DBName[0] != '\0') {
        if (mysql_select_db(&Mysql, DBName) != 0) {
            db_err = mysql_errno(&Mysql);
            snprintf(achErr, sizeof(achErr) - 1,
                 "mysql select_db error: %s",
                 mysql_error(&Mysql));
            Close();
            return (-3);
        }
    }

    return (0);
}

int CDBConn::Open(const char *DBName)
{
    int iRet;

    iRet = Connect(DBName);
    if (iRet != 0 && (get_err_no() == -CR_SERVER_GONE_ERROR ||
              get_err_no() == -CR_SERVER_LOST)) {
        iRet = Connect(DBName);
    }

    return (iRet);
}

int CDBConn::Open()
{
    return Connect(NULL);
}

int CDBConn::Close()
{
    if (Connected) {
        mysql_close(&Mysql);
        Connected = 0;
    }

    return (0);
}

int CDBConn::do_ping(void)
{
    int iRet = Open();
    if (iRet != 0)
        return (iRet);

    iRet = mysql_ping(&Mysql);
    if (iRet != 0) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1, "mysql ping error: %s",
             mysql_error(&Mysql));
        Close();
        return (-1);
    }

    return (0);
}

int CDBConn::do_query(const char *SQL)
{
    int iRet;

    iRet = Open();
    if (iRet != 0)
        return (iRet);

    if (mysql_real_query(&Mysql, SQL, strlen(SQL)) != 0) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1, "mysql query error: %s",
             mysql_error(&Mysql));
        Close();
        return (-1);
    }

    return (0);
}

int CDBConn::do_query(const char *DBName, const char *SQL)
{
    int iRet;

    iRet = Open(DBName);
    if (iRet != 0)
        return (iRet);

    if (mysql_real_query(&Mysql, SQL, strlen(SQL)) != 0) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1, "mysql query error: %s",
             mysql_error(&Mysql));
        Close();
        return (-1);
    }

    return (0);
}

int CDBConn::begin_work()
{
    return do_query("BEGIN WORK");
}

int CDBConn::do_commit()
{
    return do_query("COMMIT");
}

int CDBConn::roll_back()
{
    return do_query("ROLLBACK");
}

int64_t CDBConn::affected_rows()
{
    my_ulonglong RowNum;

    RowNum = mysql_affected_rows(&Mysql);
    if (RowNum < 0) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1,
             "mysql affected rows error: %s", mysql_error(&Mysql));
        Close();
        return (-1);
    }

    return ((int64_t)RowNum);
}

const char *CDBConn::result_info()
{
    return mysql_info(&Mysql);
}

uint64_t CDBConn::insert_id()
{
    my_ulonglong id;

    id = mysql_insert_id(&Mysql);
    return (uint64_t)id;
}

int CDBConn::use_result()
{
    Res = mysql_store_result(&Mysql);
    if (Res == NULL) {
        if (mysql_errno(&Mysql) != 0) {
            db_err = mysql_errno(&Mysql);
            snprintf(achErr, sizeof(achErr) - 1,
                 "mysql store result error: %s",
                 mysql_error(&Mysql));
            Close();
            return (-1);
        } else {
            res_num = 0;
            return (-1);
        }
    }

    res_num = mysql_num_rows(Res);
    if (res_num < 0) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1, "mysql num rows error: %s",
             mysql_error(&Mysql));
        mysql_free_result(Res);
        Close();
        return (-1);
    }
    need_free = 1;

    return (0);
}

int CDBConn::fetch_row()
{
    Row = mysql_fetch_row(Res);
    if (Row == NULL) {
        db_err = mysql_errno(&Mysql);
        snprintf(achErr, sizeof(achErr) - 1,
             "mysql fetch rows error: %s", mysql_error(&Mysql));
        free_result();
        Close();
        return (-1);
    }

    return (0);
}

int CDBConn::free_result()
{
    if (need_free) {
        mysql_free_result(Res);
        need_free = 0;
    }
    return (0);
}

uint32_t CDBConn::escape_string(char To[], const char *From)
{
    return mysql_real_escape_string(&Mysql, To, From, strlen(From));
}

uint32_t CDBConn::escape_string(char To[], const char *From, int Len)
{
    return mysql_real_escape_string(&Mysql, To, From, Len);
}

CDBConn::~CDBConn()
{
    if (need_free)
        free_result();

    Close();
}
