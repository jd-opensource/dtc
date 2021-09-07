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

#include "database_connection.h"
#include "mysql_error.h"

#define STRCPY(a, s)                                                           \
	do {                                                                   \
		strncpy(a, s, sizeof(a) - 1);                                  \
		a[sizeof(a) - 1] = 0;                                          \
	} while (0)

CDBConn::CDBConn()
{
	Connected = 0;
	NeedFree = 0;
	memset(achErr, 0, sizeof(achErr));
	dberr = 0;
	memset(&DBConfig, 0, sizeof(DBConfig));
	useMatched = 0;

	if (mysql_init(&Mysql) == NULL) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1, "mysql init error: %s",
			 mysql_error(&Mysql));
	}
}

void CDBConn::UseMatchedRows()
{
	useMatched = 1;
}

CDBConn::CDBConn(const DBHost *Host)
{
	Connected = 0;
	NeedFree = 0;
	dberr = 0;
	memset(achErr, 0, sizeof(achErr));
	memset(&DBConfig, 0, sizeof(DBConfig));
	STRCPY(DBConfig.Host, Host->Host);
	DBConfig.Port = Host->Port;
	STRCPY(DBConfig.User, Host->User);
	STRCPY(DBConfig.Password, Host->Password);
	DBConfig.ConnTimeout = Host->ConnTimeout;
	STRCPY(DBConfig.OptionFile, Host->OptionFile);

	if (mysql_init(&Mysql) == NULL) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1, "mysql init error: %s",
			 mysql_error(&Mysql));
	}
}

void CDBConn::Config(const DBHost *Host)
{
	Close();

	Connected = 0;
	dberr = 0;
	memset(achErr, 0, sizeof(achErr));
	memset(&DBConfig, 0, sizeof(DBConfig));
	STRCPY(DBConfig.Host, Host->Host);
	DBConfig.Port = Host->Port;
	STRCPY(DBConfig.User, Host->User);
	STRCPY(DBConfig.Password, Host->Password);
	DBConfig.ConnTimeout = Host->ConnTimeout;
	STRCPY(DBConfig.OptionFile, Host->OptionFile);
}

int CDBConn::ClientVersion(void)
{
	return mysql_get_client_version();
}

int CDBConn::GetErrNo()
{
	if (dberr >= 2000)
		return -(dberr - 100);
	return -dberr;
}

int CDBConn::GetRawErrNo()
{
	return dberr;
}

const char *CDBConn::GetErrMsg()
{
	return achErr;
}

int64_t CDBConn::GetVariable(const char *var)
{
	int64_t ret = -1;
	char buf[100];
	snprintf(buf, sizeof(buf), "SHOW VARIABLES LIKE '%s'", var);

	if (Query(buf) == 0 && UseResult() == 0) { // query succ and got result
		if (FetchRow() == 0 &&
		    !strcasecmp(Row[0], var)) { // got one row and var is match
			ret = atoll(Row[1]);
		}
		FreeResult();
	}

	return ret;
}

int CDBConn::Connect(const char *DBName)
{
	if (!Connected) {
		if (mysql_init(&Mysql) == NULL) {
			dberr = mysql_errno(&Mysql);
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
			dberr = mysql_errno(&Mysql);
			snprintf(achErr, sizeof(achErr) - 1,
				 "mysql_options error: %s",
				 mysql_error(&Mysql));
			return (-2);
		}

		if (mysql_real_connect(&Mysql, isunix ? NULL : DBConfig.Host,
				       DBConfig.User, DBConfig.Password, NULL,
				       isunix ? 0 : DBConfig.Port,
				       isunix ? DBConfig.Host : NULL,
				       useMatched ? CLIENT_FOUND_ROWS : 0) ==
		    NULL) {
			dberr = mysql_errno(&Mysql);
			snprintf(achErr, sizeof(achErr) - 1,
				 "mysql connect error: %s",
				 mysql_error(&Mysql));
			return (-2);
		}

		Connected = 1;
	}

	if (DBName != NULL && DBName[0] != '\0') {
		if (mysql_select_db(&Mysql, DBName) != 0) {
			dberr = mysql_errno(&Mysql);
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
	if (iRet != 0 && (GetErrNo() == -CR_SERVER_GONE_ERROR ||
			  GetErrNo() == -CR_SERVER_LOST)) {
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

int CDBConn::Ping(void)
{
	int iRet;

	iRet = Open();
	if (iRet != 0)
		return (iRet);

	iRet = mysql_ping(&Mysql);
	if (iRet != 0) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1, "mysql ping error: %s",
			 mysql_error(&Mysql));
		Close();
		return (-1);
	}

	return (0);
}

int CDBConn::Query(const char *SQL)
{
	int iRet;

	iRet = Open();
	if (iRet != 0)
		return (iRet);

	if (mysql_real_query(&Mysql, SQL, strlen(SQL)) != 0) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1, "mysql query error: %s",
			 mysql_error(&Mysql));
		Close();
		return (-1);
	}

	return (0);
}

int CDBConn::Query(const char *DBName, const char *SQL)
{
	int iRet;

	iRet = Open(DBName);
	if (iRet != 0)
		return (iRet);

	if (mysql_real_query(&Mysql, SQL, strlen(SQL)) != 0) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1, "mysql query error: %s",
			 mysql_error(&Mysql));
		Close();
		return (-1);
	}

	return (0);
}

int CDBConn::BeginWork()
{
	return Query("BEGIN WORK");
}

int CDBConn::Commit()
{
	return Query("COMMIT");
}

int CDBConn::RollBack()
{
	return Query("ROLLBACK");
}

int64_t CDBConn::AffectedRows()
{
	my_ulonglong RowNum;

	RowNum = mysql_affected_rows(&Mysql);
	if (RowNum < 0) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1,
			 "mysql affected rows error: %s", mysql_error(&Mysql));
		Close();
		return (-1);
	}

	return ((int64_t)RowNum);
}

const char *CDBConn::ResultInfo()
{
	return mysql_info(&Mysql);
}

uint64_t CDBConn::InsertID()
{
	my_ulonglong id;

	id = mysql_insert_id(&Mysql);
	return (uint64_t)id;
}

int CDBConn::UseResult()
{
	Res = mysql_store_result(&Mysql);
	if (Res == NULL) {
		if (mysql_errno(&Mysql) != 0) {
			dberr = mysql_errno(&Mysql);
			snprintf(achErr, sizeof(achErr) - 1,
				 "mysql store result error: %s",
				 mysql_error(&Mysql));
			Close();
			return (-1);
		} else {
			ResNum = 0;
			return (-1);
		}
	}

	ResNum = mysql_num_rows(Res);
	if (ResNum < 0) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1, "mysql num rows error: %s",
			 mysql_error(&Mysql));
		mysql_free_result(Res);
		Close();
		return (-1);
	}
	NeedFree = 1;

	return (0);
}

int CDBConn::FetchRow()
{
	Row = mysql_fetch_row(Res);
	if (Row == NULL) {
		dberr = mysql_errno(&Mysql);
		snprintf(achErr, sizeof(achErr) - 1,
			 "mysql fetch rows error: %s", mysql_error(&Mysql));
		FreeResult();
		Close();
		return (-1);
	}

	return (0);
}

int CDBConn::FreeResult()
{
	if (NeedFree) {
		mysql_free_result(Res);
		NeedFree = 0;
	}
	return (0);
}

uint32_t CDBConn::EscapeString(char To[], const char *From)
{
	return mysql_real_escape_string(&Mysql, To, From, strlen(From));
}

uint32_t CDBConn::EscapeString(char To[], const char *From, int Len)
{
	return mysql_real_escape_string(&Mysql, To, From, Len);
}

CDBConn::~CDBConn()
{
	if (NeedFree)
		FreeResult();

	Close();
}
