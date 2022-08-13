#ifndef DB_CONN_H
#define DB_CONN_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define list_add my_list_add
#include "mysql.h"
#undef list_add

struct DBHost
{
    char    Host[110];
    int     Port;
    char    User[64];
    char    Password[128];
	char	DbName[128];
	unsigned int ConnTimeout;
	unsigned int ReadTimeout;
	char	OptionFile[256];
};

class MysqlConn
{
private:
	DBHost	DBConfig;
	int	    Connected;
	MYSQL	Mysql;
	char	achErr[400];
	int	dberr;
	int	useMatched;

public:
	MYSQL_RES *Res;
	MYSQL_ROW Row;
	MYSQL_FIELD *Fields;
	int row_num;
	int field_num;
	int NeedFree;

protected:
	int Connect(const char* DBName);

public:
	MysqlConn();
	MysqlConn(const DBHost* Host);
	
	static int ClientVersion(void);
	void Config(const DBHost* Host);
	void UseMatchedRows(void);
	const char* GetErrMsg();
	int GetErrNo();
	int GetRawErrNo();
	
	int Open();
	int Open(const char* DBName);
	int Close();
	
	int Ping();
	int Query(const char* SQL); // connect db if needed
	int Query(const char* DBName, const char* SQL); // connect db if needed
	int TransactionQuery(const char* SQL);
	int BeginWork();
	int Commit();
	int RollBack();
	int64_t AffectedRows();
	const char *ResultInfo();
	uint64_t InsertID();
	uint32_t EscapeString(char To[], const char* From);
	uint32_t EscapeString(char To[], const char* From, int Len);
	int64_t GetVariable(const char *v);
	
	int UseResult();
	int FetchRow();
	int FreeResult();
	int FetchFields();

	inline unsigned long* getLengths (void)
	{
		return mysql_fetch_lengths (Res);	    
	}
	char* get_db_name() { return DBConfig.DbName;}

	~MysqlConn();
};

#endif
