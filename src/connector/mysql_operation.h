#ifndef __HELPER_PROCESS_H__
#define __HELPER_PROCESS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include <task_base.h>
#include "database_connection.h"
#include <dbconfig.h>
#include <buffer.h>

class CHelperProcess {
    private:
	int ErrorNo;
	CDBConn DBConn;

	class buffer sql;
	class buffer esc;

	char LeftQuote;
	char RightQuote;
	char DBName[40];
	char TableName[40];

	char name[16];
	char title[80];
	int titlePrefixSize;

	DTCTableDefinition *TableDef;
	int SelfGroupID;
	const DbConfig *dbConfig;
	DBHost DBHostConf;

	//add by frankyang field value lengths
	unsigned long *_lengths;
	time_t lastAccess;
	int pingTimeout;
	unsigned int procTimeout;

    protected:
	/* 将字符串printf在原来字符串的后面，如果buffer不够大会自动重新分配buffer */

	void InitTableName(const DTCValue *key, int field_type);
	void InitSQLBuffer(void);
	void SQLPrintf(const char *Format, ...)
		__attribute__((format(printf, 2, 3)));
	void SQLAppendString(const char *str, int len = 0);
#define SQLAppendConst(x) SQLAppendString(x, sizeof(x) - 1)
	void SQLAppendTable(void);
	void SQLAppendField(int fid);
	void SQLAppendComparator(uint8_t op);

	int ConfigDBByStruct(const DbConfig *Config);
	int InternalInit(int GroupID, int bSlave);

	int SelectFieldConcate(const DTCFieldSet *Needed);
	inline int Value2Str(const DTCValue *Value, int iFieldType);
	inline int SetDefaultValue(int field_type, DTCValue &Value);
	inline int Str2Value(char *Str, int fieldid, int field_type,
			     DTCValue &Value);
	std::string ValueToStr(const DTCValue *value, int fieldType);
	int ConditionConcate(const DTCFieldValue *Condition);
	int UpdateFieldConcate(const DTCFieldValue *UpdateInfo);
	int DefaultValueConcate(const DTCFieldValue *UpdateInfo);
	int SaveRow(RowValue *Row, DtcJob *Task);

	void TryPing(void);
	int ProcessSelect(DtcJob *Task);
	int ProcessInsert(DtcJob *Task);
	int ProcessInsertRB(DtcJob *Task);
	int ProcessUpdate(DtcJob *Task);
	int ProcessUpdateRB(DtcJob *Task);
	int ProcessDelete(DtcJob *Task);
	int ProcessDeleteRB(DtcJob *Task);
	int ProcessReplace(DtcJob *Task);
	int ProcessReloadConfig(DtcJob *Task);

    public:
	CHelperProcess();

	void UseMatchedRows(void)
	{
		DBConn.UseMatchedRows();
	}
	int Init(int GroupID, const DbConfig *Config, DTCTableDefinition *tdef,
		 int slave);
	void InitPingTimeout(void);
	int CheckTable();

	int ProcessTask(DtcJob *Task);

	void InitTitle(int m, int t);
	void SetTitle(const char *status);
	const char *Name(void)
	{
		return name;
	}
	void SetProcTimeout(unsigned int Seconds)
	{
		procTimeout = Seconds;
	}

	~CHelperProcess();
};

#endif
