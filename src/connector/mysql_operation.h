#ifndef __HELPER_PROCESS_H__
#define __HELPER_PROCESS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include <task_base.h>
#include "DBConn.h"
#include <dbconfig.h>
#include <buffer.h>
#include "HelperLogApi.h"


class CHelperProcess
{
private:
	int	ErrorNo;
	CDBConn	DBConn;

	class buffer sql;
	class buffer esc;

	char	LeftQuote;
	char	RightQuote;
	char	DBName[40];
	char	TableName[40];

	char	name[16];
	char	title[80];
	int	titlePrefixSize;

	CTableDefinition* TableDef;
	int	SelfGroupID;
	const CDbConfig *dbConfig;
	DBHost DBHostConf;
	
	//add by frankyang field value lengths
	unsigned long* _lengths;
	time_t	lastAccess;
	int	pingTimeout;
	unsigned int procTimeout;

protected:
	/* 将字符串printf在原来字符串的后面，如果buffer不够大会自动重新分配buffer */

	void InitTableName(const CValue *key, int FieldType);
	void InitSQLBuffer(void);
	void SQLPrintf(const char *Format, ...) __attribute__((format(printf,2,3)));
	void SQLAppendString(const char *str, int len=0);
	#define SQLAppendConst(x) SQLAppendString(x, sizeof(x)-1)
	void SQLAppendTable(void);
	void SQLAppendField(int fid);
	void SQLAppendComparator(uint8_t op);
	
	int ConfigDBByStruct(const CDbConfig* Config);
	int InternalInit(int GroupID, int bSlave);
	
	int SelectFieldConcate(const CFieldSet *Needed);
	inline int Value2Str(const CValue* Value, int iFieldType);
	inline int SetDefaultValue(int FieldType, CValue& Value);
	inline int Str2Value(char* Str, int fieldid, int FieldType, CValue& Value);
    std::string ValueToStr(const CValue *value, int fieldType);
	int ConditionConcate(const CFieldValue *Condition);
	int UpdateFieldConcate(const CFieldValue* UpdateInfo);
	int DefaultValueConcate(const CFieldValue* UpdateInfo);
	int SaveRow(CRowValue* Row, CTask* Task);
	
	void TryPing(void);
	int ProcessSelect(CTask* Task);
	int ProcessInsert(CTask* Task);
	int ProcessInsertRB(CTask* Task);
	int ProcessUpdate(CTask* Task);
	int ProcessUpdateRB(CTask* Task);
	int ProcessDelete(CTask* Task);
	int ProcessDeleteRB(CTask* Task);
	int ProcessReplace (CTask* Task);
	int ProcessReloadConfig(CTask *Task);

public:
	CHelperProcess();
	
	void UseMatchedRows(void) { DBConn.UseMatchedRows(); }
	int Init(int GroupID, const CDbConfig* Config, CTableDefinition *tdef, int slave);
	void InitPingTimeout(void);
	int CheckTable();
	
	int ProcessTask (CTask* Task);
	
	void InitTitle(int m, int t);
	void SetTitle(const char *status);
	const char *Name(void) { return name; }
	void SetProcTimeout(unsigned int Seconds) { procTimeout = Seconds; }
	
	~CHelperProcess();

	CHelperLogApi logapi;
};

#endif
