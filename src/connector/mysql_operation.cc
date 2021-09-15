#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <map>
#include <string>

#include "mysql_operation.h"
#include <mysqld_error.h>
#include <protocol.h>
#include <log.h>

#include "proc_title.h"
#include "table/table_def_manager.h"

#include "buffer/buffer_pond.h"
#include <daemon/daemon.h>

#define MIN(x, y) ((x) <= (y) ? (x) : (y))

CHelperProcess::CHelperProcess() : _lengths(0)
{
	ErrorNo = 0;

	LeftQuote = '`';
	RightQuote = '`';

	titlePrefixSize = 0;
	time(&lastAccess);
	pingTimeout = 9;
	procTimeout = 0;
	strncpy(name, "helper", 6);
}

void CHelperProcess::TryPing(void)
{
	time_t now;
	time(&now);
	if ((int)(now - lastAccess) >= pingTimeout)
		DBConn.Ping();
	lastAccess = now;
}

void CHelperProcess::InitPingTimeout(void)
{
	int64_t to = DBConn.GetVariable("wait_timeout");
	log4cplus_debug("Server idle timeout %lld", (long long)to);
	if (to < 10)
		to = 10;
	else if (to > 600)
		to = 600;
	pingTimeout = to * 9 / 10;
}

int CHelperProcess::ConfigDBByStruct(const DbConfig *cf)
{
	if (cf == NULL)
		return -1;
	dbConfig = cf;
	return (0);
}

#define DIM(a) (sizeof(a) / sizeof(a[0]))
static int GetFieldType(const char *szType, int &iType, unsigned int &uiSize)
{
	unsigned int i;
	int iTmp;
	typedef struct {
		char m_szName[256];
		int m_iType;
		int m_uiSize;
	} CMysqlField;
	static CMysqlField astField[] = { { "tinyint", 1, 1 },
					  { "smallint", 1, 2 },
					  { "mediumint", 1, 4 },
					  { "int", 1, 4 },
					  { "bigint", 1, 8 },
					  { "float", 3, 4 },
					  { "double", 3, 8 },
					  { "decimal", 3, 8 },
					  { "datetime", 4, 20 },
					  { "date", 4, 11 },
					  { "timestamp", 4, 20 },
					  { "time", 4, 11 },
					  { "year", 4, 5 },
					  { "varchar", 4, 255 },
					  { "char", 4, 255 },
					  { "varbinary", 5, 255 },
					  { "binary", 5, 255 },
					  { "tinyblob", 5, 255 },
					  { "tinytext", 4, 255 },
					  { "blob", 5, 65535 },
					  { "text", 4, 65535 },
					  { "mediumblob", 5, 16777215 },
					  { "mediumtext", 4, 16777215 },
					  { "longblob", 5, 4294967295U },
					  { "longtext", 4, 4294967295U },
					  { "enum", 4, 255 },
					  { "set", 2, 8 } };

	for (i = 0; i < DIM(astField); i++) {
		if (strncasecmp(szType, astField[i].m_szName,
				strlen(astField[i].m_szName)) == 0) {
			iType = astField[i].m_iType;
			uiSize = astField[i].m_uiSize;
			if (strncasecmp(szType, "varchar", 7) == 0) {
				if (sscanf(szType + 8, "%d", &iTmp) == 1)
					uiSize = iTmp;
			} else if (strncasecmp(szType, "char", 4) == 0) {
				if (sscanf(szType + 5, "%d", &iTmp) == 1)
					uiSize = iTmp;
			} else if (strncasecmp(szType, "varbinary", 9) == 0) {
				if (sscanf(szType + 10, "%d", &iTmp) == 1)
					uiSize = iTmp;
			} else if (strncasecmp(szType, "binary", 6) == 0) {
				if (sscanf(szType + 7, "%d", &iTmp) == 1)
					uiSize = iTmp;
			}
			if (iType == 1 && strstr(szType, "unsigned") != NULL)
				iType = 2;

			if (iType == 3 && strstr(szType, "unsigned") != NULL)
				fprintf(stderr,
					"#warning: ttc not support unsigned double!\n");

			break;
		}
	}

	return (0);
}

int CHelperProcess::CheckTable()
{
	int Ret;
	int i;
	int iFieldNum;
	char achFieldName[256][256];

	snprintf(DBName, sizeof(DBName), dbConfig->dbFormat,
		 dbConfig->mach[SelfGroupID].dbIdx[0]);
	snprintf(TableName, sizeof(TableName), dbConfig->tblFormat, 0);

	InitSQLBuffer();
	SQLAppendConst("show columns from `");
	SQLAppendString(TableName);
	SQLAppendConst("`");

	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	Ret = DBConn.Query(DBName, sql.c_str());
	log4cplus_debug("SELECT %d %s", Ret, DBConn.GetRawErrNo());
	if (Ret != 0) {
		log4cplus_warning("db query error: %s, pid: %d, group-id: %d",
				  DBConn.GetErrMsg(), getpid(), SelfGroupID);
		return (-1);
	}

	Ret = DBConn.UseResult();
	if (Ret != 0) {
		log4cplus_warning("db user result error: %s",
				  DBConn.GetErrMsg());
		return (-2);
	}

	// 获取返回结果的各列位置
	int iNameIdx = 0, iTypeIdx = 0;
	int iNullIdx = 0, iKeyIdx = 0;
	int iDefaultIdx = 0, iExtraIdx = 0;
	unsigned int uiNumFields = mysql_num_fields(DBConn.Res);
	MYSQL_FIELD *pstFields = mysql_fetch_fields(DBConn.Res);
	for (i = 0; i < (int)uiNumFields; i++) {
		if (strcasecmp("Field", pstFields[i].name) == 0)
			iNameIdx = i;
		else if (strcasecmp("Type", pstFields[i].name) == 0)
			iTypeIdx = i;
		else if (strcasecmp("Null", pstFields[i].name) == 0)
			iNullIdx = i;
		else if (strcasecmp("Key", pstFields[i].name) == 0)
			iKeyIdx = i;
		else if (strcasecmp("Default", pstFields[i].name) == 0)
			iDefaultIdx = i;
		else if (strcasecmp("Extra", pstFields[i].name) == 0)
			iExtraIdx = i;
	}

	int iFid;
	iFieldNum = 0;
	memset(achFieldName, 0, sizeof(achFieldName));

	int uniq_fields_cnt_table = TableDef->uniq_fields();
	for (i = 0; i < DBConn.ResNum; i++) {
		Ret = DBConn.FetchRow();

		if (Ret != 0) {
			DBConn.FreeResult();
			log4cplus_warning("db fetch row error: %s",
					  DBConn.GetErrMsg());
			return (-3);
		}

		strncpy(achFieldName[iFieldNum], DBConn.Row[iNameIdx], 255);
		iFieldNum++;

		iFid = TableDef->field_id(DBConn.Row[iNameIdx]);
		if (iFid == -1) {
			log4cplus_debug("field[%s] not found in table.conf",
					DBConn.Row[iNameIdx]);
			continue;
		}

		if (TableDef->is_volatile(iFid)) {
			log4cplus_error(
				"field[name: `%s`] found in table.conf and DB both, can't be Volatile",
				DBConn.Row[iNameIdx]);
			DBConn.FreeResult();
			return (-4);
		}

		if (TableDef->is_timestamp(iFid)) {
			log4cplus_error(
				"in table.conf, Field[name: `%s`]'s is timestamp, not support in DB mode",
				DBConn.Row[iNameIdx]);
			DBConn.FreeResult();
			return (-4);
		}

		//field type & size
		int iType = -1;
		unsigned uiSize = 0;
		GetFieldType(DBConn.Row[iTypeIdx], iType, uiSize);
		if (iType != TableDef->field_type(iFid)) {
			log4cplus_error(
				"in table.conf, Field[name: `%s`]'s type incorrect. conf: %d, mysql:%d",
				DBConn.Row[iNameIdx],
				TableDef->field_type(iFid), iType);
			DBConn.FreeResult();
			return (-4);
		}

		if ((int)uiSize != TableDef->field_size(iFid) &&
		    !(uiSize >= (64 << 20) &&
		      TableDef->field_size(iFid) >= (64 << 20))) {
			log4cplus_error(
				"in table.conf, Field[name: `%s`]'s size incorrect. conf: %d, mysql:%u",
				DBConn.Row[iNameIdx],
				TableDef->field_size(iFid), uiSize);
			DBConn.FreeResult();
			return (-4);
		}

		if (DBConn.Row[iExtraIdx] != NULL &&
		    strcasecmp("auto_increment", DBConn.Row[iExtraIdx]) == 0) {
			if (TableDef->auto_increment_field_id() != iFid) {
				log4cplus_error(
					"in table.conf, Field[name: `%s`]'s default-value incorrect. conf: non-auto_increment, mysql:auto_increment",
					DBConn.Row[iNameIdx]);
				DBConn.FreeResult();
				return (-4);
			}
		}

		/*field should be uniq in table.conf if configed primary in db -- by newman*/
		uint8_t *uniq_fields = TableDef->uniq_fields_list();
		if (DBConn.Row[iKeyIdx] != NULL &&
		    (strcasecmp("PRI", DBConn.Row[iKeyIdx]) == 0 ||
		     strcasecmp("UNI", DBConn.Row[iKeyIdx]) == 0)) {
			int j = 0;
			for (j = 0; j < TableDef->uniq_fields(); j++) {
				if (uniq_fields[j] == iFid)
					break;
			}

			if (j >= TableDef->uniq_fields()) {
				log4cplus_error(
					"in table.conf, Field[name: `%s`] is primary in db, but not uniq in ttc",
					DBConn.Row[iNameIdx]);
				return -4;
			}

			uniq_fields_cnt_table--;
		}
	}

	/*field should be primary in db if configed uniq in table.conf -- by newman*/
	if (uniq_fields_cnt_table != 0) {
		log4cplus_error(
			"table.conf have more uniq fields that not configed as primary in db");
		return -4;
	}

	for (int i = 0; i <= TableDef->num_fields(); i++) {
		//bug fix volatile不在db中
		if (TableDef->is_volatile(i))
			continue;

		const char *name = TableDef->field_name(i);
		int j;
		for (j = 0; j < iFieldNum; j++) {
			if (strcmp(achFieldName[j], name) == 0)
				break;
		}
		if (j >= iFieldNum) {
			log4cplus_error(
				"in table.conf, Field[name: `%s`] not found in mysql",
				name);
			DBConn.FreeResult();
			return (-4);
		}
	}

	log4cplus_debug(
		"pid: %d, group-id: %d check table success, db: %s, sql: %s",
		getpid(), SelfGroupID, DBName, sql.c_str());

	DBConn.FreeResult();

	return (0);
}

int CHelperProcess::InternalInit(int GroupID, int r)
{
	const char *p;

	// 初始化db配置信息
	if (dbConfig->machineCnt <= GroupID) {
		log4cplus_error(
			"parse config error, machineCnt[%d] <= GroupID[%d]",
			dbConfig->machineCnt, GroupID);
		return (-3);
	}

	typeof(&dbConfig->mach[0].role[0]) role =
		&dbConfig->mach[GroupID].role[r];

	memset(&DBHostConf, 0, sizeof(DBHost));
	p = strrchr(role->addr, ':');
	if (p == NULL) {
		strncpy(DBHostConf.Host, role->addr,
			sizeof(DBHostConf.Host) - 1);
		DBHostConf.Port = 0;
	} else {
		strncpy(DBHostConf.Host, role->addr,
			MIN(p - role->addr, (int)sizeof(DBHostConf.Host) - 1));
		DBHostConf.Port = atoi(p + 1);
	}
	strncpy(DBHostConf.User, role->user, sizeof(DBHostConf.User) - 1);
	strncpy(DBHostConf.Password, role->pass,
		sizeof(DBHostConf.Password) - 1);
	DBHostConf.ConnTimeout = procTimeout;
	strncpy(DBHostConf.OptionFile, role->optfile,
		sizeof(DBHostConf.OptionFile) - 1);

	DBConn.Config(&DBHostConf);

	if (DBConn.Open() != 0) {
		log4cplus_warning("connect db[%s] error: %s", DBHostConf.Host,
				  DBConn.GetErrMsg());
		return (-6);
	}

	log4cplus_debug("group-id: %d, pid: %d, db: %s, user: %s, pwd: %s",
			SelfGroupID, getpid(), DBHostConf.Host, DBHostConf.User,
			DBHostConf.Password);

	return (0);
}

int CHelperProcess::Init(int GroupID, const DbConfig *Config,
			 DTCTableDefinition *tdef, int slave)
{
	int Ret;

	SelfGroupID = GroupID;
	TableDef = tdef;

	Ret = ConfigDBByStruct(Config);
	if (Ret != 0) {
		return (-1);
	}

	Ret = InternalInit(GroupID, slave);
	if (Ret != 0) {
		return (-2);
	}

	return (0);
}

void CHelperProcess::InitSQLBuffer(void)
{
	sql.clear();
	ErrorNo = 0;
}

void CHelperProcess::SQLAppendString(const char *str, int len)
{
	if (len == 0)
		len = strlen(str);
	if (sql.append(str, len) < 0) {
		ErrorNo = -1;
		log4cplus_error("sql.append() error: %d, %m", sql.needed());
	}
}

/* 将字符串printf在原来字符串的后面，如果buffer不够大会自动重新分配buffer */
void CHelperProcess::SQLPrintf(const char *Format, ...)
{
	va_list Arg;
	int Len;

	va_start(Arg, Format);
	Len = sql.vbprintf(Format, Arg);
	va_end(Arg);
	if (Len < 0) {
		ErrorNo = -1;
		log4cplus_error("vsnprintf error: %d, %m", Len);
	}
}

void CHelperProcess::SQLAppendTable(void)
{
	SQLAppendString(&LeftQuote, 1);
	SQLAppendString(TableName);
	SQLAppendString(&RightQuote, 1);
}

void CHelperProcess::SQLAppendField(int fid)
{
	SQLAppendString(&LeftQuote, 1);
	SQLAppendString(TableDef->field_name(fid));
	SQLAppendString(&RightQuote, 1);
}

void CHelperProcess::SQLAppendComparator(uint8_t op)
{
	// order is important
	static const char *const CompStr[] = { "=", "!=", "<", "<=", ">", ">=" };
	if (op >= DField::TotalComparison) {
		ErrorNo = -1;
		log4cplus_error("unknow op: %d", op);
	} else {
		SQLAppendString(CompStr[op]);
	}
}

void CHelperProcess::InitTableName(const DTCValue *Key, int field_type)
{
	int dbid = 0, tableid = 0;
	uint64_t n;
	double f;

	if (Key != NULL && dbConfig->depoly != 0) {
		switch (field_type) {
		case DField::Signed:
			if (dbConfig->keyHashConfig.keyHashEnable) {
				n = dbConfig->keyHashConfig.keyHashFunction(
					(const char *)&(Key->s64),
					sizeof(Key->s64),
					dbConfig->keyHashConfig.keyHashLeftBegin,
					dbConfig->keyHashConfig
						.keyHashRightBegin);
			} else {
				if (Key->s64 >= 0)
					n = Key->s64;
				else if (Key->s64 == LONG_LONG_MIN)
					n = 0;
				else
					n = 0 - Key->s64;
			}

			dbid = (n / dbConfig->dbDiv) % dbConfig->dbMod;
			tableid = (n / dbConfig->tblDiv) % dbConfig->tblMod;
			break;

		case DField::Unsigned:
			if (dbConfig->keyHashConfig.keyHashEnable) {
				n = dbConfig->keyHashConfig.keyHashFunction(
					(const char *)&(Key->u64),
					sizeof(Key->u64),
					dbConfig->keyHashConfig.keyHashLeftBegin,
					dbConfig->keyHashConfig
						.keyHashRightBegin);
			} else {
				n = Key->u64;
			}
			dbid = (n / dbConfig->dbDiv) % dbConfig->dbMod;
			tableid = (n / dbConfig->tblDiv) % dbConfig->tblMod;
			break;

		case DField::Float:
			if (dbConfig->keyHashConfig.keyHashEnable) {
				n = dbConfig->keyHashConfig.keyHashFunction(
					(const char *)&(Key->flt),
					sizeof(Key->flt),
					dbConfig->keyHashConfig.keyHashLeftBegin,
					dbConfig->keyHashConfig
						.keyHashRightBegin);

				dbid = (n / dbConfig->dbDiv) % dbConfig->dbMod;
				tableid = (n / dbConfig->tblDiv) %
					  dbConfig->tblMod;
			} else {
				if (Key->flt >= 0)
					f = Key->flt;
				else
					f = 0 - Key->flt;

				dbid = ((int)(f / dbConfig->dbDiv)) %
				       dbConfig->dbMod;
				tableid = ((int)(f / dbConfig->tblDiv)) %
					  dbConfig->tblMod;
			}
			break;

		case DField::String:
		case DField::Binary:
			if (dbConfig->keyHashConfig.keyHashEnable) {
				n = dbConfig->keyHashConfig.keyHashFunction(
					Key->bin.ptr, Key->bin.len,
					dbConfig->keyHashConfig.keyHashLeftBegin,
					dbConfig->keyHashConfig
						.keyHashRightBegin);

				dbid = (n / dbConfig->dbDiv) % dbConfig->dbMod;
				tableid = (n / dbConfig->tblDiv) %
					  dbConfig->tblMod;
			}
			break;
		}
	}

	snprintf(DBName, sizeof(DBName), dbConfig->dbFormat, dbid);
	snprintf(TableName, sizeof(TableName), dbConfig->tblFormat, tableid);
}

int CHelperProcess::SelectFieldConcate(const DTCFieldSet *fs)
{
	if (fs == NULL) {
		SQLAppendConst("COUNT(*)");
	} else {
		int i = 0;
		uint8_t mask[32];

		FIELD_ZERO(mask);
		fs->build_field_mask(mask);
		SQLAppendField(0); // key

		for (i = 1; i < TableDef->num_fields() + 1; i++) {
			SQLAppendConst(",");
			if (FIELD_ISSET(i, mask) == 0) {
				/* Missing field as 0 */
				SQLAppendConst("0");
			} else if (TableDef->is_volatile(i) == 0) {
				SQLAppendField(i);
			} else {
				// volatile field initialized as default value
				Value2Str(TableDef->default_value(i),
					  TableDef->field_type(i));
			}
		}
	}
	return 0;
}

std::string CHelperProcess::ValueToStr(const DTCValue *v, int fieldType)
{
	if (v == NULL)
		return "NULL";

	char buf[32];
	std::string ret;

	switch (fieldType) {
	case DField::Signed:
		snprintf(buf, sizeof(buf), "%lld", (long long)v->s64);
		return buf;
	case DField::Unsigned:
		snprintf(buf, sizeof(buf), "%llu", (unsigned long long)v->u64);
		return buf;
	case DField::Float:
		snprintf(buf, sizeof(buf), "%f", v->flt);
		return buf;
	case DField::String:
	case DField::Binary:
		esc.clear();
		if (esc.expand(v->str.len * 2 + 1) < 0) {
			ErrorNo = -1;
			log4cplus_error("realloc (size: %u) error: %m",
					v->str.len * 2 + 1);
			return "NULL";
		}
		DBConn.EscapeString(esc.c_str(), v->str.ptr,
				    v->str.len); // 先对字符串进行escape
		ret = '\'';
		ret += esc.c_str();
		ret += "\'";
		return ret;
	default:
		ErrorNo = -1;
		log4cplus_error("unknown field type: %d", fieldType);
		return "UNKNOWN";
	}
}

inline int CHelperProcess::Value2Str(const DTCValue *Value, int iFieldType)
{
	log4cplus_debug("Value2Str iFieldType[%d]", iFieldType);

	if (Value == NULL) {
		SQLAppendConst("NULL");
	} else
		switch (iFieldType) {
		case DField::Signed:
			SQLPrintf("%lld", (long long)Value->s64);
			break;

		case DField::Unsigned:
			SQLPrintf("%llu", (unsigned long long)Value->u64);
			break;

		case DField::Float:
			SQLPrintf("'%f'", Value->flt);
			break;

		case DField::String:
		case DField::Binary:
			if (sql.append('\'') < 0)
				ErrorNo = -1;
			if (!Value->str.is_empty()) {
				esc.clear();
				if (esc.expand(Value->str.len * 2 + 1) < 0) {
					ErrorNo = -1;
					log4cplus_error(
						"realloc (size: %u) error: %m",
						Value->str.len * 2 + 1);
					//return(-1);
					return (0);
				}
				DBConn.EscapeString(
					esc.c_str(), Value->str.ptr,
					Value->str.len); // 先对字符串进行escape
				if (sql.append(esc.c_str()) < 0)
					ErrorNo = -1;
			}
			if (sql.append('\'') < 0)
				ErrorNo = -1;
			break;

		default:;
		};

	return 0;
}

int CHelperProcess::ConditionConcate(const DTCFieldValue *Condition)
{
	int i;

	if (Condition == NULL)
		return (0);

	for (i = 0; i < Condition->num_fields(); i++) {
		if (TableDef->is_volatile(i))
			return -1;
		SQLAppendConst(" AND ");
		SQLAppendField(Condition->field_id(i));
		SQLAppendComparator(Condition->field_operation(i));
		Value2Str(Condition->field_value(i), Condition->field_type(i));
	}

	return 0;
}

inline int CHelperProcess::SetDefaultValue(int field_type, DTCValue &Value)
{
	switch (field_type) {
	case DField::Signed:
		Value.s64 = 0;
		break;

	case DField::Unsigned:
		Value.u64 = 0;
		break;

	case DField::Float:
		Value.flt = 0.0;
		break;

	case DField::String:
		Value.str.len = 0;
		Value.str.ptr = 0;
		break;

	case DField::Binary:
		Value.bin.len = 0;
		Value.bin.ptr = 0;
		break;

	default:
		Value.s64 = 0;
	};

	return (0);
}

inline int CHelperProcess::Str2Value(char *Str, int fieldid, int field_type,
				     DTCValue &Value)
{
	if (Str == NULL) {
		log4cplus_debug(
			"Str is NULL, field_type: %d. Check mysql table definition.",
			field_type);
		SetDefaultValue(field_type, Value);
		return (0);
	}

	switch (field_type) {
	case DField::Signed:
		errno = 0;
		Value.s64 = strtoll(Str, NULL, 10);
		if (errno != 0)
			return (-1);
		break;

	case DField::Unsigned:
		errno = 0;
		Value.u64 = strtoull(Str, NULL, 10);
		if (errno != 0)
			return (-1);
		break;

	case DField::Float:
		errno = 0;
		Value.flt = strtod(Str, NULL);
		if (errno != 0)
			return (-1);
		break;

	case DField::String:
		Value.str.len = _lengths[fieldid];
		Value.str.ptr =
			Str; // 不重新new，要等这个value使用完后释放内存(如果Str是动态分配的)
		break;

	case DField::Binary:
		Value.bin.len = _lengths[fieldid];
		Value.bin.ptr = Str;
		break;

	default:
		log4cplus_error("field[%d] type[%d] invalid.", fieldid,
				field_type);
		break;
	}

	return (0);
}

int CHelperProcess::SaveRow(RowValue *Row, DtcJob *Task)
{
	int i, Ret;

	if (TableDef->num_fields() < 0)
		return (-1);

	for (i = 1; i <= TableDef->num_fields(); i++) {
		//DBConn.Row[0]是key的值，TableDef->Field[0]也是key，
		//因此从1开始。而结果Row是从0开始的(不包括key)
		Ret = Str2Value(DBConn.Row[i], i, TableDef->field_type(i),
				(*Row)[i]);

		if (Ret != 0) {
			log4cplus_error(
				"string[%s] conver to value[%d] error: %d, %m",
				DBConn.Row[i], TableDef->field_type(i), Ret);
			return (-2);
		}
	}

	Task->update_key(Row);
	Ret = Task->append_row(Row);

	if (Ret < 0) {
		return (-3);
	}

	return (0);
}

int CHelperProcess::ProcessSelect(DtcJob *Task)
{
	int Ret, i;
	RowValue *Row = NULL;
	int nRows;
	int haslimit =
		!Task->count_only() && (Task->requestInfo.limit_start() ||
					Task->requestInfo.limit_count());

	SetTitle("SELECT...");
	InitSQLBuffer();
	InitTableName(Task->request_key(), TableDef->field_type(0));

	if (haslimit)
		SQLAppendConst("SELECT SQL_CALC_FOUND_ROWS ");
	else
		SQLAppendConst("SELECT ");
	SelectFieldConcate(Task->request_fields()); // 总是SELECT所有字段
	SQLAppendConst(" FROM ");
	SQLAppendTable();

	// condition
	SQLAppendConst(" WHERE ");
	SQLAppendField(0);
	SQLAppendConst("=");
	Value2Str(Task->request_key(), TableDef->field_type(0));

	if (ConditionConcate(Task->request_condition()) != 0) {
		Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
				"Volatile condition not allowed");
		return (-7);
	}

	if (dbConfig->ordSql) {
		SQLAppendConst(" ");
		SQLAppendString(dbConfig->ordSql);
	}

	if (Task->requestInfo.limit_count() > 0) {
		SQLPrintf(" LIMIT %u, %u", Task->requestInfo.limit_start(),
			  Task->requestInfo.limit_count());
	}

	if (ErrorNo !=
	    0) { // 主要检查PrintfAppend是否发生过错误，这里统一检查一次
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
		log4cplus_error("error occur: %d", ErrorNo);
		return (-1);
	}

	//bug fixed with count *
	Ret = Task->prepare_result_no_limit();
	if (Ret != 0) {
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__,
				"task prepare-result error");
		log4cplus_error("task prepare-result error: %d, %m", Ret);
		return (-2);
	}

	if (!Task->count_only()) {
		Row = new RowValue(TableDef);
		if (Row == NULL) {
			Task->set_error(-ENOMEM, __FUNCTION__, "new row error");
			log4cplus_error("%s new RowValue error: %m", "");
			return (-3);
		}
	}

	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	Ret = DBConn.Query(DBName, sql.c_str());
	log4cplus_debug("SELECT %d %s", Ret, DBConn.GetRawErrNo());
	if (Ret != 0) {
		delete Row;
		Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
				    DBConn.GetErrMsg());
		log4cplus_warning("db query error: %s, pid: %d, group-id: %d",
				  DBConn.GetErrMsg(), getpid(), SelfGroupID);
		return (-4);
	}

	Ret = DBConn.UseResult();
	if (Ret != 0) {
		delete Row;
		Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
				    DBConn.GetErrMsg());
		log4cplus_warning("db user result error: %s",
				  DBConn.GetErrMsg());
		return (-5);
	}

	nRows = DBConn.ResNum;
	for (i = 0; i < DBConn.ResNum; i++) {
		Ret = DBConn.FetchRow();

		if (Ret != 0) {
			delete Row;
			DBConn.FreeResult();
			Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
					    DBConn.GetErrMsg());
			log4cplus_warning("db fetch row error: %s",
					  DBConn.GetErrMsg());
			return (-6);
		}

		//get field value length for the row
		_lengths = 0;
		_lengths = DBConn.getLengths();

		if (0 == _lengths) {
			delete Row;
			DBConn.FreeResult();
			Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
					    DBConn.GetErrMsg());
			log4cplus_warning("db fetch row length error: %s",
					  DBConn.GetErrMsg());
			return (-6);
		}

		// 将结果转换，并保存到task的result里
		if (Task->count_only()) {
			nRows = atoi(DBConn.Row[0]);
			//bug fixed return count *
			Task->set_total_rows(nRows);
			break;
		} else if ((Ret = SaveRow(Row, Task)) != 0) {
			delete Row;
			DBConn.FreeResult();
			Task->set_error(-EC_ERROR_BASE, __FUNCTION__,
					"task append row error");
			log4cplus_error("task append row error: %d", Ret);
			return (-7);
		}
	}

	log4cplus_debug(
		"pid: %d, group-id: %d, result: %d row, db: %s, sql: %s",
		getpid(), SelfGroupID, nRows, DBName, sql.c_str());

	delete Row;
	DBConn.FreeResult();

	//bug fixed确认客户端带Limit限制
	if (haslimit) { // 获取总行数
		InitSQLBuffer();
		SQLAppendConst("SELECT FOUND_ROWS() ");

		log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

		Ret = DBConn.Query(DBName, sql.c_str());
		log4cplus_debug("SELECT %d %s", Ret, DBConn.GetRawErrNo());
		if (Ret != 0) {
			Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
					    DBConn.GetErrMsg());
			log4cplus_warning(
				"db query error: %s, pid: %d, group-id: %d",
				DBConn.GetErrMsg(), getpid(), SelfGroupID);
			return (-4);
		}

		Ret = DBConn.UseResult();
		if (Ret != 0) {
			Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
					    DBConn.GetErrMsg());
			log4cplus_warning("db user result error: %s",
					  DBConn.GetErrMsg());
			return (-5);
		}

		Ret = DBConn.FetchRow();

		if (Ret != 0) {
			DBConn.FreeResult();
			Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
					    DBConn.GetErrMsg());
			log4cplus_warning("db fetch row error: %s",
					  DBConn.GetErrMsg());
			return (-6);
		}

		unsigned long totalRows = strtoul(DBConn.Row[0], NULL, 0);
		if (totalRows == 0) {
			if (nRows != 0)
				totalRows =
					Task->requestInfo.limit_start() + nRows;
			else
				totalRows = 0;
		}

		Ret = Task->set_total_rows(totalRows, 1);

		log4cplus_debug("db: total-rows: %lu, ret: %d", totalRows, Ret);

		DBConn.FreeResult();
	}

	return (0);
}

int CHelperProcess::UpdateFieldConcate(const DTCFieldValue *UpdateInfo)
{
	int i;

	if (UpdateInfo == NULL)
		return (0);

	for (i = 0; i < UpdateInfo->num_fields(); i++) {
		const int fid = UpdateInfo->field_id(i);

		if (TableDef->is_volatile(fid))
			continue;

		switch (UpdateInfo->field_operation(i)) {
		case DField::Set:
			if (i > 0)
				SQLAppendConst(",");
			SQLAppendField(fid);
			SQLAppendConst("=");
			Value2Str(UpdateInfo->field_value(i),
				  UpdateInfo->field_type(i));
			break;

		case DField::Add:
			if (i > 0)
				SQLAppendConst(",");
			SQLAppendField(fid);
			SQLAppendConst("=");
			SQLAppendField(fid);
			SQLAppendConst("+");
			Value2Str(UpdateInfo->field_value(i),
				  UpdateInfo->field_type(i));
			break;

#if 0
			case DField::Subtract:
				if(i>0) SQLAppendConst(",");
				SQLAppendField(fid);
				SQLAppendConst("=");
				SQLAppendField(fid);
				SQLAppendConst("-");
				Value2Str(UpdateInfo->field_value(i), UpdateInfo->field_type(i));
#endif

		default:
			break;
		};
	}

	return 0;
}

int CHelperProcess::DefaultValueConcate(const DTCFieldValue *UpdateInfo)
{
	int i;
	uint8_t mask[32];

	FIELD_ZERO(mask);
	if (UpdateInfo)
		UpdateInfo->build_field_mask(mask);
#if 0 // Allow AutoIncrement Field Insert
	if(TableDef->auto_increment_field_id() > 0)
		FIELD_CLR(TableDef->auto_increment_field_id(), mask);
#endif

	for (i = 1; i <= TableDef->num_fields(); i++) {
		if (FIELD_ISSET(i, mask) || TableDef->is_volatile(i))
			continue;
		SQLAppendConst(",");
		SQLAppendField(i);
		SQLAppendConst("=");
		Value2Str(TableDef->default_value(i), TableDef->field_type(i));
	}

	return 0;
}

int CHelperProcess::ProcessInsert(DtcJob *Task)
{
	int Ret;

	SetTitle("INSERT...");
	InitSQLBuffer();
	InitTableName(Task->request_key(), TableDef->field_type(0));

	SQLAppendConst("INSERT INTO ");
	SQLAppendTable();
	SQLAppendConst(" SET ");

	std::map<std::string, std::string> fieldValues;
	if (Task->request_key()) {
		fieldValues[TableDef->field_name(0)] = ValueToStr(
			Task->request_key(), TableDef->field_type(0));
	}

	if (Task->request_operation()) {
		const DTCFieldValue *updateInfo = Task->request_operation();
		for (int i = 0; i < updateInfo->num_fields(); ++i) {
			int fid = updateInfo->field_id(i);
			if (TableDef->is_volatile(fid))
				continue;
			fieldValues[TableDef->field_name(fid)] =
				ValueToStr(updateInfo->field_value(i),
					   updateInfo->field_type(i));
		}
	}

	for (int i = 1; i <= TableDef->num_fields(); ++i) {
		if (TableDef->is_volatile(i))
			continue;
		if (fieldValues.find(TableDef->field_name(i)) !=
		    fieldValues.end())
			continue;
		fieldValues[TableDef->field_name(i)] = ValueToStr(
			TableDef->default_value(i), TableDef->field_type(i));
	}

	for (std::map<std::string, std::string>::iterator iter =
		     fieldValues.begin();
	     iter != fieldValues.end(); ++iter) {
		SQLAppendString(&LeftQuote, 1);
		SQLAppendString(iter->first.c_str(), iter->first.length());
		SQLAppendString(&RightQuote, 1);
		SQLAppendConst("=");
		SQLAppendString(iter->second.c_str(), iter->second.length());
		SQLAppendConst(",");
	}

	if (sql.at(-1) == ',')
		sql.trunc(-1);

	if (ErrorNo != 0) { // 主要检查PrintfAppend是否发生过错误
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
		log4cplus_error("error occur: %d", ErrorNo);
		return (-1);
	}

	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	Ret = DBConn.Query(DBName, sql.c_str());
	log4cplus_debug("INSERT %d %s", Ret, DBConn.GetRawErrNo());

	if (Ret != 0) {
		int err = DBConn.GetErrNo();
		Task->set_error_dup(err, __FUNCTION__, DBConn.GetErrMsg());
		if (err != -ER_DUP_ENTRY)
			log4cplus_warning("db query error: %s",
					  DBConn.GetErrMsg());
		else
			log4cplus_info("db query error: %s",
				       DBConn.GetErrMsg());
		return (-1);
	}

	Task->resultInfo.set_affected_rows(DBConn.affected_rows());
	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	if (TableDef->has_auto_increment()) {
		uint64_t id = DBConn.InsertID();
		if (id) {
			Task->resultInfo.set_insert_id(id);
			if (TableDef->key_auto_increment())
				Task->resultInfo.set_key(id);
		}
	}

	return (0);
}

int CHelperProcess::ProcessUpdate(DtcJob *Task)
{
	int Ret;

	if (Task->request_operation() == NULL) {
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__,
				"update field not found");
		return (-1);
	}

	if (Task->request_operation()->has_type_commit() == 0) {
		// pure volatile fields update, always succeed
		return (0);
	}

	SetTitle("UPDATE...");
	InitSQLBuffer();
	InitTableName(Task->request_key(), TableDef->field_type(0));

	SQLAppendConst("UPDATE ");
	SQLAppendTable();
	SQLAppendConst(" SET ");
	UpdateFieldConcate(Task->request_operation());

	// key
	SQLAppendConst(" WHERE ");
	SQLAppendField(0);
	SQLAppendConst("=");
	Value2Str(Task->request_key(), TableDef->field_type(0));

	// condition
	if (ConditionConcate(Task->request_condition()) != 0) {
		Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
				"Volatile condition not allowed");
		return (-7);
	}

	if (ErrorNo != 0) { // 主要检查PrintfAppend是否发生过错误
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
		log4cplus_error("error occur: %d", ErrorNo);
		return (-1);
	}

	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	Ret = DBConn.Query(DBName, sql.c_str());
	log4cplus_debug("UPDATE %d %s", Ret, DBConn.GetRawErrNo());
	if (Ret != 0) {
		int err = DBConn.GetErrNo();
		Task->set_error_dup(err, __FUNCTION__, DBConn.GetErrMsg());
		if (err != -ER_DUP_ENTRY)
			log4cplus_warning("db query error: %s",
					  DBConn.GetErrMsg());
		else
			log4cplus_info("db query error: %s",
				       DBConn.GetErrMsg());
		return -1;
	}

	Task->resultInfo.set_affected_rows(DBConn.affected_rows());
	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	return (0);
}

int CHelperProcess::ProcessDelete(DtcJob *Task)
{
	int Ret;

	SetTitle("DELETE...");
	InitSQLBuffer();
	InitTableName(Task->request_key(), TableDef->field_type(0));

	SQLAppendConst("DELETE FROM ");
	SQLAppendTable();

	// key
	SQLAppendConst(" WHERE ");
	SQLAppendField(0);
	SQLAppendConst("=");
	Value2Str(Task->request_key(), TableDef->field_type(0));

	// condition
	if (ConditionConcate(Task->request_condition()) != 0) {
		Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
				"Volatile condition not allowed");
		return (-7);
	}

	if (ErrorNo !=
	    0) { // 主要检查PrintfAppend是否发生过错误，这里统一检查一次
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
		log4cplus_error("error occur: %d", ErrorNo);
		return (-1);
	}

	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	Ret = DBConn.Query(DBName, sql.c_str());
	log4cplus_debug("DELETE %d %s", Ret, DBConn.GetRawErrNo());
	if (Ret != 0) {
		Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
				    DBConn.GetErrMsg());
		log4cplus_warning("db query error: %s", DBConn.GetErrMsg());
		return (-1);
	}

	Task->resultInfo.set_affected_rows(DBConn.affected_rows());
	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	return (0);
}

int CHelperProcess::ProcessTask(DtcJob *Task)
{
	if (Task == NULL) {
		log4cplus_error("Task is NULL!%s", "");
		return (-1);
	}

	TableDef = TableDefinitionManager::instance()->get_cur_table_def();

	switch (Task->request_code()) {
	case DRequest::TYPE_PASS:
	case DRequest::Purge:
	case DRequest::Flush:
		return 0;

	case DRequest::Get:
		return ProcessSelect(Task);

	case DRequest::Insert:
		return ProcessInsert(Task);

	case DRequest::Update:
		return ProcessUpdate(Task);

	case DRequest::Delete:
		return ProcessDelete(Task);

	case DRequest::Replace:
		return ProcessReplace(Task);

	case DRequest::ReloadConfig:
		return ProcessReloadConfig(Task);

	default:
		Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
				"invalid request-code");
		return (-1);
	}
}

//add by frankyang 处理更新过的交易日志
int CHelperProcess::ProcessReplace(DtcJob *Task)
{
	int Ret;

	SetTitle("REPLACE...");
	InitSQLBuffer();
	InitTableName(Task->request_key(), TableDef->field_type(0));

	SQLAppendConst("REPLACE INTO ");
	SQLAppendTable();
	SQLAppendConst(" SET ");
	SQLAppendField(0);
	SQLAppendConst("=");
	Value2Str(Task->request_key(), TableDef->field_type(0));
	SQLAppendConst(",");

	/* 补全缺失的默认值 */
	if (Task->request_operation())
		UpdateFieldConcate(Task->request_operation());
	else if (sql.at(-1) == ',') {
		sql.trunc(-1);
	}
	DefaultValueConcate(Task->request_operation());

	if (ErrorNo != 0) { // 主要检查PrintfAppend是否发生过错误
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
		log4cplus_error("error occur: %d", ErrorNo);
		return (-1);
	}
	if (ErrorNo != 0) { // 主要检查PrintfAppend是否发生过错误
		Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
		log4cplus_error("error occur: %d", ErrorNo);
		return (-1);
	}

	log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

	Ret = DBConn.Query(DBName, sql.c_str());
	log4cplus_debug("REPLACE %d %s", Ret, DBConn.GetRawErrNo());

	if (Ret != 0) {
		Task->set_error_dup(DBConn.GetErrNo(), __FUNCTION__,
				    DBConn.GetErrMsg());
		log4cplus_warning("db query error: %s", DBConn.GetErrMsg());
		return (-1);
	}

	Task->resultInfo.set_affected_rows(DBConn.affected_rows());

	log4cplus_debug("%s",
			"CHelperProcess::ProcessReplaceTask() successful.");

	return 0;
}

CHelperProcess::~CHelperProcess()
{
}

void CHelperProcess::InitTitle(int group, int role)
{
	titlePrefixSize = snprintf(name, sizeof(name), "helper%d%c", group,
				   MACHINEROLESTRING[role]);
	memcpy(title, name, titlePrefixSize);
	title[titlePrefixSize++] = ':';
	title[titlePrefixSize++] = ' ';
	title[titlePrefixSize] = '\0';
	title[sizeof(title) - 1] = '\0';
}

void CHelperProcess::SetTitle(const char *status)
{
	strncpy(title + titlePrefixSize, status,
		sizeof(title) - 1 - titlePrefixSize);
	set_proc_title(title);
}

int CHelperProcess::ProcessReloadConfig(DtcJob *Task)
{
	const char *keyStr = g_dtc_config->get_str_val("cache", "CacheShmKey");
	int cacheKey = 0;
	if (keyStr == NULL) {
		cacheKey = 0;
		log4cplus_info("CacheShmKey not set!");
		return -1;
	} else if (!strcasecmp(keyStr, "none")) {
		log4cplus_warning("CacheShmKey set to NONE, Cache disabled");
		return -1;
	} else if (isdigit(keyStr[0])) {
		cacheKey = strtol(keyStr, NULL, 0);
	} else {
		log4cplus_warning("Invalid CacheShmKey value \"%s\"", keyStr);
		return -1;
	}
	BlockProperties stInfo;
	BufferPond cachePool;
	memset(&stInfo, 0, sizeof(stInfo));
	stInfo.ipc_mem_key = cacheKey;
	stInfo.key_size = TableDefinitionManager::instance()
				  ->get_cur_table_def()
				  ->key_format();
	stInfo.read_only = 1;

	if (cachePool.cache_open(&stInfo)) {
		log4cplus_error("%s", cachePool.error());
		Task->set_error(-EC_RELOAD_CONFIG_FAILED, __FUNCTION__,
				"open cache error!");
		return -1;
	}

	cachePool.reload_table();
	log4cplus_error(
		"cmd notify work helper reload table, tableIdx : [%d], pid : [%d]",
		cachePool.shm_table_idx(), getpid());
	return 0;
}
