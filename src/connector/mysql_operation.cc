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
#include <sstream>
#include <string>
#include <iostream>
// local include files
#include "mysql_operation.h"
// common include files
#include "protocol.h"
#include "log/log.h"
#include "proc_title.h"
#include "table/table_def_manager.h"
#include "daemon/daemon.h"
// mysql include files
#include "mysqld_error.h"
// core include files
#include "buffer/buffer_pond.h"

#define MIN(x, y) ((x) <= (y) ? (x) : (y))

ConnectorProcess::ConnectorProcess() : _lengths(0)
{
    error_no = 0;

    left_quote = '`';
    right_quote = '`';

    title_prefix_size = 0;
    time(&last_access);
    ping_timeout = 9;
    proc_timeout = 0;
    strncpy(name, "helper", 6);
}

int ConnectorProcess::try_ping(void)
{
    return db_conn.do_ping();
}

void ConnectorProcess::init_ping_timeout(void)
{
    int64_t to = db_conn.get_variable("wait_timeout");
    log4cplus_debug("Server idle timeout %lld", (long long)to);
    if (to < 10)
        to = 10;
    else if (to > 600)
        to = 600;
    ping_timeout = to * 9 / 10;
}

int ConnectorProcess::config_db_by_struct(const DbConfig *cf)
{
    if (cf == NULL)
        return -1;
    dbConfig = cf;
    return (0);
}

static char HiddenMysqlFields[][32] = {
    "id",
    "invisible_time"
};

#define DIM(a) (sizeof(a) / sizeof(a[0]))

typedef struct {
    char m_szName[256];
    int m_iType;
    int m_uiSize;

    int CheckSizeInInteger(int size) {
        if (DField::Signed == m_iType ||
        DField::Unsigned == m_iType) {
            return (m_uiSize == size) ? 0 : -1;
        } 
        return -2;
    };
} CMysqlField , CDtcField;

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

/**
 * when m_iType is Signed or Unsigned , m_uiSize is useful
 * but when m_iType is Float , String or Binary , m_uiSize is workless
*/
static CDtcField dtcFieldTab[] = { 
            {"tinyint" , DField::Signed , 1},
            {"smallint" , DField::Signed , 2},
            {"mediumint" , DField::Signed , 3},
            {"int" , DField::Signed , 4},
            {"bigint" , DField::Signed , 8},
            {"float" , DField::Float , 4},
            {"varchar" , DField::String , 65535U},
            {"varchar" , DField::Binary , 65535U}
};

static int get_field_type(
    std::string& szType,
    int i_type,
    unsigned int ui_size)
{
    int i = 0;
    for (; i < DIM(dtcFieldTab); i++) {
        int i_tmp_type = (DField::Unsigned == i_type) ? DField::Signed : i_type;
        
        if (dtcFieldTab[i].m_iType == i_tmp_type) {
            int i_ret = dtcFieldTab[i].CheckSizeInInteger(ui_size);
            if (i_ret == 0 || i_ret == -2) {
                szType =  dtcFieldTab[i].m_szName;
                break;
            }
        }
    }

    if (i >= DIM(dtcFieldTab)) {
        log4cplus_error("dtc-yaml config field info has no responding mysql type, dtc type:%d , type size:%d",
             i_type , ui_size);
        return -1;
    }

    switch (i_type)
    {
    case DField::Unsigned:
        {
            szType.append(" UNSIGNED ");
        }
        break;
    case DField::String:
    case DField::Binary:
        {
            std::stringstream s_temp;
            s_temp << "(";
            s_temp << ui_size;
            s_temp << ")";
            szType.append(s_temp.str());
        }
        break;
    default:
        break;
    }
    
    return 0;
}

static int get_field_type(const char *szType, int &i_type,
              unsigned int &ui_size)
{
    unsigned int i;
    int iTmp;
    for (i = 0; i < DIM(astField); i++) {
        if (strncasecmp(szType, astField[i].m_szName,
                strlen(astField[i].m_szName)) == 0) {
            i_type = astField[i].m_iType;
            ui_size = astField[i].m_uiSize;
            if (strncasecmp(szType, "varchar", 7) == 0) {
                if (sscanf(szType + 8, "%d", &iTmp) == 1)
                    ui_size = iTmp;
            } else if (strncasecmp(szType, "char", 4) == 0) {
                if (sscanf(szType + 5, "%d", &iTmp) == 1)
                    ui_size = iTmp;
            } else if (strncasecmp(szType, "varbinary", 9) == 0) {
                if (sscanf(szType + 10, "%d", &iTmp) == 1)
                    ui_size = iTmp;
            } else if (strncasecmp(szType, "binary", 6) == 0) {
                if (sscanf(szType + 7, "%d", &iTmp) == 1)
                    ui_size = iTmp;
            }
            if (i_type == 1 && strstr(szType, "unsigned") != NULL)
                i_type = 2;

            if (i_type == 3 && strstr(szType, "unsigned") != NULL)
                fprintf(stderr,
                    "#warning: dtc not support unsigned double!\n");

            break;
        }
    }

    return (0);
}

static 

int ConnectorProcess::create_tab_if_not_exist()
{
    snprintf(table_name, sizeof(table_name), dbConfig->tblFormat, 0);
    init_sql_buffer();
    sql_append_const("create table if not exists `");
    sql_append_string(table_name);
    sql_append_const("`");

    sql_append_const("(");

    std::string s_unique_key("");
    for (int i = 0; i <= table_def->num_fields(); i++) {
        int field_id = table_def->field_id(table_def->field_name(i));
        log4cplus_debug("field name:%s , id:%d , type:%d , size:%d , default:%d" , 
                table_def->field_name(i) ,
                field_id,
                table_def->field_type(i),
                table_def->field_size(i),
                table_def->has_default(i));

        bool is_primary_key = false;
        uint8_t* uniq_fields = table_def->uniq_fields_list();
        for (int j = 0; j < table_def->uniq_fields(); j++) {
                if (uniq_fields[j] == field_id) { 
                    s_unique_key.append("`");
                    s_unique_key.append(table_def->field_name(i));
                    s_unique_key.append("`,");
                    is_primary_key = true;
                    break;
                }
        }

        sql_append_const("`");
        sql_append_string(table_def->field_name(i));
        sql_append_const("` ");

        std::string stype = "";
        if (get_field_type(stype , table_def->field_type(i)
            ,table_def->field_size(i))) {
            return -1;
        }

        sql_append_string(stype.c_str());

        if (is_primary_key || !table_def->is_nullable(i)) {
            sql_append_string(" NOT NULL ");
        }

        if (table_def->has_default(i)) {
            sql_append_string("default ");
            format_sql_value(table_def->default_value(i), table_def->field_type(i));
        }
        
        sql_append_const(",");
    }
    sql_append_string("`id` INT(11) NOT NULL AUTO_INCREMENT,");
    sql_append_string("`invisible_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,");

    sql_append_string("UNIQUE INDEX (");
    sql_append_string(s_unique_key.c_str() , s_unique_key.length() - 1);
    sql_append_const("),");

    sql_append_string("PRIMARY KEY (`id`)");
    sql_append_string(")ENGINE=InnoDB DEFAULT CHARSET=");
    sql_append_string(db_conn.GetCharacSet().c_str());

    snprintf(DBName, sizeof(DBName), dbConfig->dbFormat,
         dbConfig->mach[self_group_id].dbIdx[0]);

    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    int i_ret = db_conn.do_query(DBName, sql.c_str());
    log4cplus_debug("create table ret:%d %d", i_ret, db_conn.get_raw_err_no());
    if (i_ret != 0) {
        log4cplus_warning("db query error: %s, pid: %d, group-id: %d",
                  db_conn.get_err_msg(), getpid(),
                  self_group_id);
        return -1;
    }

    return 0;
}

int ConnectorProcess::check_table()
{
    int Ret;
    int i;
    int i_field_num;
    char ach_field_name[256][256];

    snprintf(DBName, sizeof(DBName), dbConfig->dbFormat,
         dbConfig->mach[self_group_id].dbIdx[0]);
    snprintf(table_name, sizeof(table_name), dbConfig->tblFormat, 0);

    init_sql_buffer();
    sql_append_const("show columns from `");
    sql_append_string(table_name);
    sql_append_const("`");

    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    Ret = db_conn.do_query(DBName, sql.c_str());
    log4cplus_debug("SELECT %d %d", Ret, db_conn.get_raw_err_no());
    if (Ret != 0) {
        log4cplus_warning("db query error: %s, pid: %d, group-id: %d",
                  db_conn.get_err_msg(), getpid(),
                  self_group_id);
        return (-1);
    }

    Ret = db_conn.use_result();
    if (Ret != 0) {
        log4cplus_warning("db user result error: %s",
                  db_conn.get_err_msg());
        return (-2);
    }

    // 获取返回结果的各列位置
    int i_name_idx = 0, i_type_idx = 0;
    int i_null_idx = 0, i_key_idx = 0;
    int i_default_idx = 0, i_extra_idx = 0;
    unsigned int ui_num_fields = mysql_num_fields(db_conn.Res);
    MYSQL_FIELD *pst_fields = mysql_fetch_fields(db_conn.Res);
    for (i = 0; i < (int)ui_num_fields; i++) {
        if (strcasecmp("Field", pst_fields[i].name) == 0)
            i_name_idx = i;
        else if (strcasecmp("Type", pst_fields[i].name) == 0)
            i_type_idx = i;
        else if (strcasecmp("Null", pst_fields[i].name) == 0)
            i_null_idx = i;
        else if (strcasecmp("Key", pst_fields[i].name) == 0)
            i_key_idx = i;
        else if (strcasecmp("Default", pst_fields[i].name) == 0)
            i_default_idx = i;
        else if (strcasecmp("Extra", pst_fields[i].name) == 0)
            i_extra_idx = i;
    }

    int iFid;
    i_field_num = 0;
    memset(ach_field_name, 0, sizeof(ach_field_name));

    int uniq_fields_cnt_table = table_def->uniq_fields();
    for (i = 0; i < db_conn.res_num; i++) {
        Ret = db_conn.fetch_row();

        if (Ret != 0) {
            db_conn.free_result();
            log4cplus_warning("db fetch row error: %s",
                      db_conn.get_err_msg());
            return (-3);
        }

        strncpy(ach_field_name[i_field_num], db_conn.Row[i_name_idx],
            255);
        i_field_num++;

        int j = 0;
        for (; j < DIM(HiddenMysqlFields); j++) {
            if (!strncmp(db_conn.Row[i_name_idx] , HiddenMysqlFields[j] , 31)) {
                log4cplus_info("field:%s is no need check" , HiddenMysqlFields[j]);
                break;
            }
        }

        if (j < DIM(HiddenMysqlFields)) { continue; }
        
        iFid = table_def->field_id(db_conn.Row[i_name_idx]);
        if (iFid == -1) {
            log4cplus_debug("field[%s] not found in table.yaml",
                    db_conn.Row[i_name_idx]);
            continue;
        }

        if (table_def->is_volatile(iFid)) {
            log4cplus_error(
                "field[name: `%s`] found in table.yaml and DB both, can't be Volatile",
                db_conn.Row[i_name_idx]);
            db_conn.free_result();
            return (-4);
        }

        if (table_def->is_timestamp(iFid)) {
            log4cplus_error(
                "in table.yaml, Field[name: `%s`]'s is timestamp, not support in DB mode",
                db_conn.Row[i_name_idx]);
            db_conn.free_result();
            return (-4);
        }

        //field type & size
        int i_type = -1;
        unsigned ui_size = 0;
        get_field_type(db_conn.Row[i_type_idx], i_type, ui_size);
        if (i_type != table_def->field_type(iFid)) {
            log4cplus_error(
                "in table.yaml, Field[name: `%s`]'s type incorrect. conf: %d, mysql:%d",
                db_conn.Row[i_name_idx],
                table_def->field_type(iFid), i_type);
            db_conn.free_result();
            return (-4);
        }

        if ((int)ui_size != table_def->field_size(iFid) &&
            !(ui_size >= (64 << 20) &&
              table_def->field_size(iFid) >= (64 << 20))) {
            log4cplus_error(
                "in table.yaml, Field[name: `%s`]'s size incorrect. conf: %d, mysql:%u",
                db_conn.Row[i_name_idx],
                table_def->field_size(iFid), ui_size);
            db_conn.free_result();
            return (-4);
        }

        if (db_conn.Row[i_extra_idx] != NULL &&
            strcasecmp("auto_increment", db_conn.Row[i_extra_idx]) ==
                0) {
            if (table_def->auto_increment_field_id() != iFid) {
                log4cplus_error(
                    "in table.yaml, Field[name: `%s`]'s default-value incorrect. conf: non-auto_increment, mysql:auto_increment",
                    db_conn.Row[i_name_idx]);
                db_conn.free_result();
                return (-4);
            }
        }

        /*field should be uniq in table.yaml if configed primary in db */
        uint8_t *uniq_fields = table_def->uniq_fields_list();
        if (db_conn.Row[i_key_idx] != NULL &&
            (strcasecmp("PRI", db_conn.Row[i_key_idx]) == 0 ||
             strcasecmp("UNI", db_conn.Row[i_key_idx]) == 0)) {
            int j = 0;
            for (j = 0; j < table_def->uniq_fields(); j++) {
                if (uniq_fields[j] == iFid)
                    break;
            }

            if (j >= table_def->uniq_fields()) {
                log4cplus_error(
                    "in table.yaml, Field[name: `%s`] is primary in db, but not uniq in dtc",
                    db_conn.Row[i_name_idx]);
                return -4;
            }

            uniq_fields_cnt_table--;
        }
    }

    /*field should be primary in db if configed uniq in table.yaml*/
    if (uniq_fields_cnt_table != 0) {
        log4cplus_error(
            "table.yaml have more uniq fields that not configed as primary in db");
        return -4;
    }

    for (int i = 0; i <= table_def->num_fields(); i++) {
        //bug fix volatile不在db中
        if (table_def->is_volatile(i))
            continue;

        const char *name = table_def->field_name(i);
        int j;
        for (j = 0; j < i_field_num; j++) {
            if (strcmp(ach_field_name[j], name) == 0)
                break;
        }
        if (j >= i_field_num) {
            log4cplus_error(
                "in table.yaml, Field[name: `%s`] not found in mysql",
                name);
            db_conn.free_result();
            return (-4);
        }
    }

    log4cplus_debug(
        "pid: %d, group-id: %d check table success, db: %s, sql: %s",
        getpid(), self_group_id, DBName, sql.c_str());

    db_conn.free_result();

    return (0);
}

int ConnectorProcess::machine_init(int GroupID, int r)
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

    memset(&db_host_conf, 0, sizeof(DBHost));
    p = strrchr(role->addr, ':');
    if (p == NULL) {
        strncpy(db_host_conf.Host, role->addr,
            sizeof(db_host_conf.Host) - 1);
        db_host_conf.Port = 0;
    } else {
        strncpy(db_host_conf.Host, role->addr,
            MIN(p - role->addr,
                (int)sizeof(db_host_conf.Host) - 1));
        db_host_conf.Port = atoi(p + 1);
    }
    strncpy(db_host_conf.User, role->user, sizeof(db_host_conf.User) - 1);
    strncpy(db_host_conf.Password, role->pass,
        sizeof(db_host_conf.Password) - 1);
    db_host_conf.ConnTimeout = proc_timeout;
    strncpy(db_host_conf.OptionFile, role->optfile,
        sizeof(db_host_conf.OptionFile) - 1);

    db_conn.do_config(&db_host_conf);

    if (db_conn.Open() != 0) {
        log4cplus_warning("connect db[%s] error: %s", db_host_conf.Host,
                  db_conn.get_err_msg());
        return (-6);
    }

    log4cplus_debug("group-id: %d, pid: %d, db: %s, user: %s, pwd: %s , client charac_set:%s",
            self_group_id, getpid(), db_host_conf.Host,
            db_host_conf.User, db_host_conf.Password,
            db_conn.GetCharacSet().c_str());

    return (0);
}

int ConnectorProcess::do_init(int GroupID, const DbConfig *do_config,
                  DTCTableDefinition *tdef, int slave)
{
    int Ret;

    self_group_id = GroupID;
    table_def = tdef;

    Ret = config_db_by_struct(do_config);
    if (Ret != 0) {
        return (-1);
    }

    Ret = machine_init(GroupID, slave);
    if (Ret != 0) {
        return (-2);
    }

    return (0);
}

void ConnectorProcess::init_sql_buffer(void)
{
    sql.clear();
    error_no = 0;
}

void ConnectorProcess::sql_append_string(const char *str, int len)
{
    if (len == 0)
        len = strlen(str);
    if (sql.append(str, len) < 0) {
        error_no = -1;
        log4cplus_error("sql.append() error: %d, %m", sql.needed());
    }
}

/* 将字符串printf在原来字符串的后面，如果buffer不够大会自动重新分配buffer */
void ConnectorProcess::sql_printf(const char *Format, ...)
{
    va_list Arg;
    int Len;

    va_start(Arg, Format);
    Len = sql.vbprintf(Format, Arg);
    va_end(Arg);
    if (Len < 0) {
        error_no = -1;
        log4cplus_error("vsnprintf error: %d, %m", Len);
    }
}

void ConnectorProcess::sql_append_table(void)
{
    sql_append_string(&left_quote, 1);
    sql_append_string(table_name);
    sql_append_string(&right_quote, 1);
}

void ConnectorProcess::sql_append_field(int fid)
{
    sql_append_string(&left_quote, 1);
    sql_append_string(table_def->field_name(fid));
    sql_append_string(&right_quote, 1);
}

void ConnectorProcess::sql_append_comparator(uint8_t op)
{
    // order is important
    static const char *const CompStr[] = { "=", "!=", "<", "<=", ">", ">=" };
    if (op >= DField::TotalComparison) {
        error_no = -1;
        log4cplus_error("unknow op: %d", op);
    } else {
        sql_append_string(CompStr[op]);
    }
}

void ConnectorProcess::init_table_name(const DTCValue *Key, int field_type)
{
    log4cplus_info("line:%d" ,__LINE__);
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
            log4cplus_info("div:%d , mod:%d" , dbConfig->dbDiv , dbConfig->dbMod);
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
    log4cplus_info("line:%d" ,__LINE__);
    snprintf(DBName, sizeof(DBName), dbConfig->dbFormat, dbid);
    snprintf(table_name, sizeof(table_name), dbConfig->tblFormat, tableid);
    log4cplus_info("DBName:%s , table_name:%s" ,DBName , table_name);
}

int ConnectorProcess::select_field_concate(const DTCFieldSet *fs)
{
    if (fs == NULL) {
        sql_append_const("COUNT(*)");
    } else {
        int i = 0;
        uint8_t mask[32];

        FIELD_ZERO(mask);
        fs->build_field_mask(mask);
        sql_append_field(0); // key

        for (i = 1; i < table_def->num_fields() + 1; i++) {
            sql_append_const(",");
            if (FIELD_ISSET(i, mask) == 0) {
                /* Missing field as 0 */
                sql_append_const("0");
            } else if (table_def->is_volatile(i) == 0) {
                sql_append_field(i);
            } else {
                // volatile field initialized as default value
                format_sql_value(table_def->default_value(i),
                         table_def->field_type(i));
            }
        }
    }
    return 0;
}

std::string ConnectorProcess::value_to_str(const DTCValue *v, int fieldType)
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
            error_no = -1;
            log4cplus_error("realloc (size: %u) error: %m",
                    v->str.len * 2 + 1);
            return "NULL";
        }
        db_conn.escape_string(esc.c_str(), v->str.ptr,
                      v->str.len); // 先对字符串进行escape
        ret = '\'';
        ret += esc.c_str();
        ret += "\'";
        return ret;
    default:
        error_no = -1;
        log4cplus_error("unknown field type: %d", fieldType);
        return "UNKNOWN";
    }
}

inline int ConnectorProcess::format_sql_value(const DTCValue *Value,
                          int iFieldType)
{
    log4cplus_debug("format_sql_value iFieldType[%d]", iFieldType);

    if (Value == NULL) {
        sql_append_const("NULL");
    } else
        switch (iFieldType) {
        case DField::Signed:
            sql_printf("%lld", (long long)Value->s64);
            break;

        case DField::Unsigned:
            sql_printf("%llu", (unsigned long long)Value->u64);
            break;

        case DField::Float:
            sql_printf("'%f'", Value->flt);
            break;

        case DField::String:
        case DField::Binary:
            if (sql.append('\'') < 0)
                error_no = -1;
            if (!Value->str.is_empty()) {
                esc.clear();
                if (esc.expand(Value->str.len * 2 + 1) < 0) {
                    error_no = -1;
                    log4cplus_error(
                        "realloc (size: %u) error: %m",
                        Value->str.len * 2 + 1);
                    //return(-1);
                    return (0);
                }
                db_conn.escape_string(
                    esc.c_str(), Value->str.ptr,
                    Value->str.len); // 先对字符串进行escape
                if (sql.append(esc.c_str()) < 0)
                    error_no = -1;
            }
            if (sql.append('\'') < 0)
                error_no = -1;
            break;

        default:;
        };

    return 0;
}

int ConnectorProcess::condition_concate(const DTCFieldValue *Condition)
{
    int i;

    if (Condition == NULL)
        return (0);

    for (i = 0; i < Condition->num_fields(); i++) {
        if (table_def->is_volatile(i))
            return -1;
        sql_append_const(" AND ");
        sql_append_field(Condition->field_id(i));
        sql_append_comparator(Condition->field_operation(i));
        format_sql_value(Condition->field_value(i),
                 Condition->field_type(i));
    }

    return 0;
}

inline int ConnectorProcess::set_default_value(int field_type, DTCValue &Value)
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

inline int ConnectorProcess::str_to_value(char *Str, int fieldid,
                      int field_type, DTCValue &Value)
{
    if (Str == NULL) {
        log4cplus_debug(
            "Str is NULL, field_type: %d. Check mysql table definition.",
            field_type);
        set_default_value(field_type, Value);
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

int ConnectorProcess::save_row(RowValue *Row, DtcJob *Task)
{
    int i, Ret;

    if (table_def->num_fields() < 0)
        return (-1);

    for (i = 1; i <= table_def->num_fields(); i++) {
        //db_conn.Row[0]是key的值，table_def->Field[0]也是key，
        //因此从1开始。结果Row也是从1开始的(不包括key)
        Ret = str_to_value(db_conn.Row[i], i, table_def->field_type(i),
                   (*Row)[i]);

        if (Ret != 0) {
            log4cplus_error(
                "string[%s] conver to value[%d] error: %d, %m",
                db_conn.Row[i], table_def->field_type(i), Ret);
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

int ConnectorProcess::process_statement_query(
    const DTCValue* key,
    std::string& s_sql)
{
    // hash 计算key落在哪库哪表
    init_table_name(key, table_def->field_type(0));
    log4cplus_debug("db: %s, sql: %s", DBName, s_sql.c_str());

    // 分表时，需更更换表名
    if (dbConfig->depoly&2) {
        const char* p_table_name = table_def->table_name();
        if (NULL == p_table_name) {
            return -1;
        }
    
        int i_pos = s_sql.find(p_table_name);
        if (i_pos != std::string::npos) {
            int i_table_name_len = strlen(p_table_name);
            s_sql.replace(i_pos , i_table_name_len , table_name);
        }
    }
    
    // 重新选库，并查询
    int i_ret = db_conn.do_query(DBName, s_sql.c_str());
    if (i_ret != 0) {
        int i_err = db_conn.get_err_no();
        if (i_err != -ER_DUP_ENTRY) {
            log4cplus_warning("db query error: %s",
                      db_conn.get_err_msg());
        } else {
            log4cplus_info("db query error: %s",
                       db_conn.get_err_msg());
            return -ER_DUP_ENTRY;
        }
    }
    return i_ret;
}

int ConnectorProcess::process_select(DtcJob *Task)
{
    log4cplus_info("line:%d" ,__LINE__);
    int Ret, i;
    RowValue *Row = NULL;
    int nRows;
    int haslimit =
        !Task->count_only() && (Task->requestInfo.limit_start() ||
                    Task->requestInfo.limit_count());
    log4cplus_info("line:%d" ,__LINE__);

    set_title("SELECT...");
    init_sql_buffer();
    log4cplus_info("line:%d" ,__LINE__);
    if (Task == NULL)
    {
        log4cplus_info("line:%d" ,__LINE__);
        return 0;
    }

    if (table_def == NULL)
    {
        log4cplus_info("line:%d" ,__LINE__);
        return 0;
    }
    log4cplus_info("line:%d" ,__LINE__);
    init_table_name(Task->request_key(), table_def->field_type(0));
    log4cplus_info("line:%d" ,__LINE__);

    if (haslimit)
        sql_append_const("SELECT SQL_CALC_FOUND_ROWS ");
    else
        sql_append_const("SELECT ");
    select_field_concate(Task->request_fields()); // 总是SELECT所有字段
    sql_append_const(" FROM ");
    sql_append_table();
    log4cplus_info("line:%d" ,__LINE__);

    // condition
    sql_append_const(" WHERE ");
    sql_append_field(0);
    sql_append_const("=");
    format_sql_value(Task->request_key(), table_def->field_type(0));
    log4cplus_info("line:%d" ,__LINE__);

    if (condition_concate(Task->request_condition()) != 0) {
        Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
                "Volatile condition not allowed");
        return (-7);
    }
    log4cplus_info("line:%d" ,__LINE__);
    if (dbConfig->ordSql) {
        sql_append_const(" ");
        sql_append_string(dbConfig->ordSql);
    }

    if (Task->requestInfo.limit_count() > 0) {
        sql_printf(" LIMIT %u, %u", Task->requestInfo.limit_start(),
               Task->requestInfo.limit_count());
    }
    log4cplus_info("line:%d" ,__LINE__);
    if (error_no !=
        0) { // 主要检查PrintfAppend是否发生过错误，这里统一检查一次
        Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
        log4cplus_error("error occur: %d", error_no);
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
        Row = new RowValue(table_def);
        if (Row == NULL) {
            Task->set_error(-ENOMEM, __FUNCTION__, "new row error");
            log4cplus_error("%s new RowValue error: %m", "");
            return (-3);
        }
    }
    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    Ret = db_conn.do_query(DBName, sql.c_str());
    log4cplus_debug("SELECT %d %d", Ret, db_conn.get_raw_err_no());
    if (Ret != 0) {
        delete Row;
        Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                    db_conn.get_err_msg());
        log4cplus_warning("db query error: %s, pid: %d, group-id: %d",
                  db_conn.get_err_msg(), getpid(),
                  self_group_id);
        return (-4);
    }

    Ret = db_conn.use_result();
    if (Ret != 0) {
        delete Row;
        Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                    db_conn.get_err_msg());
        log4cplus_warning("db user result error: %s",
                  db_conn.get_err_msg());
        return (-5);
    }

    nRows = db_conn.res_num;
    for (i = 0; i < db_conn.res_num; i++) {
        Ret = db_conn.fetch_row();

        if (Ret != 0) {
            delete Row;
            db_conn.free_result();
            Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                        db_conn.get_err_msg());
            log4cplus_warning("db fetch row error: %s",
                      db_conn.get_err_msg());
            return (-6);
        }

        //get field value length for the row
        _lengths = 0;
        _lengths = db_conn.get_lengths();

        if (0 == _lengths) {
            delete Row;
            db_conn.free_result();
            Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                        db_conn.get_err_msg());
            log4cplus_warning("db fetch row length error: %s",
                      db_conn.get_err_msg());
            return (-6);
        }

        // 将结果转换，并保存到task的result里
        if (Task->count_only()) {
            nRows = atoi(db_conn.Row[0]);
            //bug fixed return count *
            Task->set_total_rows(nRows);
            break;
        } else if ((Ret = save_row(Row, Task)) != 0) {
            delete Row;
            db_conn.free_result();
            Task->set_error(-EC_ERROR_BASE, __FUNCTION__,
                    "task append row error");
            log4cplus_error("task append row error: %d", Ret);
            return (-7);
        }
    }

    log4cplus_debug(
        "pid: %d, group-id: %d, result: %d row, db: %s, sql: %s",
        getpid(), self_group_id, nRows, DBName, sql.c_str());

    delete Row;
    db_conn.free_result();

    //bug fixed确认客户端带Limit限制
    if (haslimit) { // 获取总行数
        init_sql_buffer();
        sql_append_const("SELECT FOUND_ROWS() ");

        log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

        Ret = db_conn.do_query(DBName, sql.c_str());
        log4cplus_debug("SELECT %d %d", Ret, db_conn.get_raw_err_no());
        if (Ret != 0) {
            Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                        db_conn.get_err_msg());
            log4cplus_warning(
                "db query error: %s, pid: %d, group-id: %d",
                db_conn.get_err_msg(), getpid(), self_group_id);
            return (-4);
        }

        Ret = db_conn.use_result();
        if (Ret != 0) {
            Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                        db_conn.get_err_msg());
            log4cplus_warning("db user result error: %s",
                      db_conn.get_err_msg());
            return (-5);
        }

        Ret = db_conn.fetch_row();

        if (Ret != 0) {
            db_conn.free_result();
            Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                        db_conn.get_err_msg());
            log4cplus_warning("db fetch row error: %s",
                      db_conn.get_err_msg());
            return (-6);
        }

        unsigned long totalRows = strtoul(db_conn.Row[0], NULL, 0);
        if (totalRows == 0) {
            if (nRows != 0)
                totalRows =
                    Task->requestInfo.limit_start() + nRows;
            else
                totalRows = 0;
        }

        Ret = Task->set_total_rows(totalRows, 1);

        log4cplus_debug("db: total-rows: %lu, ret: %d", totalRows, Ret);

        db_conn.free_result();
    }

    return (0);
}

int ConnectorProcess::update_field_concate(const DTCFieldValue *UpdateInfo)
{
    int i;

    if (UpdateInfo == NULL)
        return (0);

    for (i = 0; i < UpdateInfo->num_fields(); i++) {
        const int fid = UpdateInfo->field_id(i);

        if (table_def->is_volatile(fid))
            continue;

        switch (UpdateInfo->field_operation(i)) {
        case DField::Set:
            if (i > 0)
                sql_append_const(",");
            sql_append_field(fid);
            sql_append_const("=");
            format_sql_value(UpdateInfo->field_value(i),
                     UpdateInfo->field_type(i));
            break;

        case DField::Add:
            if (i > 0)
                sql_append_const(",");
            sql_append_field(fid);
            sql_append_const("=");
            sql_append_field(fid);
            sql_append_const("+");
            format_sql_value(UpdateInfo->field_value(i),
                     UpdateInfo->field_type(i));
            break;

        default:
            break;
        };
    }

    return 0;
}

int ConnectorProcess::default_value_concate(const DTCFieldValue *UpdateInfo)
{
    int i;
    uint8_t mask[32];

    FIELD_ZERO(mask);
    if (UpdateInfo)
        UpdateInfo->build_field_mask(mask);

    for (i = 1; i <= table_def->num_fields(); i++) {
        if (FIELD_ISSET(i, mask) || table_def->is_volatile(i))
            continue;
        sql_append_const(",");
        sql_append_field(i);
        sql_append_const("=");
        format_sql_value(table_def->default_value(i),
                 table_def->field_type(i));
    }

    return 0;
}

int ConnectorProcess::process_insert(DtcJob *Task)
{
    int Ret;

    set_title("INSERT...");
    init_sql_buffer();
    init_table_name(Task->request_key(), table_def->field_type(0));

    sql_append_const("INSERT INTO ");
    sql_append_table();
    sql_append_const(" SET ");

    std::map<std::string, std::string> fieldValues;
    if (Task->request_key()) {
        fieldValues[table_def->field_name(0)] = value_to_str(
            Task->request_key(), table_def->field_type(0));
    }

    if (Task->request_operation()) {
        const DTCFieldValue *updateInfo = Task->request_operation();
        for (int i = 0; i < updateInfo->num_fields(); ++i) {
            int fid = updateInfo->field_id(i);
            if (table_def->is_volatile(fid))
                continue;
            fieldValues[table_def->field_name(fid)] =
                value_to_str(updateInfo->field_value(i),
                         updateInfo->field_type(i));
        }
    }

    for (std::map<std::string, std::string>::iterator iter =
             fieldValues.begin();
         iter != fieldValues.end(); ++iter) {
        sql_append_string(&left_quote, 1);
        sql_append_string(iter->first.c_str(), iter->first.length());
        sql_append_string(&right_quote, 1);
        sql_append_const("=");
        sql_append_string(iter->second.c_str(), iter->second.length());
        sql_append_const(",");
    }

    if (sql.at(-1) == ',')
        sql.trunc(-1);

    if (error_no != 0) { // 主要检查PrintfAppend是否发生过错误
        Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
        log4cplus_error("error occur: %d", error_no);
        return (-1);
    }

    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    Ret = db_conn.do_query(DBName, sql.c_str());
    log4cplus_debug("INSERT %d %d", Ret, db_conn.get_raw_err_no());

    if (Ret != 0) {
        int err = db_conn.get_err_no();
        Task->set_error_dup(err, __FUNCTION__, db_conn.get_err_msg());
        if (err != -ER_DUP_ENTRY)
            log4cplus_warning("db query error: %s",
                      db_conn.get_err_msg());
        else
            log4cplus_info("db query error: %s",
                       db_conn.get_err_msg());
        return (-1);
    }

    Task->resultInfo.set_affected_rows(db_conn.affected_rows());
    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    if (table_def->has_auto_increment()) {
        uint64_t id = db_conn.insert_id();
        if (id) {
            Task->resultInfo.set_insert_id(id);
            if (table_def->key_auto_increment())
                Task->resultInfo.set_key(id);
        }
    }

    return (0);
}

int ConnectorProcess::process_update(DtcJob *Task)
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

    set_title("UPDATE...");
    init_sql_buffer();
    init_table_name(Task->request_key(), table_def->field_type(0));

    sql_append_const("UPDATE ");
    sql_append_table();
    sql_append_const(" SET ");
    update_field_concate(Task->request_operation());

    // key
    sql_append_const(" WHERE ");
    sql_append_field(0);
    sql_append_const("=");
    format_sql_value(Task->request_key(), table_def->field_type(0));

    // condition
    if (condition_concate(Task->request_condition()) != 0) {
        Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
                "Volatile condition not allowed");
        return (-7);
    }

    if (error_no != 0) { // 主要检查PrintfAppend是否发生过错误
        Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
        log4cplus_error("error occur: %d", error_no);
        return (-1);
    }

    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    Ret = db_conn.do_query(DBName, sql.c_str());
    log4cplus_debug("UPDATE %d %d", Ret, db_conn.get_raw_err_no());
    if (Ret != 0) {
        int err = db_conn.get_err_no();
        Task->set_error_dup(err, __FUNCTION__, db_conn.get_err_msg());
        if (err != -ER_DUP_ENTRY)
            log4cplus_warning("db query error: %s",
                      db_conn.get_err_msg());
        else
            log4cplus_info("db query error: %s",
                       db_conn.get_err_msg());
        return -1;
    }

    Task->resultInfo.set_affected_rows(db_conn.affected_rows());
    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    return (0);
}

int ConnectorProcess::process_delete(DtcJob *Task)
{
    int Ret;

    set_title("DELETE...");
    init_sql_buffer();
    init_table_name(Task->request_key(), table_def->field_type(0));

    sql_append_const("DELETE FROM ");
    sql_append_table();

    // key
    sql_append_const(" WHERE ");
    sql_append_field(0);
    sql_append_const("=");
    format_sql_value(Task->request_key(), table_def->field_type(0));

    // condition
    if (condition_concate(Task->request_condition()) != 0) {
        Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
                "Volatile condition not allowed");
        return (-7);
    }

    if (error_no !=
        0) { // 主要检查PrintfAppend是否发生过错误，这里统一检查一次
        Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
        log4cplus_error("error occur: %d", error_no);
        return (-1);
    }

    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    Ret = db_conn.do_query(DBName, sql.c_str());
    log4cplus_debug("DELETE %d %d", Ret, db_conn.get_raw_err_no());
    if (Ret != 0) {
        Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                    db_conn.get_err_msg());
        log4cplus_warning("db query error: %s", db_conn.get_err_msg());
        return (-1);
    }

    Task->resultInfo.set_affected_rows(db_conn.affected_rows());
    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    return (0);
}

int ConnectorProcess::do_process(DtcJob* Task)
{
    log4cplus_info("line:%d" ,__LINE__);
    if (Task == NULL) {
        log4cplus_error("Task is NULL!%s", "");
        return (-1);
    }
    log4cplus_info("line:%d" ,__LINE__);
    table_def = TableDefinitionManager::instance()->get_cur_table_def();

    switch (Task->request_code()) {
    case DRequest::TYPE_PASS:
    case DRequest::Purge:
    case DRequest::Flush:
        return 0;
    
    case DRequest::Get:
        return process_select(Task);

    case DRequest::Insert:
        return process_insert(Task);

    case DRequest::Update:
        return process_update(Task);

    case DRequest::Delete:
        return process_delete(Task);

    case DRequest::Replace:
        return process_replace(Task);

    // case DRequest::ReloadConfig:
    //     return process_reload_config(Task);

    default:
        Task->set_error(-EC_BAD_COMMAND, __FUNCTION__,
                "invalid request-code");
        return (-1);
    }
}

int ConnectorProcess::process_replace(DtcJob *Task)
{
    int Ret;

    set_title("REPLACE...");
    init_sql_buffer();
    init_table_name(Task->request_key(), table_def->field_type(0));

    sql_append_const("REPLACE INTO ");
    sql_append_table();
    sql_append_const(" SET ");
    sql_append_field(0);
    sql_append_const("=");
    format_sql_value(Task->request_key(), table_def->field_type(0));
    sql_append_const(",");

    /* 补全缺失的默认值 */
    if (Task->request_operation())
        update_field_concate(Task->request_operation());
    else if (sql.at(-1) == ',') {
        sql.trunc(-1);
    }

    if (error_no != 0) { // 主要检查PrintfAppend是否发生过错误
        Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
        log4cplus_error("error occur: %d", error_no);
        return (-1);
    }
    if (error_no != 0) { // 主要检查PrintfAppend是否发生过错误
        Task->set_error(-EC_ERROR_BASE, __FUNCTION__, "printf error");
        log4cplus_error("error occur: %d", error_no);
        return (-1);
    }

    log4cplus_debug("db: %s, sql: %s", DBName, sql.c_str());

    Ret = db_conn.do_query(DBName, sql.c_str());
    log4cplus_debug("REPLACE %d %d", Ret, db_conn.get_raw_err_no());

    if (Ret != 0) {
        Task->set_error_dup(db_conn.get_err_no(), __FUNCTION__,
                    db_conn.get_err_msg());
        log4cplus_warning("db query error: %s", db_conn.get_err_msg());
        return (-1);
    }

    Task->resultInfo.set_affected_rows(db_conn.affected_rows());

    log4cplus_debug("%s",
            "ConnectorProcess::ProcessReplaceTask() successful.");

    return 0;
}

ConnectorProcess::~ConnectorProcess()
{
}

void ConnectorProcess::init_title(int group, int role)
{
    title_prefix_size = snprintf(name, sizeof(name), "connector%d%c", group,
                     MACHINEROLESTRING[role]);
    memcpy(title, name, title_prefix_size);
    title[title_prefix_size++] = ':';
    title[title_prefix_size++] = ' ';
    title[title_prefix_size] = '\0';
    title[sizeof(title) - 1] = '\0';
}

void ConnectorProcess::set_title(const char *status)
{
    strncpy(title + title_prefix_size, status,
        sizeof(title) - 1 - title_prefix_size);
    set_proc_title(title);
}

int ConnectorProcess::process_reload_config(DtcJob *Task)
{
    int cacheKey = DbConfig::get_shm_id(g_dtc_config->get_config_node());;

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