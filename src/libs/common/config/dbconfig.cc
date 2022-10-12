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
*/
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <dlfcn.h>
#include <ctype.h>

#include "value.h"
#include "protocol.h"
#include "../log/log.h"
#include "config/dbconfig.h"
#include "../table/table_def.h"
#include "config.h"
#include <libgen.h>
#include "../../../core/global.h"

#define HELPERPATHFORMAT "@dtcd[%d]helper%d%c"
#define DTC_EXPIRE_TIME_FIELD "_dtc_sys_expiretime"

static int ParseDbLine(const char *buf, uint16_t *dbIdx)
{
    char *p = (char *)buf; // discard const

    int n = 0;
    while (*p) {
        if (!isdigit(p[0]))
            break;
        int begin, end;
        begin = strtol(p, &p, 0);
        if (*p != '-')
            end = begin;
        else {
            p++;
            if (!isdigit(p[0]))
                break;
            end = strtol(p, &p, 0);
        }
        while (begin <= end)
            dbIdx[n++] = begin++;

        if (p[0] != ',')
            break;
        else
            p++;
    }
    return n;
}

/* 前置空格已经过滤了 */
static char *skip_blank(char *p)
{
    char *iter = p;
    while (!isspace(*iter) && *iter != '\0')
        ++iter;

    *iter = '\0';
    return p;
}

bool MachineConfig::is_same(MachineConfig *mach)
{
    // no need check NULL pointer as NULL is '\0'
    if (strcmp(this->role[0].addr, mach->role[0].addr))
        return false;
    if (strcmp(this->role[0].user, mach->role[0].user))
        return false;
    if (strcmp(this->role[0].pass, mach->role[0].pass))
        return false;
    if (mode != mach->mode)
        return false;
    if (mode > 0) {
        if (strcmp(this->role[1].addr, mach->role[1].addr))
            return false;
        if (strcmp(this->role[1].user, mach->role[1].user))
            return false;
        if (strcmp(this->role[1].pass, mach->role[1].pass))
            return false;
    }
    return true;
}

bool FieldConfig::is_same(FieldConfig *field)
{
    if (type != field->type && size != field->size &&
        flags != field->flags && strcmp(name, field->name))
        return false;
    switch (type) {
    case DField::Signed:
    case DField::Unsigned:
    case DField::Float:
        if (dval.u64 != field->dval.u64)
            return false;
        break;
    case DField::String:
    case DField::Binary:
        if (dval.bin.len != field->dval.bin.len)
            return false;
        if (dval.bin.len == 0)
            return true;
        if (memcmp(dval.bin.ptr, field->dval.bin.ptr, dval.bin.len) !=
            0)
            return false;
        break;
    default:
        return false;
    }
    return true;
}

int DbConfig::load_key_hash(DTCConfig *raw)
{
    log4cplus_debug("tryto load key-hash plugin");

    /* init. */
    keyHashConfig.keyHashEnable = 0;
    keyHashConfig.keyHashFunction = 0;
    keyHashConfig.keyHashLeftBegin = 0;
    keyHashConfig.keyHashRightBegin = 0;

    /* not enable key-hash */
    std::string hashopen = raw->get_config_node()["props"]["hash.custom"].as<std::string>();
    if (str2int(hashopen.c_str(), 0) == 0) {
        log4cplus_debug("key-hash plugin disable");
        return 0;
    }

    /* read key_hash_module */
    std::string so = raw->get_config_node()["props"]["hash.custom.module"].as<std::string>();
    if (so.length() == 0) {
        log4cplus_info(
            "not set key-hash plugin name, use default value");
        so = DEFAULT_KEY_HASH_SO_NAME;
    }

    /* read key_hash_function */
    std::string var =
        raw->get_config_node()["props"]["hash.custom.functon"].as<std::string>();
    if (var.length() == 0) {
        log4cplus_error("not set key-hash plugin function name");
        var = DEFAULT_KEY_HASH_FUNCTION;
    }

    char *fun = 0;
    int isfunalloc = 0;
    const char *iter = strchr(var.c_str(), '(');
    if (NULL == iter) {
        /* 
         * 按照整个buffer来处理
         */
        keyHashConfig.keyHashLeftBegin = 1;
        keyHashConfig.keyHashRightBegin = -1;
        fun = (char *)var.c_str();
    } else {
        fun = strndup(var.c_str(), (size_t)(iter - var.c_str()));
        isfunalloc = 1;
        if (sscanf(iter, "(%d, %d)", &keyHashConfig.keyHashLeftBegin,
               &keyHashConfig.keyHashRightBegin) != 2) {
            free(fun);
            log4cplus_error(
                "key-hash plugin function format error, %s:%s",
                so.c_str(), var.c_str());
            return -1;
        }

        if (keyHashConfig.keyHashLeftBegin == 0)
            keyHashConfig.keyHashLeftBegin = 1;
        if (keyHashConfig.keyHashRightBegin == 0)
            keyHashConfig.keyHashRightBegin = -1;
    }

    /* 过滤fun中的空格*/
    fun = skip_blank(fun);

    void *dll = dlopen(so.c_str(), RTLD_NOW | RTLD_GLOBAL);
    if (dll == (void *)NULL) {
        if (isfunalloc)
            free(fun);
        log4cplus_error("dlopen(%s) error: %s", so.c_str(), dlerror());
        return -1;
    }

    /* find key-hash function in key-hash.so */
    keyHashConfig.keyHashFunction = (key_hash_interface)dlsym(dll, fun);
    if (keyHashConfig.keyHashFunction == NULL) {
        log4cplus_error(
            "key-hash plugin function[%s] not found in [%s]", fun,
            so.c_str());
        if (isfunalloc)
            free(fun);
        return -1;
    }

    /* check passed, enable key-hash */
    keyHashConfig.keyHashEnable = 1;

    log4cplus_info("key-hash plugin %s->%s(%d, %d) %s",
               basename((char *)so.c_str()), fun,
               keyHashConfig.keyHashLeftBegin,
               keyHashConfig.keyHashRightBegin,
               keyHashConfig.keyHashEnable ? "enable" : "disable");

    if (isfunalloc)
        free(fun);

    return 0;
}

bool DbConfig::compare_mach(DbConfig *config)
{
    bool found = true;
    if (this->machineCnt != config->machineCnt)
        return false;
    for (int i = 0; found && i < this->machineCnt; ++i) {
        found = false;
        for (int j = 0; j < config->machineCnt; ++j) {
            if (this->mach[i].is_same(config->mach + j)) {
                found = true;
                break;
            }
        }
        if (!found)
            return false;
    }
    return true;
}

bool DbConfig::Compare(DbConfig *config, bool compareMach)
{
    if (compareMach && !compare_mach(config))
        return false;

    if (depoly != config->depoly || keyFieldCnt != config->keyFieldCnt ||
        idxFieldCnt != config->idxFieldCnt ||
        fieldCnt != config->fieldCnt ||
        database_max_count != config->database_max_count ||
        dbDiv != config->dbDiv || dbMod != config->dbMod ||
        tblMod != config->tblMod || tblDiv != config->tblDiv) {
        log4cplus_error(
            "before origin after param, depoly:[%d,%d],keyFieldCnt:[%d,%d],idxFieldCnt:[%d,%d],fieldCnt:[%d,%d],database_max_count:[%d,%d],dbDiv:[%d,%d],dbMod:[%d,%d],tblMod:[%d,%d],tblDiv:[%d,%d]",
            depoly, config->depoly, keyFieldCnt,
            config->keyFieldCnt, idxFieldCnt, config->idxFieldCnt,
            fieldCnt, config->fieldCnt, database_max_count,
            config->database_max_count, dbDiv, config->dbDiv, dbMod,
            config->dbMod, tblMod, config->tblMod, tblDiv,
            config->tblDiv);
        return false;
    }

    for (int i = 0; i < fieldCnt; ++i) {
        if (!(field + i)->is_same(config->field + i)) {
            log4cplus_error("field%d does not match", i + 1);
            return false;
        }
    }

    return true;
}

int DbConfig::find_new_mach(DbConfig *config, std::vector<int> &newMach,
                std::map<int, int> &machMap)
{
    // be careful, same machine numbers, but change db distribution with new machine cause bug
    if (this->machineCnt == config->machineCnt)
        return 0;
    if (newMach.size() > 0)
        newMach.clear();
    if (machMap.size() > 0)
        machMap.clear();
    std::map<int, int> old2New;
    for (int i = 0; i < this->machineCnt; ++i) {
        bool found = false;
        for (int j = 0; j < config->machineCnt; ++j) {
            if (config->mach[j].is_same(this->mach + i)) {
                if (old2New.find(j) != old2New.end()) {
                    log4cplus_error(
                        "multi new machines to one old machine");
                    return 0;
                }
                found = true;
                machMap[i] = j;
                old2New[j] = i;
                break;
            }
        }
        if (!found) {
            newMach.push_back(i);
            log4cplus_debug("found new db machine No. %d", i + 1);
        }
    }
    return newMach.size();
}

int DbConfig::convert_case_sensitivity(
    std::string& s_val)
{
    int i = 0;
    while (s_val[i] != '\0' && i < s_val.length())
    {
        if (isupper(s_val[i])) {
            s_val[i] = tolower(s_val[i]);
        } else if (islower(s_val[i])) {
            s_val[i] = toupper(s_val[i]);
        }
        ++i;
    }
    return i;
}

int get_db_machine_count()
{
    return 1;
}

std::string get_merge_string(YAML::Node node)
{
    std::string str = "";
    if(!node)
        return str;

    for(int i = 0; i < node.size(); i++)
    {
        str += node[i].as<string>();
    }

    return str;
}

int DbConfig::get_dtc_config(YAML::Node dtc_config, DTCConfig* raw, int i_server_type)
{
    std::string layer("");
    if(!dtc_config)
        return -1;

    log4cplus_info("primary table: %s", dtc_config["primary"]["table"].as<string>().c_str());

    switch (i_server_type)
    {
    case 0:
        layer = "hot";
        break;
    case 1:
        layer = "full";
        break;
    default:
        log4cplus_error("%s: %d.", "invalid layer", i_server_type);
        return -1;
    }
        
    log4cplus_info("layered storage: %s.", layer.c_str());

    const char *cp = NULL;
    char *p;

    //DB section
    if(dtc_config["primary"][layer])    //cache.datasource mode
    {
        machineCnt = get_db_machine_count();
        if (machineCnt <= 0) {
            log4cplus_error("%s", "invalid server_count");
            return -1;
        }
    }
    else{
        machineCnt = 0;
    }

    //Depoly
    if(dtc_config["primary"][layer]) //cache.datasource mode
    {
        YAML::Node node = dtc_config["primary"][layer]["real"];
        if(node.size() == 1) //single db
        {
            if(dtc_config["primary"][layer]["sharding"] && 
                dtc_config["primary"][layer]["sharding"]["table"]["last"].as<int>() - dtc_config["primary"][layer]["sharding"]["table"]["start"].as<int>() + 1 > 1)
                depoly = SINGLE_DB_SHARDING_TAB;
            else
                depoly = SINGLE;
        }
        else if(node.size() > 1) //multi db
        {
            if(dtc_config["primary"][layer]["sharding"] && 
            dtc_config["primary"][layer]["sharding"]["table"]["last"].as<int>() - dtc_config["primary"][layer]["sharding"]["table"]["start"].as<int>() + 1 > 1)
                depoly = SHARDING_DB_SHARDING_TAB;
            else
                depoly = SHARDING_DB_ONE_TAB;
        }
        else
        {
            log4cplus_error("%s", "invalid server_count");
            return -1;
        }
    }
    else
    {
        depoly = SINGLE;
    }

    //DB Name
    if(dtc_config["primary"][layer]) //cache.datasource mode
    {
        YAML::Node node = dtc_config["primary"][layer]["real"][0]["db"];
        if(!node)
        {
            log4cplus_error("primary.layer.real db not defined");
            return -1;
        }
        else
        {
            if(node.IsScalar())
                dbName = STRDUP(node.as<string>().c_str());
            else
                dbName = STRDUP(get_merge_string(node["prefix"]).c_str());
        }
    }
    dstype = 0;
    checkTable = 0;

    //TODO: string key supporting.
    // key-hash dll
    //if (load_key_hash(raw) != 0)
    //    return -1;

    if(dtc_config["primary"][layer])
    {
        if ((depoly & 1) == 0) {    //single db
            if (strchr(dbName, '%') != NULL) {
                log4cplus_error(
                    "Invalid [DATABASE_CONF].database_name, cannot contain symbol '%%'");
                return -1;
            }

            dbDiv = 1;
            dbMod = 1;
            dbFormat = dbName;
        } else {
            dbDiv = 1;
            dbMod = dtc_config["primary"][layer]["real"].size();

            if (dbMod > 100) {
                log4cplus_error(
                    "invalid [DATABASE_CONF].DbMod = %d, mod value too large",
                    dbMod);
                return -1;
            }
            p = strchr(dbName, '%');
            if (p) {
                dbFormat = STRDUP(dbName);
                *p = '\0';
            } else {
                dbFormat = (char *)MALLOC(strlen(dbName) + 3);
                snprintf(dbFormat, strlen(dbName) + 3, "%s%%d", dbName);
            }
        }

        database_max_count = dtc_config["primary"][layer]["real"].size();
        if (database_max_count < 0 || database_max_count > 10000) {
            log4cplus_error("%s", "invalid [DATABASE_CONF].DbMax");
            return -1;
        }
        if (database_max_count < (int)dbMod) {
            log4cplus_warning(
                "invalid [TABLE_CONF].DbMax too small, increase to %d",
                dbMod);
            database_max_count = dbMod;
        }

        //Table section with DATABASE_IN_ADDITION.
        YAML::Node node = dtc_config["primary"][layer]["sharding"]["table"]["prefix"];
        if(node)
        {
            tblName = STRDUP(get_merge_string(node).c_str());
        }
        else if(dtc_config["primary"]["table"])
        {
            tblName = STRDUP(dtc_config["primary"]["table"].as<string>().c_str());
        }
        else
        {
            log4cplus_error("table name not defined");
            return -1;
        }

        if ((depoly & 2) == 0) {
            if (strchr(tblName, '%') != NULL) {
                log4cplus_error(
                    "Invalid table_name, cannot contain symbol '%%'");
                return -1;
            }

            tblDiv = 1;
            tblMod = 1;
            tblFormat = tblName;
        } else {
            tblDiv = dtc_config["primary"][layer]["real"].size();
            tblMod = dtc_config["primary"][layer]["sharding"]["table"]["last"].as<int>() - dtc_config["primary"][layer]["sharding"]["table"]["start"].as<int>() + 1;

            if(tblDiv == 0 || tblMod == 0) {
                log4cplus_error("invalid [TABLE_CONF].tblDiv = %d, tblMod = %d",
                        tblDiv, tblMod);
                return -1;
            }

            p = strchr(tblName, '%');
            if (p) {
                tblFormat = STRDUP(tblName);
                *p = '\0';
            } else {
                tblFormat = (char *)MALLOC(strlen(tblName) + 3);
                snprintf(tblFormat, strlen(tblName) + 3, "%s%%d",
                    tblName);
            }
        }
    }
    else
    {
        //Table section with CACHE_ONLY
        YAML::Node node = dtc_config["primary"]["table"];
        if(!node)
        {
            log4cplus_error("table name not defined");
            return -1;
        }
        tblName = STRDUP(node.as<string>().c_str());
        if ((depoly & 2) == 0) 
        {
            if (strchr(tblName, '%') != NULL) {
                log4cplus_error(
                    "Invalid table_name, cannot contain symbol '%%'");
                return -1;
            }

            tblDiv = 1;
            tblMod = 1;
            tblFormat = tblName;
        }
    }

    fieldCnt = dtc_config["primary"]["cache"]["field"].size();
    if (fieldCnt <= 0 || fieldCnt > 240) {
        log4cplus_error("invalid [TABLE_CONF].field_count:%d", fieldCnt);
        return -1;
    }

    keyFieldCnt = 1;
    if (keyFieldCnt <= 0 || keyFieldCnt > 32 || keyFieldCnt > fieldCnt) {
        log4cplus_error("invalid [TABLE_CONF].key_count");
        return -1;
    }
    log4cplus_info("keyFieldCnt:%d" , keyFieldCnt);

    idxFieldCnt = 0;
    if (keyFieldCnt < 0 || keyFieldCnt + idxFieldCnt > fieldCnt) {
        log4cplus_error("invalid [TABLE_CONF].IndexFieldCount");
        return -1;
    }

    ordIns = 0;
    if (ordIns < 0) {
        log4cplus_error("bad [TABLE_CONF].ServerOrderInsert");
        return -1;
    }
    //Machine setction
    mach = (struct MachineConfig *)calloc(machineCnt,
                          sizeof(struct MachineConfig));
    if (!mach) {
        log4cplus_error("malloc failed, %m");
        return -1;
    }
    for (int i = 0; i < machineCnt; i++) {
        struct MachineConfig *m = &mach[i];

        m->helperType = MYSQL_HELPER;

        //master
        m->role[0].addr = STRDUP(dtc_config["primary"][layer]["real"][i]["addr"].as<string>().c_str());
        m->role[0].user = STRDUP(dtc_config["primary"][layer]["real"][i]["user"].as<string>().c_str());
        m->role[0].pass = STRDUP(dtc_config["primary"][layer]["real"][i]["pwd"].as<string>().c_str());
        m->role[0].optfile = STRDUP("../conf/my.conf");
        log4cplus_info("addr:%s,user:%s" , m->role[0].addr , m->role[0].user);

        /* master DB settings */
        if (m->role[0].addr == NULL) {
            log4cplus_error("missing database_address");
            return -1;
        }
        if (m->role[0].user == NULL)
            m->role[0].user = "";
        if (m->role[0].pass == NULL)
            m->role[0].pass = "";
        if (m->role[0].optfile == NULL)
            m->role[0].optfile = "";

        m->mode = 0;
                
        if(dtc_config["primary"][layer]["real"][i]["db"].IsScalar())
            m->dbCnt = 1;
        else
            m->dbCnt = dtc_config["primary"][layer]["real"][i]["db"]["last"].as<int>() - 
                        dtc_config["primary"][layer]["real"][i]["db"]["start"].as<int>() + 1;
        for (int j = 0; j < m->dbCnt; j++) {
            if (m->dbIdx[j] >= database_max_count) {
                log4cplus_error(
                    "dbConfig error, database_max_count=%d, machine[%d].dbIdx=%d",
                    database_max_count, j + 1, m->dbIdx[j]);
                return -1;
            }
        }
        /* Helper number alter */
        m->gprocs[0] = raw->get_int_val(NULL, "Procs", 1);
        if (m->gprocs[0] < 1)
            m->gprocs[0] = 0;
        m->gprocs[1] = raw->get_int_val(NULL, "WriteProcs", 1);
        if (m->gprocs[1] < 1)
            m->gprocs[1] = 0;
        m->gprocs[2] = raw->get_int_val(NULL, "CommitProcs", 1);
        if (m->gprocs[2] < 1)
            m->gprocs[2] = 0;
        /* Helper Queue Size */
        m->gqueues[0] = raw->get_int_val(NULL, "QueueSize", 0);
        m->gqueues[1] =
            raw->get_int_val(NULL, "WriteQueueSize", 0);
        m->gqueues[2] =
            raw->get_int_val(NULL, "CommitQueueSize", 0);
        if (m->gqueues[0] <= 0)
            m->gqueues[0] = 1000;
        if (m->gqueues[1] <= 0)
            m->gqueues[1] = 1000;
        if (m->gqueues[2] <= 0)
            m->gqueues[2] = 10000;
        if (m->gqueues[0] <= 2)
            m->gqueues[0] = 2;
        if (m->gqueues[1] <= 2)
            m->gqueues[1] = 2;
        if (m->gqueues[2] <= 1000)
            m->gqueues[2] = 1000;
        if (m->dbCnt == 0) // database_index is NULL, no helper needed
        {
            m->gprocs[0] = 0;
            m->gprocs[1] = 0;
            m->gprocs[2] = 0;
            m->mode = 0;
        }
        switch (m->mode) {
        case BY_SLAVE:
        case BY_DB:
        case BY_TABLE:
        case BY_KEY:
            m->gprocs[3] = m->gprocs[0];
            m->gqueues[3] = m->gqueues[0];
            if (slaveGuard == 0) {
                slaveGuard = raw->get_int_val(
                    "DATABASE_CONF", "SlaveGuardTime", 15);
                if (slaveGuard < 5)
                    slaveGuard = 5;
            }
            break;
        default:
            m->gprocs[3] = 0;
            m->gqueues[3] = 0;
        }
        m->procs = m->gprocs[0] + m->gprocs[1] + m->gprocs[2];
        procs += m->procs;
    }
    //Field section
    field = (struct FieldConfig *)calloc(fieldCnt,
                         sizeof(struct FieldConfig));
    if (!field) {
        log4cplus_error("malloc failed, %m");
        return -1;
    }
    autoinc = -1;
    lastmod = -1;
    lastcmod = -1;
    lastacc = -1;
    compressflag = -1;
    expireTime = -1;
    for (int i = 0; i < fieldCnt; i++) {
        struct FieldConfig *f = &field[i];

        f->name = STRDUP(dtc_config["primary"]["cache"]["field"][i]["name"].as<string>().c_str());
        if (f->name == NULL) {
            log4cplus_error("field name missing for %d",
                    i);
            return -1;
        }
        f->type = -1;
        string type = dtc_config["primary"]["cache"]["field"][i]["type"].as<string>();
        if(type == "signed")
            f->type = 1;
        else if(type == "unsigned")
            f->type = 2;
        else if(type == "float")
            f->type = 3;
        else if(type == "string")
            f->type = 4;
        else if(type == "binary")
            f->type = 5;

        if (f->type <= 0 || f->type > 5) {
            log4cplus_error("Invalid value [%d].field_type:%s",
                    i, type.c_str());
            return -1;
        }
        f->size = dtc_config["primary"]["cache"]["field"][i]["size"].as<int>();
        if (f->size == -1) {
            log4cplus_error("field size missing for %d",
                    i);
            return -1;
        }
        if (i >= keyFieldCnt &&
            i < keyFieldCnt + idxFieldCnt) { // index field
            if (field[i].size > 255) {
                log4cplus_error(
                    "index field[%s] size must less than 256",
                    f->name);
                return -1;
            }

            std::string idx_order = dtc_config["primary"]["cache"]["order"].as<std::string>();
            if (idx_order == "desc")
                f->flags |= DB_FIELD_FLAGS_DESC_ORDER;
            else
                log4cplus_debug("index field[%s] order: ASC",
                        f->name);
        }
        if (field[i].size >= (64 << 20)) {
            log4cplus_error("field[%s] size must less than 64M",
                    f->name);
            return -1;
        }
        if (i >= keyFieldCnt &&
            raw->get_int_val(NULL, "ReadOnly", 0) > 0)
            f->flags |= DB_FIELD_FLAGS_READONLY;
        if(dtc_config["primary"]["cache"]["field"][i]["unique"])
        {
            if(dtc_config["primary"]["cache"]["field"][i]["unique"].as<int>() > 0)
            {
                log4cplus_debug("set index: %d unique", i);
                f->flags |= DB_FIELD_FLAGS_UNIQ;
            }
        }
        if (raw->get_int_val(NULL, "Volatile", 0) > 0) {
            if (i < keyFieldCnt) {
                log4cplus_error(
                    "field%d: key field can't be volatile",
                    i + 1);
                return -1;
            }
            if ((f->flags & DB_FIELD_FLAGS_UNIQ)) {
                log4cplus_error(
                    "field%d: uniq field can't be volatile",
                    i + 1);
                return -1;
            }
            f->flags |= DB_FIELD_FLAGS_VOLATILE;
        }
        if (raw->get_int_val(NULL, "Discard", 0) > 0) {
            if (i < keyFieldCnt) {
                log4cplus_error(
                    "field%d: key field can't be discard",
                    i + 1);
                return -1;
            }
            if ((f->flags & DB_FIELD_FLAGS_UNIQ)) {
                log4cplus_error(
                    "field%d: uniq field can't be discard",
                    i + 1);
                return -1;
            }
            f->flags |= DB_FIELD_FLAGS_DISCARD |
                    DB_FIELD_FLAGS_VOLATILE;
        }
        if (!strcmp(f->name, DTC_EXPIRE_TIME_FIELD)) {
            if (f->type != DField::Unsigned &&
                f->type != DField::Signed) {
                log4cplus_error(
                    "field%d: expire time field byte must be unsigned",
                    i + 1);
                return -1;
            }
            if (expireTime >= 0) {
                log4cplus_error(
                    "field%d already defined as expire time",
                    expireTime + 1);
                return -1;
            }
            expireTime = i;
        }
        /* ATTN: must be last one */
        if(dtc_config["primary"]["cache"]["field"][i]["default"])
        {
            YAML::Node default_node = dtc_config["primary"]["cache"]["field"][i]["default"];
            std::string default_val = default_node.as<std::string>();
            if (default_val == "auto_increment") {
                if (f->type != DField::Unsigned &&
                    f->type != DField::Signed) {
                    log4cplus_error(
                        "field%d: auto_increment field byte must be unsigned",
                        i + 1);
                    return -1;
                }
                if (autoinc >= 0) {
                    log4cplus_error(
                        "field%d already defined as auto_increment",
                        autoinc + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_DISCARD)) {
                    log4cplus_error(
                        "field%d: auto_increment can't be Discard",
                        autoinc + 1);
                    return -1;
                }
                autoinc = i;
                f->flags |= DB_FIELD_FLAGS_READONLY;
            } else if (default_val == "lastmod") {
                if (i < keyFieldCnt) {
                    log4cplus_error("key field%d can't be lastmod",
                            i + 1);
                    return -1;
                }
                if ((f->type != DField::Unsigned &&
                    f->type != DField::Signed) ||
                    f->size < 4) {
                    log4cplus_error(
                        "field%d: lastmod field byte must be unsigned & size>=4",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_UNIQ)) {
                    log4cplus_error(
                        "field%d: lastmod field byte can't be field_unique",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_DISCARD)) {
                    log4cplus_error(
                        "field%d: lastmod can't be Discard",
                        autoinc + 1);
                    return -1;
                }
                if (lastmod >= 0) {
                    log4cplus_error(
                        "field%d already defined as lastmod",
                        lastmod + 1);
                    return -1;
                }
                lastmod = i;
            } else if (default_val == "lastcmod") {
                if (i < keyFieldCnt) {
                    log4cplus_error("key field%d can't be lastcmod",
                            i + 1);
                    return -1;
                }
                if ((f->type != DField::Unsigned &&
                    f->type != DField::Signed) ||
                    f->size < 4) {
                    log4cplus_error(
                        "field%d: lastcmod field byte must be unsigned & size>=4",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_UNIQ)) {
                    log4cplus_error(
                        "field%d: lastcmod field byte can't be field_unique",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_DISCARD)) {
                    log4cplus_error(
                        "field%d: lastcmod can't be Discard",
                        autoinc + 1);
                    return -1;
                }
                if (lastcmod >= 0) {
                    log4cplus_error(
                        "field%d already defined as lastcmod",
                        lastcmod + 1);
                    return -1;
                }
                lastcmod = i;
            } else if (default_val == "lastacc") {
                if (i < keyFieldCnt) {
                    log4cplus_error("key field%d can't be lastacc",
                            i + 1);
                    return -1;
                }
                if ((f->type != DField::Unsigned &&
                    f->type != DField::Signed) ||
                    f->size != 4) {
                    log4cplus_error(
                        "field%d: lastacc field byte must be unsigned & size==4",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_UNIQ)) {
                    log4cplus_error(
                        "field%d: lastacc field byte can't be field_unique",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_DISCARD)) {
                    log4cplus_error(
                        "field%d: lastacc can't be Discard",
                        autoinc + 1);
                    return -1;
                }
                if (lastacc >= 0) {
                    log4cplus_error(
                        "field%d already defined as lastacc",
                        lastacc + 1);
                    return -1;
                }
                lastacc = i;
            } else if (default_val == "compressflag") {
                if (i < keyFieldCnt) {
                    log4cplus_error(
                        "key field%d can't be compressflag",
                        i + 1);
                    return -1;
                }
                if ((f->type != DField::Unsigned &&
                    f->type != DField::Signed) ||
                    f->size != 8) {
                    log4cplus_error(
                        "field%d: compressflag field byte must be unsigned & size==8",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_UNIQ)) {
                    log4cplus_error(
                        "field%d: compressflag field byte can't be field_unique",
                        i + 1);
                    return -1;
                }
                if ((f->flags & DB_FIELD_FLAGS_DISCARD)) {
                    log4cplus_error(
                        "field%d: compressflag can't be Discard",
                        i + 1);
                    return -1;
                }
                if (compressflag >= 0) {
                    log4cplus_error(
                        "field%d already defined as compressflag",
                        i + 1);
                    return -1;
                }
                compressflag = i;
            } else {
                if (i == 0 && default_val.length() > 0) {
                    log4cplus_error(
                        "specify DefaultValue for Field1 is invalid");
                    return -1;
                }
                switch (f->type) {
                case DField::Unsigned:
                    f->dval.Set(default_node.as<int>());
                    break;
                case DField::Signed:
                    f->dval.Set(default_node.as<int>());
                    break;
                case DField::Float:
                    f->dval.Set(default_node.as<float>());
                    break;
                case DField::String:
                case DField::Binary:
                    int len;
                    cp = default_val.c_str();
                    if (!cp || !cp[0]) {
                        f->dval.Set(NULL, 0);
                        break;
                    } else if (cp[0] == '"') {
                        /* Decode quoted string */
                        cp++;
                        len = strlen(cp);
                        if (cp[len - 1] != '"') {
                            log4cplus_error(
                                "field%d: unmatched quoted default value",
                                i + 1);
                            return -1;
                        }
                        len--;
                    } else if (cp[0] == '0' &&
                        (cp[1] | 0x20) == 'x') {
                        /* Decode hex value */
                        int j = 2;
                        len = 0;
                        while (cp[j]) {
                            char v;
                            if (cp[j] == '-')
                                continue;
                            if (cp[j] >= '0' &&
                                cp[j] <= '9')
                                v = cp[j] - '0';
                            else if (cp[j] >= 'a' &&
                                cp[j] <= 'f')
                                v = cp[j] - 'a' + 10;
                            else if (cp[j] >= 'A' &&
                                cp[j] <= 'F')
                                v = cp[j] - 'A' + 10;
                            else {
                                log4cplus_error(
                                    "field%d: invalid hex default value",
                                    i + 1);
                                return -1;
                            }
                            j++;
                            if (cp[j] >= '0' &&
                                cp[j] <= '9')
                                v = (v << 4) + cp[j] -
                                    '0';
                            else if (cp[j] >= 'a' &&
                                cp[j] <= 'f')
                                v = (v << 4) + cp[j] -
                                    'a' + 10;
                            else if (cp[j] >= 'A' &&
                                cp[j] <= 'F')
                                v = (v << 4) + cp[j] -
                                    'A' + 10;
                            else {
                                log4cplus_error(
                                    "field%d: invalid hex default value",
                                    i + 1);
                                return -1;
                            }
                            j++;
                            //buf[len++] = v;
                            len++;
                        }
                    } else {
                        log4cplus_error(
                            "field%d: string default value must quoted or hex value",
                            i + 1);
                        return -1;
                    }

                    if (len > f->size) {
                        log4cplus_error(
                            "field%d: default value size %d truncated to %d",
                            i + 1, len, f->size);
                        return -1;
                    }
                    if (len == 0)
                        f->dval.Set(NULL, 0);
                    else {
                        char *p = (char *)MALLOC(len + 1);
                        memcpy(p, cp, len);
                        p[len] = '\0';
                        f->dval.Set(p, len);
                    }
                    break;
                }
                f->flags |= DB_FIELD_FLAGS_HAS_DEFAULT;
            }
        }
        
        if (dtc_config["primary"]["cache"]["field"][i]["nullable"]) {
            YAML::Node nullable_node = dtc_config["primary"]["cache"]["field"][i]["nullable"];
            int null_val = nullable_node.as<int>();
            if (null_val) {
                if (f->flags & DB_FIELD_FLAGS_UNIQ) {
                    log4cplus_error(
                        "field%d: can't be null",
                        i + 1);
                    return -1;
                }
                f->flags |= DB_FIELD_FLAGS_NULLABLE;
            }
        }
    }

    if (field[0].type == DField::Float) {
        log4cplus_error("%s", "FloatPoint key not supported");
        return -1;
    }

    if (((field[0].type == DField::String ||
          field[0].type == DField::Binary) &&
         keyHashConfig.keyHashEnable == 0) ||
        autoinc == 0) {
        if (machineCnt != 1) {
            log4cplus_error(
                "%s",
                "String/Binary/AutoInc key require server_count==1");
            return -1;
        }
        if (database_max_count != 1) {
            log4cplus_error(
                "%s",
                "String/Binary/AutoInc key require database_max_count==1");
            return -1;
        }
        if (depoly != 0) {
            log4cplus_error(
                "%s",
                "String/Binary/AutoInc key require Depoly==0");
            return -1;
        }
    }
    // expire time only support uniq key
    if (expireTime != -1 && !(field[0].flags & DB_FIELD_FLAGS_UNIQ)) {
        log4cplus_error("%s", "expire time only support uniq key");
        return -1;
    }
    if (keyFieldCnt > 1) {
        for (int j = 0; j < keyFieldCnt; j++) {
            struct FieldConfig *f1 = &field[j];
            if (f1->type != DField::Signed &&
                f1->type != DField::Unsigned) {
                log4cplus_error(
                    "%s",
                    "Only Multi-Integer-Key supported");
                return -1;
            }
        }
    }
    return 0;
}

struct DbConfig *DbConfig::load_buffered(char *buf)
{
    DTCConfig *raw = new DTCConfig();
    if (raw->load_yaml_buffer(buf) < 0) {
        delete raw;
        return NULL;
    }

    DbConfig *dbConfig =
        (struct DbConfig *)calloc(1, sizeof(struct DbConfig));
    dbConfig->cfgObj = raw;
    if (dbConfig->get_dtc_config(raw->get_config_node(), raw, HOT) == -1) {
        log4cplus_error("get config error, destory now.");
        dbConfig->destory();
        dbConfig = NULL;
    }

    return dbConfig;
}

struct DbConfig *DbConfig::Load(const char *file)
{
    DTCConfig *raw = new DTCConfig();
    if (raw->load_yaml_file(file) < 0) {
        delete raw;
        return NULL;
    }

    DbConfig *dbConfig =
        (struct DbConfig *)calloc(1, sizeof(struct DbConfig));
    dbConfig->cfgObj = raw;
    if (dbConfig->get_dtc_config(raw->get_config_node(), raw, HOT) == -1) {
        dbConfig->destory();
        dbConfig = NULL;
    }
    return dbConfig;
}

struct DbConfig *DbConfig::Load(DTCConfig *raw , int i_server_type)
{
    DbConfig *dbConfig =
        (struct DbConfig *)calloc(1, sizeof(struct DbConfig));
    dbConfig->cfgObj = raw;
    if (dbConfig->get_dtc_config(raw->get_config_node(), raw, i_server_type) == -1) {
        dbConfig->destory();
        dbConfig = NULL;
    }
    return dbConfig;
}

bool DbConfig::build_path(char *path, int n, int pid, int group, int role,
              int type)
{
    if (type == DUMMY_HELPER || type == DTC_HELPER)
        return false;
    memset(path, 0, n);
    snprintf(path, n - 1, HELPERPATHFORMAT, pid, group,
         MACHINEROLESTRING[role]);
    return true;
}

int DbConfig::get_dtc_mode(YAML::Node dtc_config)
{
    if(dtc_config["primary"]["hot"])
        return DTC_MODE_DATABASE_ADDITION;
    else 
        return DTC_MODE_CACHE_ONLY;
}

std::string DbConfig::get_shm_size(YAML::Node dtc_config)
{
    if(dtc_config["props"]["shm.mem.size"])
        return dtc_config["props"]["shm.mem.size"].as<string>();
    else 
        return "0";
}

int DbConfig::get_shm_id(YAML::Node dtc_config)
{
    if(dtc_config["props"]["listener.port.dtc"])
        return dtc_config["props"]["listener.port.dtc"].as<int>();
    else
        return 0;
}

std::string DbConfig::get_bind_addr(YAML::Node dtc_config)
{
    if(dtc_config["props"]["listener.port.dtc"])
    {
        int port = dtc_config["props"]["listener.port.dtc"].as<int>();
        char sz[200] = {0};
        sprintf(sz, "*:%d/tcp", port);
        return std::string(sz, strlen(sz));
    }
    else
        return "";
}

void DbConfig::set_helper_path(int serverId)
{
    //multi DTC datasource
    for (int i = 0; i < machineCnt; ++i) {
        MachineConfig *machine_config = &mach[i];
        for (int j = 0; j < ROLES_PER_MACHINE;
             ++j) //#define ROLES_PER_MACHINE 2
        {
            //check dtc datasource
            switch (machine_config->helperType) {
            case DUMMY_HELPER:
                break;
            case DTC_HELPER:
                snprintf(machine_config->role[j].path,
                     sizeof(machine_config->role[j].path) -
                         1,
                     "%s", machine_config->role[j].addr);
                log4cplus_info(
                    "build helper path, serverId:%d, addr:%s, path:%s, dstype:%d",
                    serverId, machine_config->role[j].addr,
                    machine_config->role[j].path, dstype);
                break;
            default:
                snprintf(machine_config->role[j].path,
                     sizeof(machine_config->role[j].path) -
                         1,
                     HELPERPATHFORMAT, serverId, i,
                     MACHINEROLESTRING[j]);
                log4cplus_info(
                    "build helper path, serverId:%d, addr:%s, path:%s, dstype:%d",
                    serverId, machine_config->role[j].addr,
                    machine_config->role[j].path, dstype);
            }

            // rocksdb only start one process
            if (dstype == 2)
                break;
        }
    }
}

void DbConfig::destory(void)
{
    if (this == NULL)
        return;

    // for (int i = 0; i < GROUPS_PER_MACHINE; i++) {
    //     FREE_IF(mach->role[i].addr);
    //     FREE_IF(mach->role[i].user);
    //     FREE_IF(mach->role[i].pass);
    //     FREE_IF(mach->role[i].optfile);
    //     FREE_IF(mach->role[i].dm);
    // }
    
    // machine hasn't dynamic objects
    FREE_IF(mach);

    if (field) {
        for (int i = 0; i < fieldCnt; i++) {
            switch (field[i].type) {
            case DField::String:
            case DField::Binary:
                if (field[i].dval.str.ptr)
                    FREE(field[i].dval.str.ptr);
            }
        }
        FREE(field);
    }

    if (dbFormat != dbName)
        FREE_IF(dbFormat);
    FREE_IF(dbName);

    if (tblFormat != tblName)
        FREE_IF(tblFormat);
    FREE_IF(tblName);

    FREE_IF(ordSql);
    DELETE(cfgObj);
    FREE((void *)this);
}

void DbConfig::dump_db_config(const struct DbConfig *cf)
{
    int i, j;

    printf("database_name: %s\n", cf->dbName);
    printf("database_number: (%d,%d)\n", cf->dbDiv, cf->dbMod);
    printf("server_count: %d\n", cf->machineCnt);
    for (i = 0; i < cf->machineCnt; i++) {
        struct MachineConfig *mach = &cf->mach[i];
        printf("\n");
        printf("Machine[%d].addr: %s\n", i, mach->role[0].addr);
        printf("Machine[%d].user: %s\n", i, mach->role[0].user);
        printf("Machine[%d].pass: %s\n", i, mach->role[0].pass);
        printf("Machine[%d].procs: %d\n", i, mach->procs);
        printf("Machine[%d].dbCnt: %d\n", i, mach->dbCnt);
        printf("Machine[%d].dbIdx: ", i);
        for (j = 0; j < mach->dbCnt; j++)
            printf("%d ", mach->dbIdx[j]);
        printf("\n");
    }
    printf("\nTableName: %s\n", cf->tblName);
    printf("TableNum: (%d,%d)\n", cf->tblDiv, cf->tblMod);
    printf("field_count: %d key %d\n", cf->fieldCnt, cf->keyFieldCnt);
    for (i = 0; i < cf->fieldCnt; i++)
        printf("Field[%d].name: %s, size: %d, type: %d\n", i,
               cf->field[i].name, cf->field[i].size, cf->field[i].type);
}
