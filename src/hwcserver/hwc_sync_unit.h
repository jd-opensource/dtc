/*
 * =====================================================================================
 *
 *       Filename:  sync_unit.h
 *
 *    Description:  sync_unit class definition.
 *
 *        Version:  1.0
 *        Created:  13/01/2021
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  chenyujie, chenyujie28@jd.com@jd.com
 *        Company:  JD.com, Inc.
 *
 * =====================================================================================
 */

#ifndef __SYNC_UNIT_H
#define __SYNC_UNIT_H

#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
// local
#include "async_file.h"
// common
#include "log/log.h"
#include "task/task_request.h"
#include "task/task_pkey.h"
#include "mem_check.h"
#include "hwc_binlog_obj.h"
#include "table/hotbackup_table_def.h"
#include "table/table_def_manager.h"
// libs/api/cc_api/include
#include "dtcapi.h"

enum E_HWC_SYNC_ERROR_ID
{
    E_HWC_SYNC_NORMAL_EXIT,
    E_HWC_ACCOUNT_CHECK_FAILL = -100,
    E_HWC_SYNC_DTC_ERROR
};

class HwcSync
{
public:
    HwcSync(DTC::Server* p_server);
    ~HwcSync();

    int Run();

    void SetLimit(int iLimit) {
        // Here we need sync one by one
        i_limit_ = iLimit;
    }

public:
    int query_cold_server(DTCJobOperation* p_job , const DTCValue* key);
    void decode_hotbin_result(ResultSet* o_hot_res, const HwcBinlogCont& o_hwc_bin);
    void sql_statement_query(const DTCValue* p_key , std::string& s_sql);
    int get_current_time();

private:
    int i_limit_;
    DTC::Server* p_master_;
    JournalID o_journal_id_;
};

class HwcSyncUnit {
public:
    HwcSyncUnit();
    ~HwcSyncUnit();

    bool Run(DTC::Server* m , int limit = 1);

private:
    HwcSync* p_hwc_sync_;
};

#endif
