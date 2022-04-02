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
// libs/api/cc_api/include
#include "dtcapi.h"

enum E_HWC_SYNC_ERROR_ID
{
    E_HWC_SYNC_NORMAL_EXIT,
    E_HWC_ACCOUNT_CHECK_FAILL = -100,
    E_HWC_SYNC_DTC_ERROR
};

class DTCJobOperation;

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

    const char* ErrorMessage() {
        return s_err_msg_;
    }

public:
    static int direct_query_sql_server(DTCJobOperation* p_job);

private:
    int i_limit_;
    DTC::Server* p_master_;
    JournalID o_journal_id_;
    char s_err_msg_[256];
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
