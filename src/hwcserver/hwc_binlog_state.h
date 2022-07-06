/*
 * =====================================================================================
 *
 *       Filename:  sync_binlog_state.h
 *
 *    Description:  sync_binlog_state class definition.
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

#ifndef HWC_BINLOG_STATE_H_
#define HWC_BINLOG_STATE_H_
// local
#include "hwc_state_base.h"

class HwcSyncUnit;

SyncState(Binlog)

private:
    HwcSyncUnit* p_hwc_sync_unit_;

ENDFLAG

#endif