/*
 * =====================================================================================
 *
 *       Filename:  hwc_state_manager.h
 *
 *    Description:  HwcStateManager class definition.
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

#ifndef HWC_STATE_MANAGER_H_
#define HWC_STATE_MANAGER_H_

#include <utility>
#include <assert.h>
// local
#include "hwc_common.h"

class HwcStateBase;
class DbConfig;

class HwcStateManager
{
public:
    HwcStateManager();
    ~HwcStateManager();

public:
    void Start();
    void ChangeState(int iNewState);

public:
    void BindDBConfigParser(const DbConfig* const p_parser)
    { p_db_config_ = p_parser;}

    DbConfig* const GetDBConfigParser()
    { return p_db_config_; }

    int GetCurrentState() { return i_current_state_;}
    int GetNextState() { return i_next_state_;}
    int GetPreState() { return i_pre_state_;}

private:
    int i_pre_state_;
    int i_current_state_;
    int i_next_state_;
    HwcStateBase* p_current_state_;
    DbConfig* p_db_config_;
    HwcStateMap o_hwc_state_map_;
};

#endif