/*
 * =====================================================================================
 *
 *       Filename:  sync_state_manager.h
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

#ifndef HWC_STATE_BASE_H_
#define HWC_STATE_BASE_H_

#include <assert.h>
// local
#include "comm.h"
#include "hwc_state_manager.h"
#include "system_state.h"
// common
#include "log/log.h"
#include "mem_check.h"

class HwcStateBase
{
public:
    HwcStateBase() : p_hwc_state_manager_(NULL) {};
    virtual ~HwcStateBase() {};

public:
    /// **************************
    /// 进入当前状态时，一些处理，比如: 初始化
    /// **************************
    virtual void Enter(void) = 0;

    /// **************************
    /// 退出当前状态时，一些处理
    /// **************************
    virtual void Exit(void) = 0;

    /// **************************
    /// 当前状态时，所要处理的业务逻辑，包括：状态跳转判断逻辑
    /// **************************
    virtual void HandleEvent() = 0;

protected:
    HwcStateManager* p_hwc_state_manager_;
};

// 减少冗余代码编写
#define SYNCSTATE_NAME(stateName)  stateName##State

#define SyncState(stateName)                                    \
class SYNCSTATE_NAME(stateName) : public HwcStateBase          \
{                                                               \
public:                                                         \
    SYNCSTATE_NAME(stateName)(HwcStateManager*);               \
    virtual ~SYNCSTATE_NAME(stateName)();                       \
                                                                \
public:                                                         \
    virtual void Enter(void);                                   \
    virtual void Exit(void);                                    \
    virtual void HandleEvent();                                 

#define ENDFLAG };


#endif