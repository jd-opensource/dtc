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
    /// Some processing when entering current state, such as: initialization
    /// **************************
    virtual void Enter(void) = 0;

    /// **************************
    /// Some processing when exiting current state
    /// **************************
    virtual void Exit(void) = 0;

    /// **************************
    /// Business logic to be processed in current state, including: state transition judgment logic
    /// **************************
    virtual void HandleEvent() = 0;

protected:
    HwcStateManager* p_hwc_state_manager_;
};

// Reduce redundant code writing
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