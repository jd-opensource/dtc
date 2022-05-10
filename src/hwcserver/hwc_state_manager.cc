#include "hwc_state_manager.h"
// local 
#include "hwc_init_state.h"
#include "hwc_register_state.h"
#include "hwc_binlog_state.h"
#include "hwc_fault_state.h"

HwcStateManager::HwcStateManager()
    : i_pre_state_(E_HWC_STATE_INIT)
    , i_current_state_(E_HWC_STATE_INIT)
    , i_next_state_(E_HWC_STATE_INIT)
    , p_current_state_(NULL)
    , p_db_config_(NULL)
    , o_hwc_state_map_()
{
    o_hwc_state_map_.insert(std::make_pair(E_HWC_STATE_INIT , new InitState(this)));
    o_hwc_state_map_.insert(std::make_pair(E_HWC_STATE_REGISTER , new RegisterState(this)));
    o_hwc_state_map_.insert(std::make_pair(E_HWC_STATE_BINLOG_SYNC , new BinlogState(this)));
    o_hwc_state_map_.insert(std::make_pair(E_HWC_STATE_FAULT , new FaultState(this)));

    p_current_state_ = o_hwc_state_map_[E_HWC_STATE_INIT];
}

HwcStateManager::~HwcStateManager()
{
    HwcStateMapIter iter = o_hwc_state_map_.begin();
    for (; iter != o_hwc_state_map_.end(); ++iter)
    {
        DELETE(iter->second);
    }
    
    o_hwc_state_map_.clear();

    DELETE(p_db_config_);
}

void HwcStateManager::Start()
{
    assert(p_current_state_ != NULL);
    p_current_state_->Enter();
    p_current_state_->HandleEvent();
}

void HwcStateManager::ChangeState(int iNewState)
{
    assert(p_current_state_ != NULL);
    i_next_state_ = iNewState;
    p_current_state_->Exit();
    log4cplus_info(LOG_KEY_WORD "changeState from %d to %d" , i_current_state_ , iNewState);

    assert(o_hwc_state_map_.find(iNewState) != o_hwc_state_map_.end());
    p_current_state_ = o_hwc_state_map_[iNewState];

    i_pre_state_ = i_current_state_;
    i_current_state_ = iNewState;
    p_current_state_->Enter();
    p_current_state_->HandleEvent();
}