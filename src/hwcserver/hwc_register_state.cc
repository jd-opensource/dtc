#include "hwc_register_state.h"
#include "hwc_sync_unit.h"

#include "task/task_request.h"
#include "task/task_pkey.h"

RegisterState::RegisterState(HwcStateManager* p_hwc_state_manager)
    : HwcStateBase()
{
    p_hwc_state_manager_ = p_hwc_state_manager;
}

RegisterState::~RegisterState()
{ }

void RegisterState::Enter()
{
    log4cplus_info(LOG_KEY_WORD "enter into register state...");

    // Get local DTC listening port and probe connectivity
    DbConfig* pParser = p_hwc_state_manager_->GetDBConfigParser();
    if (CComm::ReInitDtcAgency(pParser)) {
        log4cplus_error("init local dtc error.");
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }
    // Create cold storage table based on DTC configuration table structure
    if (CComm::mysql_process_.create_tab_if_not_exist()) {
        log4cplus_error("create hwc table error.");
    }
    
    // Double check cold storage table structure against DTC configuration table structure
    if (CComm::mysql_process_.check_table() != 0) {
        log4cplus_error("mysql field setting is not same as dtc");
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }
}

void RegisterState::Exit()
{
    log4cplus_error(LOG_KEY_WORD "exit register state");
}

void RegisterState::HandleEvent()
{
    // Get local DTC Binlog position in advance
    int i_ret = CComm::registor.Regist();
    if (i_ret != -DTC::EC_INC_SYNC_STAGE
        && i_ret != -DTC::EC_FULL_SYNC_STAGE) {
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }

    // Jump to next state
    p_hwc_state_manager_->ChangeState(E_HWC_STATE_BINLOG_SYNC);
}