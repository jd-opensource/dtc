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

    // 获取本机dtc监听端口，并探活
    DbConfig* pParser = p_hwc_state_manager_->GetDBConfigParser();
    if (CComm::ReInitDtcAgency(pParser)) {
        log4cplus_error("init local dtc error.");
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }
    // 根据dtc配置表结构 创建冷库表
    if (CComm::mysql_process_.create_tab_if_not_exist()) {
        log4cplus_error("create hwc table error.");
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }
    
    // double check 冷库表与dtc配置表结构
    if (CComm::mysql_process_.check_table() != 0) {
        log4cplus_error("mysql field setting is not same as dtc");
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return -1;
    }
}

void RegisterState::Exit()
{
    log4cplus_error(LOG_KEY_WORD "exit register state");
}

void RegisterState::HandleEvent()
{
    // 提前获取本机dtc Binlog的位置
    int i_ret = CComm::registor.Regist();
    if (i_ret != -DTC::EC_INC_SYNC_STAGE
        && i_ret != -DTC::EC_FULL_SYNC_STAGE) {
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }

    // 跳转至下一个状态
    p_hwc_state_manager_->ChangeState(E_HWC_STATE_BINLOG_SYNC);
}