#include "hwc_binlog_state.h"
// local
#include "comm.h"
#include "hwc_sync_unit.h"

BinlogState::BinlogState(HwcStateManager* p_hwc_state_manager)
    : HwcStateBase()
    , p_hwc_sync_unit_(new HwcSyncUnit())
{
    p_hwc_state_manager_ = p_hwc_state_manager;
}

BinlogState::~BinlogState()
{
    DELETE(p_hwc_sync_unit_);
}

void BinlogState::Enter()
{
    log4cplus_info(LOG_KEY_WORD "enter into binlog state...");
}

void BinlogState::Exit()
{
    log4cplus_info(LOG_KEY_WORD "exit binlog state");
}

void BinlogState::HandleEvent()
{
        // Get local dtc write requests
        switch (CComm::registor.Regist())
        {
            case -DTC::EC_FULL_SYNC_STAGE:
            case -DTC::EC_INC_SYNC_STAGE:
            {
                if (p_hwc_sync_unit_->Run(&CComm::master)) {
                    p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
                }
            }
            break;
            case -DTC::EC_ERR_SYNC_STAGE:
            default:
            {
                log4cplus_error("hwc status is not correct , try use --fixed parament to start");
                p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
            }
            break;
        }
}