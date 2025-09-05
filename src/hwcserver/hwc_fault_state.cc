#include "hwc_fault_state.h"

FaultState::FaultState(HwcStateManager* p_hwc_state_manager)
    : HwcStateBase()
{
    p_hwc_state_manager_ = p_hwc_state_manager;
}

FaultState::~FaultState()
{

}

void FaultState::Enter()
{
    log4cplus_error("Here is FaultState");
}

void FaultState::Exit()
{
    // do nothing
}

void FaultState::HandleEvent()
{
    // Simple handling, program exit
    log4cplus_info("GoodBye hwc_service.");
    exit(0);
}