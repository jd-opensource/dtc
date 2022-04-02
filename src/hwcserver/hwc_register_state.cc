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
    // 探活冷数据库集群
    // if (CComm::mysql_process_.try_ping() != 0) {
    //     log4cplus_error("ping cold server error .");
    //     p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
    //     return;
    // }

    #if FOR_DEBUG
    //HwcSync* p_hwc_sync = new HwcSync(&CComm::master);
    DTCTableDefinition* p_dtc_tab_def = SystemState::Instance()->GetDtcTabDef();
    DTCValue astKey[p_dtc_tab_def->key_fields()];// always single key;
    astKey[0].u64 = 110;
    DTCJobOperation* p_job = new DTCJobOperation(p_dtc_tab_def);
    p_job->set_request_key(astKey);
    HwcSync::direct_query_sql_server(p_job);
    p_job->process_internal_result();

    ResultSet* p_cold_res = p_job->result;

    for (int i = 0; i < p_cold_res->total_rows(); i++) {
        RowValue* p_cold_raw = p_cold_res->fetch_row();
        log4cplus_info("sex:%d" , p_cold_raw->field_value(3)->u64);
    }
    #endif
}

void RegisterState::Exit()
{
    log4cplus_error(LOG_KEY_WORD "exit register state");
}

void RegisterState::HandleEvent()
{
    // 提前获取本机dtc Binlog的位置
    log4cplus_info("line:%d",__LINE__);
    int i_ret = CComm::registor.Regist();
    if (i_ret != -DTC::EC_INC_SYNC_STAGE
        && i_ret != -DTC::EC_FULL_SYNC_STAGE) {
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }

    // 跳转至下一个状态
    p_hwc_state_manager_->ChangeState(E_HWC_STATE_BINLOG_SYNC);
}