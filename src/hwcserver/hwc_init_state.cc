#include "hwc_init_state.h"
// local
#include "daemon.h"
// common
#include "config/dbconfig.h"
#include "table/table_def_manager.h"
// connector
#include "mysql_operation.h"

InitState::InitState(HwcStateManager* p_hwc_state_manager)
    : HwcStateBase()
{ 
    p_hwc_state_manager_ = p_hwc_state_manager;
}

InitState::~InitState()
{ }

void InitState::Enter()
{
    log4cplus_info(LOG_KEY_WORD "enter into init state...");
    DaemonBase::DaemonStart(CComm::backend);

    assert(p_hwc_state_manager_);

    if (CComm::registor.Init()) {
        log4cplus_error("init dump controller file error.");
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }

    // Lock HWC log directory
    if (CComm::uniq_lock()) {
        log4cplus_error("another process already running, exit");
        p_hwc_state_manager_->ChangeState(E_HWC_STATE_FAULT);
        return;
    }
}

void InitState::Exit()
{
    log4cplus_error(LOG_KEY_WORD "exit init state");
}

void InitState::HandleEvent()
{
    // Parse YAML configuration file
    log4cplus_debug("dtc conf file:%s " , CComm::dtc_conf);
	DTCConfig* p_dtc_config = new DTCConfig();
	if (p_dtc_config->load_yaml_file(CComm::dtc_conf,  false) == -1)
		return;

	DbConfig* p_db_Config = DbConfig::Load(p_dtc_config , 1);
	if (p_db_Config == NULL)
		return;
    DTCTableDefinition* p_dtc_tab_def = p_db_Config->build_table_definition();

    TableDefinitionManager::instance()->set_cur_table_def(p_dtc_tab_def , 0);
    // Initialize MySQL process
    // Temporarily not selecting machines by key, cold database configured as single access point
    CComm::mysql_process_.do_init(0 , p_db_Config, p_dtc_tab_def, 0);

    // Bind YAML file parser to StateManager
    p_hwc_state_manager_->BindDBConfigParser(p_db_Config);

    // Jump to next state
    p_hwc_state_manager_->ChangeState(E_HWC_STATE_REGISTER);
}
