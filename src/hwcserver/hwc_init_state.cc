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

    // 锁住hwc的日志目录
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
    // 解析yaml配置文件
    log4cplus_debug("dtc conf file:%s , table conf file:%s" , CComm::dtc_conf ,CComm::table_conf);
	DTCConfig* p_dtc_config = new DTCConfig();
	if (p_dtc_config->parse_config(CComm::table_conf, "DATABASE_CONF", false) == -1)
		return -1;

	if (p_dtc_config->parse_config(CComm::dtc_conf, "cache", false))
		return -1;

	DbConfig* p_db_Config = DbConfig::Load(p_dtc_config , 1);
	if (p_db_Config == NULL)
		return -1;

    DTCTableDefinition* p_dtc_tab_def = p_db_Config->build_table_definition();

    TableDefinitionManager::instance()->set_cur_table_def(p_dtc_tab_def , 0);
    // 初始化mysql process
    // 暂时不按key选择机器，冷数据库对外为一台访问配置
    CComm::mysql_process_.do_init(0 , p_db_Config, p_dtc_tab_def, 0);

    // 绑定yaml文件解析器至StateManager
    p_hwc_state_manager_->BindDBConfigParser(p_db_Config);

    // 跳转至下一个状态
    p_hwc_state_manager_->ChangeState(E_HWC_STATE_REGISTER);
}
