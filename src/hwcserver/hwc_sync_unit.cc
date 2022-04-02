#include "hwc_sync_unit.h"
#include <string>
// local
#include "comm.h"
#include "system_state.h"
// common
#include "log/log.h"
#include "task/task_request.h"
#include "task/task_pkey.h"
#include "mem_check.h"
#include "table/hotbackup_table_def.h"
// connector
#include "mysql_operation.h"
// libs/api/cc_api/include/
#include "dtcapi.h"

HwcSync::HwcSync(DTC::Server* p_server)
    : i_limit_(1)
    , p_master_(p_server)
    , o_journal_id_(SystemState::Instance()->GetJournalID()) // CComm::registor.JournalId())
{
    bzero(s_err_msg_, sizeof(s_err_msg_));
} 

HwcSync::~HwcSync()
{ }

int HwcSync::direct_query_sql_server(
    DTCJobOperation* p_job)
{
    p_job->set_request_code(DRequest::Get);
    DTCFieldSet* p_dtc_field_set = p_job->request_fields();
    DELETE(p_dtc_field_set);

    DTCTableDefinition* p_dtc_tab_def = SystemState::Instance()->GetDtcTabDef();
    
    #if FOR_DEBUG
    for (int i = 1; i < p_job->num_fields() + 1; i++) {
        log4cplus_info("field id:%d" , p_dtc_tab_def->raw_fields_list()[i]);
    }
    #endif

    // begin at 1 ,exclude key field id
    p_dtc_field_set = new DTCFieldSet(
        p_dtc_tab_def->raw_fields_list(),
        p_job->num_fields() + 1);
    p_job->set_request_fields(p_dtc_field_set);

    return CComm::mysql_process_.do_process(p_job);
}

int HwcSync::Run()
{
    /* 先关闭连接，防止fd重路 */
    p_master_->Close();

    timeval now;
    gettimeofday(&now, NULL);
    int i_sec = now.tv_sec + 1;

    while (true) {
        usleep(1000000); // 1s
        gettimeofday(&now, NULL);
        if (now.tv_sec >= i_sec) {
            if (CComm::registor.CheckMemoryCreateTime()) {
                log4cplus_error("detect share memory changed");
            }
            i_sec += 1;
        }

        DTC::SvrAdminRequest request_m(p_master_);
        request_m.SetAdminCode(DTC::GetUpdateKey);

        request_m.Need("type");
        request_m.Need("flag");
        request_m.Need("key");
        request_m.Need("value");
        request_m.SetHotBackupID((uint64_t)o_journal_id_);
        log4cplus_info("begin serial:%d , offset:%d" , o_journal_id_.serial , o_journal_id_.offset);

        request_m.Limit(0, i_limit_);

        DTC::Result result_m;
        int ret = request_m.Execute(result_m);
        log4cplus_error("aliving....., return:%d", ret);

        if (-DTC::EC_BAD_HOTBACKUP_JID == ret) {
            log4cplus_error("master report journalID is not match");
        }

        // 重试
        if (0 != ret) {
            log4cplus_error("fetch key-list from master failed, limit[%d], ret=%d, err=%s",
            i_limit_, ret, result_m.ErrorMessage());
            
            snprintf(s_err_msg_, sizeof(s_err_msg_),
                 "fetch key-list from master failed, limit[%d], ret=%d, err=%s",
                  i_limit_, ret, result_m.ErrorMessage());
            usleep(100);
            continue;
        }
        // 写请求 插入 冷数据库
        for (int i = 0; i < result_m.NumRows(); ++i) {
            ret = result_m.FetchRow();
            if (ret < 0) {
                snprintf(s_err_msg_, sizeof(s_err_msg_),
                     "fetch key-list from master failed, limit[%d], ret=%d, err=%s",
                      i_limit_, ret, result_m.ErrorMessage());

                // dtc可以运行失败
                return E_HWC_SYNC_DTC_ERROR;
            }

            int i_type = result_m.IntValue("type");
            if (i_type != DTCHotBackup::SYNC_NONE) {
                log4cplus_info("no sync none type , continue");
                break;
            }
            
            // key parse
            int i_key_size = 0;
            char* p_key = result_m.BinaryValue("key", i_key_size);

            DTCTableDefinition* p_dtc_tab_def = SystemState::Instance()->GetDtcTabDef();

            DTCValue astKey[p_dtc_tab_def->key_fields()];// always single key
            TaskPackedKey::unpack_key(p_dtc_tab_def, p_key, astKey);

            // value parse
            // |(int)len|mysql cmd content|(int)check flag|(unsigned int)numRows|(int)len|raw value格式
            int i_value_size = 0;
            char* p_value = (char *)result_m.BinaryValue("value", i_value_size);

            int i_mysql_len = *(int*)p_value;
            p_value += sizeof(int);

            std::string s_mysql_cmd(p_value , i_mysql_len);
            p_value += i_mysql_len;

            int i_check = *(int*)p_value;
            p_value += sizeof(int);

            log4cplus_info(" mysql cmd:%s , check flag:%d" , s_mysql_cmd.c_str(), i_check);

            if (i_check) { // 需进行行数 check ，对账
                log4cplus_info("check: starting...");
                unsigned int ui_num_rows = *(unsigned int*)p_value;
                p_value += sizeof(unsigned int);

                DTCBinary o_value;
                o_value.len = *(int*)p_value;

                p_value += sizeof(int);
                o_value.ptr = p_value;

                
                uint8_t* p_fiedld_list = p_dtc_tab_def->raw_fields_list();

                ResultSet* p_result_set = new ResultSet(*p_fiedld_list , p_dtc_tab_def);
                p_result_set->set_value_data(ui_num_rows , o_value);

                log4cplus_info(" row num:%d" , ui_num_rows);

                // 冷数据库key查询
                DTCJobOperation* p_job = new DTCJobOperation(p_dtc_tab_def);
                p_job->set_request_key(astKey);
                direct_query_sql_server(p_job);
                p_job->process_internal_result();

                ResultSet* p_cold_res = p_job->result;
                if (NULL == p_cold_res) {
                    log4cplus_info("cold res is null");
                    return E_HWC_SYNC_NORMAL_EXIT;
                }

                for (int i = 0; i < ui_num_rows; i++) {
                    RowValue* p_hot_raw = p_result_set->fetch_row();

                    bool b_check = false;
                    // 冷数据库为base,只要冷数据库中没有热的，就插入
                    for (int j = 0; j < p_cold_res->total_rows(); j++) {
                        RowValue* p_cold_raw = p_cold_res->fetch_row();
                        if(p_hot_raw->Compare(*p_cold_raw ,
                                p_fiedld_list , 
                                p_dtc_tab_def->num_fields()) == 0) {
                            log4cplus_info("check: row data has been in cold table");
                            b_check = true;
                            break;
                        }
                    }

                    if (!b_check) {
                        // 对账失败，执行sql语句 ，容错逻辑
                        log4cplus_info("check: need insert in cold table");
                        if (CComm::mysql_process_.process_statement_query(astKey,
                         s_mysql_cmd) != 0) {
                            /* code */
                        }
                        break;
                    }
                    p_cold_res->rewind();
                }
                log4cplus_info("check: finish");
            } else { // 插入冷数据库
                if (CComm::mysql_process_.process_statement_query(astKey,
                         s_mysql_cmd) != 0) {
                    /* code */
                }
            }
        }
        // 成功，则更新控制文件中的journalID
        o_journal_id_ = (uint64_t)result_m.HotBackupID();
        log4cplus_info("end serial:%d , offset:%d" , o_journal_id_.serial , o_journal_id_.offset);
        // CComm::registor.JournalId() = o_journal_id_;
        SystemState::Instance()->SetJournalID(o_journal_id_);
    }

    return E_HWC_SYNC_NORMAL_EXIT;
}

//***************************分割线***************************
HwcSyncUnit::HwcSyncUnit()
    : p_hwc_sync_(NULL)
{ } 

HwcSyncUnit::~HwcSyncUnit()
{
    DELETE(p_hwc_sync_);
}

bool HwcSyncUnit::Run(DTC::Server* m , int limit)
{
        log4cplus_warning("hwc sync unit is start");
        
        if (NULL == p_hwc_sync_) {
            p_hwc_sync_ = new HwcSync(m);
            if (!p_hwc_sync_) {
                log4cplus_error("hwcsync is not complete, err: create HwcSync obj failed");
                return false;
            }
        }

        p_hwc_sync_->SetLimit(limit);

        int i_ret = p_hwc_sync_->Run();

        log4cplus_warning("hwcsync is stop , errorid:%d" , i_ret);
        return (i_ret != E_HWC_SYNC_NORMAL_EXIT);

}
