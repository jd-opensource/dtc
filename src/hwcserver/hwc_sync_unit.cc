#include "hwc_sync_unit.h"
#include <string>
#include <sys/time.h>
// local
#include "comm.h"
// connector
#include "mysql_operation.h"
// libs/api/cc_api/include/
#include "dtcapi.h"
#include "mysqld_error.h"

HwcSync::HwcSync(DTC::Server* p_server)
    : i_limit_(1)
    , p_master_(p_server)
    , o_journal_id_(CComm::registor.JournalId())
{ } 

HwcSync::~HwcSync()
{ }

int HwcSync::query_cold_server(
    DTCJobOperation* p_job,
    const DTCValue* key)
{
    p_job->set_request_key(key);
    p_job->set_request_code(DRequest::Get);
    DTCFieldSet* p_dtc_field_set = p_job->request_fields();
    DELETE(p_dtc_field_set);

    DTCTableDefinition* p_dtc_tab_def = TableDefinitionManager::instance()->get_cur_table_def();
    
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

    if (CComm::mysql_process_.do_process(p_job)) {
        return -1;
    }

    return p_job->process_internal_result();

}

void HwcSync::sql_statement_query(
    const DTCValue* p_key,
    std::string& s_sql)
{
    uint32_t ui_count = 0;

    do {
        int i_ret = CComm::mysql_process_.process_statement_query(p_key , s_sql);
        if (-ER_DUP_ENTRY == i_ret || 0 == i_ret) {
            break;
        }
        
        uint64_t ui_interval = pow(2, ++ui_count);
        sleep(ui_interval);
        log4cplus_error("sql statement query fail sequence:%d" , ui_count);
    } while (true);
}

void HwcSync::decode_hotbin_result(
    ResultSet* o_hot_res,
    const HwcBinlogCont& o_hwc_bin)
{
    DTCBinary o_raw_bin;
    o_raw_bin.len = o_hwc_bin.i_raw_len;
    o_raw_bin.ptr = o_hwc_bin.p_raw_val;

    o_hot_res->set_value_data(o_hwc_bin.i_raw_nums , o_raw_bin);
}

int HwcSync::get_current_time()
{
    timeval now;
    gettimeofday(&now, NULL);
    return now.tv_sec;
}

int HwcSync::Run()
{
    /* 先关闭连接，防止fd重路 */
    p_master_->Close();
    int i_sec = get_current_time() + 1;
    while (true) {
        usleep(1000000); // 1s
        if (get_current_time() >= i_sec) {
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
        request_m.Limit(0, i_limit_);
        log4cplus_info("begin serial:%d , offset:%d" , o_journal_id_.serial , o_journal_id_.offset);

        DTC::Result result_m;
        int ret = request_m.Execute(result_m);
        log4cplus_warning("hwc server is aliving....., return:%d", ret);

        if (-DTC::EC_BAD_HOTBACKUP_JID == ret) {
            log4cplus_error("master report journalID is not match");
        }

        // 重试
        if (0 != ret) {
            log4cplus_warning("fetch key-list from master, limit[%d], ret=%d, err=%s",
            i_limit_, ret, result_m.ErrorMessage());
            usleep(100);
            continue;
        }
        // 写请求 插入 冷数据库
        for (int i = 0; i < result_m.NumRows(); ++i) {
            ret = result_m.FetchRow();
            if (ret < 0) {
                log4cplus_error("fetch key-list from master failed, limit[%d], ret=%d, err=%s",
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

            DTCTableDefinition* p_dtc_tab_def = TableDefinitionManager::instance()->get_cur_table_def();

            DTCValue astKey[p_dtc_tab_def->key_fields()];// always single key
            TaskPackedKey::unpack_key(p_dtc_tab_def, p_key, astKey);

            int i_value_size = 0;
            char* p_value = (char *)result_m.BinaryValue("value", i_value_size);

            HwcBinlogCont o_hot_bin;
            bool b_ret = o_hot_bin.ParseFromString(p_value , i_value_size);
            if (!b_ret) {
                log4cplus_error("report alarm to manager");
                break;
            }

            std::string s_sql(o_hot_bin.p_sql , o_hot_bin.i_sql_len);
            log4cplus_info(" mysql cmd:%s , check flag:%d , row len:%d" ,
                 s_sql.c_str(), o_hot_bin.i_check_flag
                 , o_hot_bin.i_raw_len);

            if (0 == o_hot_bin.i_check_flag) {
                sql_statement_query(astKey, s_sql);
                break;
            } else if (1 == o_hot_bin.i_check_flag) {
                log4cplus_info("check: starting...");

                DTCJobOperation o_cold_job(p_dtc_tab_def);
                query_cold_server(&o_cold_job , astKey);

                ResultSet* p_cold_res = o_cold_job.result;
                if (!p_cold_res) {
                    log4cplus_info("cold res is null");
                    return E_HWC_SYNC_NORMAL_EXIT;
                }

                log4cplus_info("hot row num:%d ,cold row num:%d" , 
                        o_hot_bin.i_raw_nums , p_cold_res->total_rows());

                if (o_hot_bin.i_raw_nums > p_cold_res->total_rows() ||
                    o_hot_bin.i_raw_nums < p_cold_res->total_rows()) {
                    sql_statement_query(astKey, s_sql);
                    break;
                }

                uint8_t* p_fiedld_list = p_dtc_tab_def->raw_fields_list();
                DTCFieldSet o_dtc_field_set(p_fiedld_list , p_dtc_tab_def->num_fields() + 1);

                ResultSet p_hot_result(o_dtc_field_set , p_dtc_tab_def);
                decode_hotbin_result(&p_hot_result , o_hot_bin);

                for (int i = 0; i < p_hot_result.total_rows(); i++) {
                    RowValue* p_hot_raw = p_hot_result.fetch_row();
                    
                    bool b_check = false;
                    // 冷数据库为base,只要冷数据库中没有热的，就插入
                    for (int j = 0; j < p_cold_res->total_rows(); j++) {
                        RowValue* p_cold_raw = p_cold_res->fetch_row();
                        if(p_hot_raw->Compare(*p_cold_raw ,
                                p_fiedld_list , 
                                p_dtc_tab_def->num_fields() + 1) == 0) {
                            log4cplus_info("check: row data has been in cold table");
                            b_check = true;
                            break;
                        }
                    }

                    if (!b_check) {
                        // 对账失败，执行sql语句 ，容错逻辑
                        log4cplus_info("check: need insert in cold table");
                        sql_statement_query(astKey, s_sql);
                        break;
                    }
                    p_cold_res->rewind();
                }
                log4cplus_info("check: finish");
            } else {
                log4cplus_error("illegal check flag");
                continue;
            }
        }
        // 成功，则更新控制文件中的journalID
        o_journal_id_ = (uint64_t)result_m.HotBackupID();
        log4cplus_info("end serial:%d , offset:%d" , o_journal_id_.serial , o_journal_id_.offset);
        CComm::registor.JournalId() = o_journal_id_;
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
