#include "registor.h"
// local
#include "comm.h" 

int CRegistor::Regist() {
    DTC::SvrAdminRequest rq(_master);
    rq.SetAdminCode(DTC::RegisterHB);

    // Send own JournalID
    JournalID self = _controller.JournalId();
    log4cplus_info("registed to master, master[serial=%u, offset=%u]",
                self.serial , self.offset);
    rq.SetHotBackupID((uint64_t) self);

    DTC::Result rs;
    rq.Execute(rs);

    switch (rs.ResultCode()) {
    case -DTC::EC_INC_SYNC_STAGE:
        {
            log4cplus_warning("server report: \"INC-SYNC\"");

            _master_ctime = QueryMemoryCreateTime(_master, 1);

            if (_master_ctime <= 0) 
            {
                log4cplus_debug("master mem time: %lld",(long long int)_master_ctime);
                log4cplus_error("share memory create time changed");
                return -DTC::EC_ERR_SYNC_STAGE;
            }

            return -DTC::EC_INC_SYNC_STAGE;
        }
        break;
    case -DTC::EC_FULL_SYNC_STAGE:
        {
            log4cplus_warning("server report: \"FULL-SYNC\"");
            _master_ctime = QueryMemoryCreateTime(_master, 1);

            if (_master_ctime <= 0) 
            {
                log4cplus_debug("master mem time: %lld",(long long int)_master_ctime);
                log4cplus_error("share memory create time changed");
                return -DTC::EC_ERR_SYNC_STAGE;
            }
            _controller.JournalId() = rs.HotBackupID();

            log4cplus_info
                ("registed to master, master[serial=%u, offset=%u]",
                 _controller.JournalId().serial,
                 _controller.JournalId().offset);
                 
            return -DTC::EC_FULL_SYNC_STAGE;
        }
        break;
    default:
        {
            log4cplus_warning("server report: \"ERR-SYNC\"");
            return -DTC::EC_ERR_SYNC_STAGE;
        }
        break;
    }

    return 0;
};
