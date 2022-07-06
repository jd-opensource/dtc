#ifndef __GLOBAL_H__
#define __GLOBAL_H__
#include "log/log.h"
#include "daemon.h"

enum { 
    DTC_CODE_SUCCESS = 0,
    DTC_CODE_FAILED = 101, 
    DTC_CODE_INIT_DAEMON_ERR,
    DTC_CODE_LOAD_CONFIG_ERR,
    DTC_CODE_PARSE_CONFIG_ERR,
    DTC_CODE_MYSQL_QRY_ERR,
    DTC_CODE_MYSQL_DEL_ERR,
};

#endif
