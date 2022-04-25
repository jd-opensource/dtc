/*
 * =====================================================================================
 *
 *       Filename:  comm.h
 *
 *    Description:  comm class definition.
 *
 *        Version:  1.0
 *        Created:  04/01/2021
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  chenyujie, chenyujie28@jd.com@jd.com
 *        Company:  JD.com, Inc.
 *
 * =====================================================================================
 */

#ifndef __HB_COMM_H
#define __HB_COMM_H
// local
#include "registor.h"
// libs/api/cc_api/include
#include "dtcapi.h"
// connecter
#include "mysql_operation.h"

class DbConfig;

class CComm {
public:
	static void parse_argv(int argc, char **argv);
	static void show_usage(int argc, char **argv);
	static void show_version(int argc, char **argv);
	static int load_config(const char *p = SYS_CONFIG_FILE);
	static int check_hb_status();
	static int fixed_hb_env();
	static int fixed_slave_env();
	static int connect_ttc_server(int ping_master, DbConfig* pParser);
	static int uniq_lock(const char *p = ASYNC_FILE_PATH);
	static int ReInitDtcAgency(DbConfig* pParser);

public:
	static CRegistor registor;
	static DTC::Server master;
	static ConnectorProcess mysql_process_;

	static const char* version;
	static char* dtc_conf;
	static char* table_conf;
	static int backend;
	static int normal;
};

#endif
