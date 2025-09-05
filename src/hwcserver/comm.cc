#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <fcntl.h>
#include <getopt.h>
#include <string>
// local 
#include "comm.h"
#include "hwc_global.h"
#include "async_file.h"
// common
#include "log/log.h"
#include "config/dbconfig.h"
#include "table/table_def_manager.h"

DTC::Server CComm::master;
CRegistor CComm::registor;
ConnectorProcess CComm::mysql_process_;

const char *CComm::version = "hwc.0.1";
const char* CComm::dtc_conf = "../conf/dtc.yaml";
int CComm::backend = 0;
int CComm::normal = 1;

void CComm::show_usage(int argc, char **argv)
{
    fprintf(stderr, "Usage: %s [OPTION]...\n"
        "Sync DTC/Bitmap master data to DTC/bitmap slave.\n\n" 
        "\t -n, --normal   normal running mode.\n"
        "\t -b, --backend  runing in background.\n"
        "\t -v, --version  show version.\n"
        "\t -d, --dtc_conf dtc config file path.\n"
        "\t -h, --help     display this help and exit.\n\n", argv[0]);
    return;
}

void CComm::show_version(int argc, char **argv)
{
    fprintf(stderr, "%s(%s)\n", argv[0], version);
    return;
}

void CComm::parse_argv(int argc, char **argv)
{
    if (argc < 2) {
        show_usage(argc, argv);
        exit(-1);
    }

    int option_index = 0, c = 0;
    static struct option long_options[] = {
        {"normal", 0, 0, 'n'},
        {"backend", 0, 0, 'b'},
        {"version", 0, 0, 'v'},
        {"help", 0, 0, 'h'},
        {"dtc_conf", 1, 0, 'd'},
        {0, 0, 0, 0},
    };

    while ((c =
        getopt_long(argc, argv, "nbvt:hd:", long_options,
                &option_index)) != -1) {
        switch (c) {
        case 'n':
            normal = 1;
            break;
        case 'b':
            backend = 1;
            break;;
        case 'v':
            show_version(argc, argv);
            exit(0);
            break;
        case 'h':
            show_usage(argc, argv);
            exit(0);
            break;
        case 'd':
            if (optarg)
                {
                    dtc_conf = optarg;
                }
            break;
        case '?':
        default:
            show_usage(argc, argv);
            exit(-1);
        }
    }

    if (optind < argc) {
        show_usage(argc, argv);
        exit(-1);
    }

    return;
}

int CComm::ReInitDtcAgency(DbConfig* pParser)
{
    if (CComm::connect_ttc_server(1, pParser)) {
        return -1;
    }
    CComm::registor.SetMasterServer(&CComm::master);
    return 0;
}

int CComm::connect_ttc_server(
    int ping_master,
    DbConfig* pParser)
{
    log4cplus_warning("try to ping master server");
    std::string p_bind_addr = pParser->get_bind_addr(pParser->cfgObj->get_config_node());
    if (p_bind_addr.empty()) {
        return -1;
    }
    log4cplus_debug("master:%s.", p_bind_addr.c_str());

    if(ping_master) {
        int ret =  master.SetAddress(p_bind_addr.c_str());
        master.SetTimeout(30);

        DTCTableDefinition* p_dtc_tab_def = TableDefinitionManager::instance()->get_cur_table_def();
        if (DField::Signed == p_dtc_tab_def->key_type() ||  
        DField::Unsigned == p_dtc_tab_def->key_type()) {
            master.IntKey();
            log4cplus_debug("register int key");
        } else if(DField::String == p_dtc_tab_def->key_type() || 
        DField::Binary == p_dtc_tab_def->key_type() ) {
            master.StringKey();
            log4cplus_debug("register string key");
        }
        
        master.SetTableName(pParser->tblFormat);
        master.SetAutoUpdateTab(false);
        if (-DTC::EC_BAD_HOST_STRING == ret
            || (ret = master.Ping()) != 0) {
            log4cplus_error("ping master[%s] failed, err:%d ,errinfo:%s", p_bind_addr.c_str(), ret
                    , master.ErrorMessage());
            return -1;
        }

        log4cplus_warning("ping master[%s] success", p_bind_addr.c_str());
    }

    return 0;
}

int CComm::load_config(const char *p)
{
    const char *f = strdup(p);

    // if (config.ParseConfig(f, "SYSTEM")) {
    //     fprintf(stderr, "parse config %s failed\n", f);
    //     return -1;
    // }

    return 0;
}

int CComm::check_hb_status()
{
    CAsyncFileChecker checker;

    if (checker.Check()) {
        log4cplus_error("check hb status, __NOT__ pass! errmsg: %s, try use --fixed parament to start",
             checker.ErrorMessage());
        return -1;
    }

    log4cplus_warning("check hb status, passed");

    return 0;
}

int CComm::fixed_hb_env()
{
    /* FIXME: Simple deletion, consider how to restore later */
    if (system("cd ../bin/ && ./hb_fixed_env.sh hbp")) {
        log4cplus_error("invoke hb_fixed_env.sh hbp failed, %m");
        return -1;
    }

    log4cplus_warning("fixed hb env, passed");

    return 0;
}

int CComm::fixed_slave_env()
{
    if (system("cd ../bin/ && ./hb_fixed_env.sh slave")) {
        log4cplus_error("invoke hb_fixed_env.sh slave failed, %m");
        return -1;
    }

    log4cplus_warning("fixed slave env, passed");

    return 0;
}

/* Ensure hbp uniqueness, lock hbp's control file directory */
int CComm::uniq_lock(const char *p)
{
    if (access(p, F_OK | X_OK))
        mkdir(p, 0777);

    int fd = open(p, O_RDONLY);
    if (fd < 0)
        return -1;

    fcntl(fd, F_SETFD, FD_CLOEXEC);

    return flock(fd, LOCK_EX | LOCK_NB);
}


