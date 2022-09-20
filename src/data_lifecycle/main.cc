#include "global.h"
#include "data_manager.h"
#include "data_conf.h"
#include "dtc_global.h"
#include "config.h"
#include "proc_title.h"
#include "dbconfig.h"
#include "log.h"

#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <iostream>
#include <thread>

const char data_project_name[] = "data_lifecycle_manager";

int scan_file(const char* path, std::vector<std::string>& config_vec){
    DIR *dir = opendir(path);
    if(dir == NULL){
        log4cplus_error("opendir error.");
        return DTC_CODE_LOAD_CONFIG_ERR;
    }
    struct dirent *dirent;
    while(dirent = readdir(dir)){
        string dir_name = dirent->d_name;
        string match_str="dtc-conf";
        if(dir_name == "." || dir_name == ".." || dir_name == "dtc-conf-0.yaml"){
            continue;
        } else if(dir_name.size() > match_str.size() && dir_name.substr(0, match_str.size()) == match_str){
            config_vec.push_back("../conf/" + dir_name);
        }
    }
    return 0;
}

void thread_func(const std::string& config_path){
    DataConf* p_data_conf = new DataConf();
    if(p_data_conf->LoadConfig(config_path) != 0){
        log4cplus_error("load_config error.");
        return;
    }
    ConfigParam config_param;
    if(p_data_conf->ParseConfig(config_path, config_param) != 0){
        log4cplus_error("parse_config error.");
        return;
    }
    DataManager* p_data_manager = new DataManager(config_param);
    if(0 != p_data_manager->ConnectAgent()){
        log4cplus_error("ConnectAgent error.");
        return;
    }
    if(0 != p_data_manager->ConnectFullDB()){
        log4cplus_error("ConnectFullDB error.");
        return;
    }
    if(0 != p_data_manager->CreateTable()){
        log4cplus_error("CreateTable error.");
        return;
    }
    p_data_manager->DoProcess();
    if(NULL != p_data_manager){
        delete p_data_manager;
    }
    if(NULL != p_data_conf){
        delete p_data_conf;
    }
}

int main(int argc, char *argv[]){
    init_proc_title(argc, argv);
    set_proc_title("agent-data-lifecycle");
    init_log4cplus();
    log4cplus_info("%s v%s: starting....", data_project_name, version);
    if(init_daemon() < 0){
        log4cplus_error("init_daemon error.");
        return DTC_CODE_INIT_DAEMON_ERR;
    }

    std::vector<std::string> config_vec;
    int ret = scan_file("../conf", config_vec);
    if(0 != ret){
        log4cplus_error("scan_file error.");
        return DTC_CODE_LOAD_CONFIG_ERR;
    }
    std::vector<std::thread> thread_vec;
    for(auto config_path : config_vec){
        thread_vec.push_back(std::thread(thread_func, config_path));
    }

    for (auto vit = thread_vec.begin(); vit != thread_vec.end(); vit++) {
        if (vit->joinable()) {
            vit->join();
        }
    }

    /*if(NULL != p_data_manager){
        delete p_data_manager;
    }
    */
    log4cplus_info("%s v%s: stopped", data_project_name, version);
    //Logger::shutdown();
    //daemon_cleanup();
    //DaemonCrashed(1);
    // log4cplus::deinitialize();
    return 0;
}
