/*
* Copyright JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#ifndef __MAIN_SUPPLY_H
#define __MAIN_SUPPLY_H

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>

#include <version.h>
#include <table/table_def.h>
#include <config/config.h>
#include <poll/poller_base.h>
#include <listener/listener_pool.h>
#include <barrier_ask_answer_chain.h>
#include <client/client_unit.h>
#include <data_connector_ask_chain.h>
#include <connector/connector_group.h>
#include <buffer_process_ask_chain.h>
#include <buffer_bypass_ask_chain.h>
#include <daemons.h>
#include <config/dbconfig.h>
#include <log/log.h>
#include <daemon/daemon.h>
#include <pipetask.h>
#include <mem_check.h>
#include "socket/unix_socket.h"
#include "stat_dtc.h"
#include "system_command_ask_chain.h"
#include "task/task_multi_unit.h"
#include "black_hole_ask_chain.h"
#include "container.h"
#include "proc_title.h"
#include "plugin/plugin_mgr.h"
#include "dtc_global.h"
#include "remote_dtc_ask_answer_chain.h"
#include "key/key_route_ask_chain.h"
#include "agent/agent_listen_pool.h"
#include "agent/agent_unit.h"
#include "version.h"
#include "dtcutils.h"
#include "algorithm/relative_hour_calculator.h"
#include "buffer_remoteLog.h"
#include "hot_backup_ask_chain.h"
#include "logger.h"
#include "data_process.h"
#include "namespace.h"
#include "global.h"

DTC_BEGIN_NAMESPACE

int plugin_start(void);
int plugin_stop(void);
int stat_open_fd(void);
int init_cache_mode(void);
int init_hotbackup_chain_thread(void);
int init_main_chain_thread(void);
int init_buffer_process_ask_chain_thread(void);
int collect_load_config(DbConfig *dbconfig);
int init_remote_dtc_chain_thread(void);
int init_remote_dtc_chain(PollerBase *thread);
int init_data_connector_chain_thread(void);
int init_buffer_process_ask_chain(PollerBase *thread);
int init_data_connector_ask_chain(PollerBase *thread);

int init_remote_log_config();
int init_config_info();
void free_all_resource();

DTC_END_NAMESPACE

#endif