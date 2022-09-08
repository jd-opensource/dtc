/*
* Copyright [2021] JD.com, Inc.
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
* 
*/
#include <signal.h>

#include "daemons.h"
#include "main_entry.h"
#include "stattool.h"
#include "hwc.h"
#include "listener/listener.h"
#include "helper.h"
#include "logger.h"
#include "config/dbconfig.h"
#include "log/log.h"
#include "daemon/daemon.h"
#include "../core/global.h"
/* 打开看门狗 */
int start_dtc(int (*entry)(void *), void *args)
{
    int delay = 5;
    dbConfig->set_helper_path(getpid());
    WatchDog *wdog = NULL;
    WatchDogListener *srv = NULL;

	  if (g_dtc_config->get_int_val("cache", "DisableWatchDog", 0) == 0) {
		signal(SIGCHLD, SIG_DFL);
		delay = g_dtc_config->get_int_val("cache", "WatchDogTime", 30);
		if (delay < 5)
			delay = 5;
		wdog = new WatchDog;
		srv = new WatchDogListener(wdog, delay);
		srv->attach_watch_dog();
		start_fault_logger(wdog);
		if (g_dtc_config->get_int_val("cache", "StartStatReporter", 0) >
		    0) {
			WatchDogStatTool *stat_tool =
				new WatchDogStatTool(wdog, delay);
			if (stat_tool->new_proc_fork() < 0)
				/* cann't fork reporter */
				return -1;
			log4cplus_info("fork stat reporter");
		}
	}
	
	if (DbConfig::get_dtc_mode(g_dtc_config->get_config_node()) == DTC_MODE_DATABASE_ADDITION) {
		int nh = 0;
		/* starting master helper */
		for (int g = 0; g < dbConfig->machineCnt; ++g) {
			for (int r = 0; r < ROLES_PER_MACHINE; ++r) {
				HELPERTYPE t = dbConfig->mach[g].helperType;
				log4cplus_debug("helper type = %d", t);
				/* check helper type is dtc */
				if (DTC_HELPER >= t)
					break;
				int i, n = 0;
				for (i = 0; i < GROUPS_PER_ROLE &&
					    (r * GROUPS_PER_ROLE + i) <
						    GROUPS_PER_MACHINE;
				     i++) {
					n += dbConfig->mach[g].gprocs
						     [r * GROUPS_PER_ROLE + i];
				}
				if (n <= 0)
					continue;
				WatchDogHelper *h = NULL;
				NEW(WatchDogHelper(
					    wdog, delay,
					    dbConfig->mach[g].role[r].path, g,
					    r, n + 1, t),
				    h);
				if (NULL == h) {
					log4cplus_error(
						"create WatchDogHelper object failed, msg:%m");
					return -1;
				}
				if (h->new_proc_fork() < 0 || h->verify() < 0)
				{
					log4cplus_error("fork failed and exit.");
					return -1;
				}
				nh++;
			}
		}
		log4cplus_info("fork %d helper groups", nh);
	}
	if (wdog) {
		const int recovery = g_dtc_config->get_idx_val(
			"cache", "ServerRecovery",
			((const char *const[]){ "none", "crash", "crashdebug",
						"killed", "error", "always",
						NULL }),
			2);
		MainEntry *dtc =
			new MainEntry(wdog, entry, args, recovery);
		if (dtc->fork_main() < 0)
			return -1;
    // wait for dtc entry step to agentlisten
    usleep(100 * 1000);
	
    if (g_dtc_config->get_config_node()["primary"]["hot"]) {	// open hwcserver at DATABASE_IN_ADDITION mode.
            WatchDogHWC* p_hwc_wd = new WatchDogHWC(wdog, delay);
            if (p_hwc_wd->new_proc_fork() < 0) {
                log4cplus_error("fork hwc server fail");
                return -1;
            }
            log4cplus_info ("fork hwc server");
    } else {
		log4cplus_info("not hot");
	}
		wdog->run_loop();
		exit(0);
	}
	return 0;
}
