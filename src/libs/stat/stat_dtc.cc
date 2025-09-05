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
* 
*/
#include <unistd.h>
#include <stdlib.h>
#include "stat_dtc.h"
#include "version.h"
#include "log/log.h"

StatThread g_stat_mgr;
//Initialize memory mapping
int init_statistics(void)
{
	int ret;
	ret = g_stat_mgr.init_stat_info("core", STATIDX);
	// -1, recreate, -2, failed
	if (ret == -1) {
		unlink(STATIDX);
		char buf[64];
		ret = g_stat_mgr.create_stat_index(
			"core", STATIDX, g_stat_definition, buf, sizeof(buf));
		if (ret != 0) {
			log4cplus_error("CreateStatIndex failed: %s",
					g_stat_mgr.get_error_message());
			exit(ret);
		}
		ret = g_stat_mgr.init_stat_info("core", STATIDX);
	}
	if (ret == 0) {
		int v1, v2, v3;
		sscanf(DTC_VERSION, "%d.%d.%d", &v1, &v2, &v3);
		g_stat_mgr.get_stat_iterm(S_VERSION) =
			v1 * 10000 + v2 * 100 + v3;
		g_stat_mgr.get_stat_iterm(C_TIME) = compile_time;
	} else {
		log4cplus_error("init_stat_info failed: %s",
				g_stat_mgr.get_error_message());
		exit(ret);
	}
	return ret;
}
