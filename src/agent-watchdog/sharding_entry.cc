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
#include "sharding_entry.h"
#include <unistd.h>
#include "proc_title.h"

const char *sharding_name = "sharding";

ShardingEntry::ShardingEntry(WatchDog *watchdog, int sec)
	: WatchDogDaemon(watchdog, sec)
{
	strncpy(watchdog_object_name_, "sharding", sizeof(watchdog_object_name_));
}

ShardingEntry::~ShardingEntry(void)
{
}

void ShardingEntry::exec()
{
	char *argv[4];

	int ret = system("./conf-gen-utils");
	if(ret == 0)
	{
		set_proc_title("agent_sharding");
		argv[0] = (char *)"../sharding/bin/start.sh";
		argv[1] = const_cast<char*>("3307");
		argv[2] = const_cast<char*>("../conf");
		argv[3] = NULL;
		execv(argv[0], argv);
	}
}
