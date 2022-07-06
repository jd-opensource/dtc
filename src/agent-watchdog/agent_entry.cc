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
#include "agent_entry.h"
#include <unistd.h>

const char *agent_name = "dtcagent";

AgentEntry::AgentEntry(WatchDog *watchdog, int sec)
	: WatchDogDaemon(watchdog, sec)
{
	strncpy(watchdog_object_name_, agent_name, sizeof(watchdog_object_name_) < strlen(agent_name)? sizeof(watchdog_object_name_): strlen(agent_name));
}

AgentEntry::~AgentEntry(void)
{
}

void AgentEntry::exec()
{
	char *argv[6];

	argv[1] = (char *)"-v";
	argv[2] = (char *)"7";
	argv[3] = (char *)"-p";
	argv[4] = (char *)"agent.pid";
	argv[5] = NULL;
	argv[0] = (char *)agent_name;
	execv(argv[0], argv);
}
