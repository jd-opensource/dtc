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
#include "core_entry.h"
#include <unistd.h>

const char *core_name = "dtcd";

CoreEntry::CoreEntry(WatchDog *watchdog, int sec)
	: WatchDogDaemon(watchdog, sec)
{
	strncpy(watchdog_object_name_, core_name, sizeof(watchdog_object_name_) < strlen(core_name)? sizeof(watchdog_object_name_): strlen(core_name));
}

CoreEntry::~CoreEntry(void)
{
}

void CoreEntry::exec()
{
	char *argv[2];

	argv[0] = (char *)core_name;
	argv[1] = NULL;
	execv(argv[0], argv);
}
