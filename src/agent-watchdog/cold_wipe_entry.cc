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
#include "cold_wipe_entry.h"
#include <unistd.h>

const char *cold_wipe_name = "data_lifecycle_manager";

DataLifeCycleEntry::DataLifeCycleEntry(WatchDog *watchdog, int sec)
	: WatchDogDaemon(watchdog, sec)
{
	strncpy(watchdog_object_name_, cold_wipe_name, sizeof(watchdog_object_name_) < strlen(cold_wipe_name)? sizeof(watchdog_object_name_): strlen(cold_wipe_name));
}

DataLifeCycleEntry::~DataLifeCycleEntry(void)
{
}

void DataLifeCycleEntry::exec()
{
	char *argv[3];
	argv[2] = NULL;
	argv[0] = (char *)cold_wipe_name;
	argv[1] = (char*)"-d";
	execv(argv[0], argv);
}
