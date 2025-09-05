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

#include "stattool.h"

WatchDogStatTool::WatchDogStatTool(WatchDog *watchdog, int sec)
	: WatchDogDaemon(watchdog, sec)
{
	strncpy(watchdog_object_name_, "stattool", sizeof(watchdog_object_name_));
}

WatchDogStatTool::~WatchDogStatTool(void)
{
}

void WatchDogStatTool::exec()
{
	char *argv[3];

	argv[1] = (char *)"reporter";
	argv[2] = NULL;

	argv[0] = (char *)"stattool32";
	execv(argv[0], argv);
	argv[0] = (char *)"../bin/stattool32";
	execv(argv[0], argv);
	argv[0] = (char *)"stattool";
	execv(argv[0], argv);
	argv[0] = (char *)"../bin/stattool";
	execv(argv[0], argv);
}
