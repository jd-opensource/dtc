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
#include "async_conn_entry.h"
#include <unistd.h>
#include "log.h"

#define ROOT_PATH "../conf/"
const char *fulldata_name = "async-conn";
extern std::map<std::string, std::string> map_dtc_conf; //key:value --> dtc addr:conf file name

AsyncConnEntry::AsyncConnEntry(WatchDog *watchdog, int sec)
	: WatchDogDaemon(watchdog, sec)
{
	strncpy(watchdog_object_name_, fulldata_name, sizeof(watchdog_object_name_) < strlen(fulldata_name) ? sizeof(watchdog_object_name_) : strlen(fulldata_name));
}

AsyncConnEntry::~AsyncConnEntry(void)
{
}

void AsyncConnEntry::exec()
{
	std::map<std::string, std::string>::iterator it;
	for(it = map_dtc_conf.begin(); it != map_dtc_conf.end(); it++)
	{
		std::string addr = (*it).first;
		std::string filename = (*it).second;

		std::string filepath = string(ROOT_PATH) + filename;
		log4cplus_debug("filepath:%s", filepath.c_str());

		char *argv[3];

		argv[2] = NULL;
		argv[0] = (char *)fulldata_name;
		argv[1] = (char *)filepath.c_str();
		execv(argv[0], argv);
	}
}
