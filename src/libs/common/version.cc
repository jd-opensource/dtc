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
#include <stdlib.h>
#include <time.h>
#include "version.h"

const char compdatestr[] = __DATE__;
const char comptimestr[] = __TIME__;
const char version[] = DTC_VERSION;
const char version_detail[] = DTC_VERSION_DETAIL;
long compile_time;

__attribute__((constructor)) void init_comptime(void)
{
	struct tm tm;
	switch (compdatestr[0]) {
	case 'A':
		tm.tm_mon = compdatestr[1] == 'p' ? 3 /*Apr*/ : 7 /*Aug*/;
		break;
	case 'D':
		tm.tm_mon = 11;
		break; //Dec
	case 'F':
		tm.tm_mon = 1;
		break; //Feb
	case 'J':
		tm.tm_mon =
			compdatestr[1] == 'a' ?
				0 /*Jan*/ :
				compdatestr[2] == 'n' ? 5 /*Jun*/ : 6 /*Jul*/;
		break;
	case 'M':
		tm.tm_mon = compdatestr[2] == 'r' ? 2 /*Mar*/ : 4 /*May*/;
		break;
	case 'N':
		tm.tm_mon = 10; /*Nov*/
		break;
	case 'S':
		tm.tm_mon = 8; /*Sep*/
		break;
	case 'O':
		tm.tm_mon = 9; /*Oct*/
		break;
	default:
		return;
	}

	tm.tm_mday = strtol(compdatestr + 4, 0, 10);
	tm.tm_year = strtol(compdatestr + 7, 0, 10) - 1900;
	tm.tm_hour = strtol(comptimestr + 0, 0, 10);
	tm.tm_min = strtol(comptimestr + 3, 0, 10);
	tm.tm_sec = strtol(comptimestr + 6, 0, 10);
	compile_time = mktime(&tm);
}
