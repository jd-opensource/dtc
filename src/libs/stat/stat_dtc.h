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
#ifndef _STAT_DTC_H__
#define _STAT_DTC_H__

#include "stat_thread.h"
#include "stat_dtc_def.h"

#define STATIDX "../stat/dtc.stat.idx"
extern StatThread g_stat_mgr;
extern int init_statistics(void);

#endif
