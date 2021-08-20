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

#ifndef __HELPER_PROCESS_BASE_H__
#define __HELPER_PROCESS_BASE_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include <task/task_base.h>
#include <config/dbconfig.h>
#include <buffer.h>
#include "helper_log_api.h"

class DirectRequestContext;
class DirectResponseContext;

class HelperProcessBase {
    public:
	HelperLogApi logapi;

    public:
	virtual ~HelperProcessBase()
	{
	}

	virtual void use_matched_rows(void) = 0;
	virtual int do_init(int group_id, const DbConfig *Config,
			    DTCTableDefinition *tdef, int slave) = 0;
	virtual void init_ping_timeout(void) = 0;
	virtual int check_table() = 0;

	virtual int process_task(DtcJob *Job) = 0;

	virtual void init_title(int m, int t) = 0;
	virtual void set_title(const char *status) = 0;
	virtual const char *Name(void) = 0;
	virtual void set_proc_timeout(unsigned int seconds) = 0;

	virtual int
	process_direct_query(DirectRequestContext *request_context,
			     DirectResponseContext *response_context)
	{
		return -1;
	}
};

#endif
