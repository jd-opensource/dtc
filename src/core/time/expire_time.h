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

#ifndef __DTC_EXPIRE_TIME_H
#define __DTC_EXPIRE_TIME_H

#include "namespace.h"
#include "timer/timer_list.h"
#include "log/log.h"
#include "stat_dtc.h"
#include "buffer_pond.h"
#include "data_process.h"
#include "raw_data_process.h"

DTC_BEGIN_NAMESPACE

class TimerObject;
class ExpireTime : private TimerObject {
    public:
	ExpireTime(TimerList *t, BufferPond *c, DataProcess *p,
		   DTCTableDefinition *td, int e);
	virtual ~ExpireTime(void);
	virtual void job_timer_procedure(void);
	void start_key_expired_task(void);
	int try_expire_count();

    private:
	TimerList *timer;
	BufferPond *cache;
	DataProcess *process;
	DTCTableDefinition *table_definition_;

	StatCounter stat_expire_count;
	StatCounter stat_get_request_count;
	StatCounter stat_insert_request_count;
	StatCounter stat_update_request_count;
	StatCounter stat_delete_request_count;
	StatCounter stat_purge_request_count;

	int max_expire_;
};

DTC_END_NAMESPACE

#endif
