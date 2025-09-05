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

#include "task_pendlist.h"
#include "buffer_process_ask_chain.h"
#include "log/log.h"

DTC_USING_NAMESPACE

TaskPendingList::TaskPendingList(JobAskInterface<DTCJobOperation> *o, int to)
	: _timeout(to), _timelist(0), _owner(o), _wakeup(0)
{
	_timelist = _owner->owner->get_timer_list(_timeout);
}

TaskPendingList::~TaskPendingList()
{
	std::list<slot_t>::iterator it;
	for (it = _pendlist.begin(); it != _pendlist.end(); ++it) {
		//Return all requests back to clients
		it->first->set_error(-ETIMEDOUT, __FUNCTION__,
				     "object deconstruct");
		it->first->turn_around_job_answer();
	}
}

void TaskPendingList::add2_list(DTCJobOperation *job)
{
	if (job) {
		if (_pendlist.empty())
			attach_timer(_timelist);

		_pendlist.push_back(std::make_pair(job, time(NULL)));
	}

	return;
}

// Wake up all tasks that are already pending in the queue
void TaskPendingList::Wakeup(void)
{
	log4cplus_debug("TaskPendingList Wakeup");

	//Wake up all tasks
	_wakeup = 1;

	attach_ready_timer(_owner->owner);

	return;
}

void TaskPendingList::job_timer_procedure(void)
{
	log4cplus_debug("enter timer procedure");
	std::list<slot_t> copy;
	copy.swap(_pendlist);
	std::list<slot_t>::iterator it;

	if (_wakeup) {
		for (it = copy.begin(); it != copy.end(); ++it) {
			_owner->job_ask_procedure(it->first);
		}

		_wakeup = 0;
	} else {
		time_t now = time(NULL);

		for (it = copy.begin(); it != copy.end(); ++it) {
			//Timeout handling
			if (it->second + _timeout >= now) {
				_pendlist.push_back(*it);
			} else {
				it->first->set_error(-ETIMEDOUT, __FUNCTION__,
						     "pending job is timedout");
				it->first->turn_around_job_answer();
			}
		}

		if (!_pendlist.empty())
			attach_timer(_timelist);
	}

	log4cplus_debug("leave timer procedure");
	return;
}
