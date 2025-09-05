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

#include <stdio.h>

#include <fence_queue.h>
#include <barrier_ask_answer_chain.h>
#include <poll/poller_base.h>

#include "log/log.h"

//-------------------------------------------------------------------------
BarrierAskAnswerChain::BarrierAskAnswerChain(PollerBase *o, int max,
					     int maxkeycount,
					     E_BARRIER_UNIT_PLACE place)
	: JobAskInterface<DTCJobOperation>(o), count(0), max_barrier(max),
	  max_key_count_(maxkeycount), main_chain(o)
{
	free_list.InitList();
	for (int i = 0; i < BARRIER_HASH_MAX; i++)
		hash_slot_[i].InitList();
	//stat
	if (IN_FRONT == place) {
		stat_barrier_count = g_stat_mgr.get_stat_int_counter(
			DTC_FRONT_BARRIER_COUNT);
		stat_barrier_max_task = g_stat_mgr.get_stat_int_counter(
			DTC_FRONT_BARRIER_MAX_TASK);
	} else if (IN_BACK == place) {
		stat_barrier_count =
			g_stat_mgr.get_stat_int_counter(DTC_BACK_BARRIER_COUNT);
		stat_barrier_max_task = g_stat_mgr.get_stat_int_counter(
			DTC_BACK_BARRIER_MAX_TASK);
	} else {
		log4cplus_error("bad place value %d", place);
	}
	stat_barrier_count = 0;
	stat_barrier_max_task = 0;
}

BarrierAskAnswerChain::~BarrierAskAnswerChain()
{
	while (!free_list.ListEmpty()) {
		delete static_cast<BarrierQueue *>(free_list.ListNext());
	}
	for (int i = 0; i < BARRIER_HASH_MAX; i++) {
		while (!hash_slot_[i].ListEmpty()) {
			delete static_cast<BarrierQueue *>(
				hash_slot_[i].ListNext());
		}
	}
}

BarrierQueue *BarrierAskAnswerChain::get_barrier(unsigned long key)
{
	ListObject<BarrierQueue> *h = &hash_slot_[key2idx(key)];
	ListObject<BarrierQueue> *p;

	for (p = h->ListNext(); p != h; p = p->ListNext()) {
		if (p->ListOwner()->key() == key)
			return p->ListOwner();
	}

	return NULL;
}

BarrierQueue *BarrierAskAnswerChain::get_barrier_by_idx(unsigned long idx)
{
	if (idx >= BARRIER_HASH_MAX)
		return NULL;

	ListObject<BarrierQueue> *h = &hash_slot_[idx];
	ListObject<BarrierQueue> *p;

	p = h->ListNext();
	return p->ListOwner();
}

void BarrierAskAnswerChain::attach_free_barrier(BarrierQueue *barrier)
{
	barrier->ListMove(free_list);
	count--;
	stat_barrier_count = count;
	//Stat.set_barrier_count (count);
}

void BarrierAskAnswerChain::job_ask_procedure(DTCJobOperation *job_operation)
{
	log4cplus_debug("enter job_ask_procedure");
	if (job_operation->request_code() == DRequest::TYPE_SYSTEM_COMMAND &&
	    job_operation->requestInfo.admin_code() !=
		    DRequest::SystemCommand::Migrate) {
		//Migrate command has already calculated PackedKey and hash during PrepareRequest, needs to queue with normal tasks
		chain_request(job_operation);
		return;
	}
	if (job_operation->is_batch_request()) {
		chain_request(job_operation);
		return;
	}

	unsigned long key = job_operation->barrier_key();
	BarrierQueue *barrier = get_barrier(key);

	if (barrier) {
		if (barrier->Count() < max_key_count_) {
			barrier->Push(job_operation);
			if (barrier->Count() >
			    stat_barrier_max_task) //max key number
				stat_barrier_max_task = barrier->Count();
		} else {
			log4cplus_warning(
				"barrier[%s]: overload max key count %d bars %d",
				owner->Name(), max_key_count_, count);
			job_operation->set_error(
				-EC_SERVER_BUSY, __FUNCTION__,
				"too many request blocked at key");
			job_operation->turn_around_job_answer();
		}
	} else if (count >= max_barrier) {
		log4cplus_warning("too many barriers, count=%d", count);
		job_operation->set_error(-EC_SERVER_BUSY, __FUNCTION__,
					 "too many barriers");
		job_operation->turn_around_job_answer();
	} else {
		if (free_list.ListEmpty()) {
			barrier = new BarrierQueue(&task_queue_allocator);
		} else {
			barrier = free_list.NextOwner();
		}
		barrier->set_key(key);
		barrier->list_move_tail(hash_slot_[key2idx(key)]);
		barrier->Push(job_operation);
		count++;
		stat_barrier_count = count; //barrier number

		chain_request(job_operation);
	}
	log4cplus_debug("leave job_ask_procedure");
}

void BarrierAskAnswerChain::job_answer_procedure(DTCJobOperation *job_operation)
{
	if (job_operation->request_code() == DRequest::TYPE_SYSTEM_COMMAND &&
	    job_operation->requestInfo.admin_code() !=
		    DRequest::SystemCommand::Migrate) {
		job_operation->turn_around_job_answer();
		return;
	}
	if (job_operation->is_batch_request()) {
		job_operation->turn_around_job_answer();
		return;
	}

	unsigned long key = job_operation->barrier_key();
	BarrierQueue *barrier = get_barrier(key);
	if (barrier == NULL) {
		log4cplus_error("return job not in barrier, key=%lu", key);
	} else if (barrier->Front() == job_operation) {
		if (barrier->Count() == stat_barrier_max_task) //max key number
			stat_barrier_max_task--;
		barrier->Pop();
		DTCJobOperation *next = barrier->Front();
		if (next == NULL) {
			attach_free_barrier(barrier);
		} else {
			queue_request(next);
		}
	} else {
		log4cplus_error("return job not barrier header, key=%lu", key);
	}

	job_operation->turn_around_job_answer();
}
