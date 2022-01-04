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
*/
#include "agent/agent_multi_request.h"
#include "task/task_request.h"
#include "agent/agent_client.h"
#include "table/table_def_manager.h"

extern DTCTableDefinition *g_table_def[];

AgentMultiRequest::AgentMultiRequest(DTCJobOperation *o)
	: packetCnt(0), owner(o), taskList(NULL), compleTask(0),
	  owner_client_(NULL)
{
	if (o)
		owner_client_ = o->owner_client();
}

AgentMultiRequest::~AgentMultiRequest()
{
	list_del();

	if (taskList) {
		for (int i = 0; i < packetCnt; i++)
			if (taskList[i].job)
				delete taskList[i].job;

		delete[] taskList;
	}

	if (!!packets)
		free(packets.ptr);
}

void AgentMultiRequest::complete_task(int index)
{
	if (taskList[index].job) {
		delete taskList[index].job;
		taskList[index].job = NULL;
	}

	compleTask++;
	/* delete owner taskrequest along with us if all sub request's result putted into ClientAgent's send queue */

	if (compleTask == packetCnt) {
		delete owner;
	}
}

void AgentMultiRequest::clear_owner_info()
{
	owner_client_ = NULL;

	if (taskList == NULL)
		return;

	for (int i = 0; i < packetCnt; i++) {
		if (taskList[i].job)
			taskList[i].job->clear_owner_client();
	}
}

/*
error case: set this job processed
1. no mem: set job processed
2. decode error: set job processed, reply this job
*/
void AgentMultiRequest::DecodeOneRequest(char *packetstart, int packetlen,
					 int index)
{
	int err = 0;
	DTCJobOperation *job = NULL;
	DecodeResult decoderes;

	job = new DTCJobOperation(
		TableDefinitionManager::instance()->get_cur_table_def());
	if (NULL == job) {
		log4cplus_error(
			"not enough mem for new job creation, client wont recv response");
		compleTask++;
		return;
	}

	job->set_hotbackup_table(
		TableDefinitionManager::instance()->get_hot_backup_table_def());
	decoderes = job->do_decode(packetstart, packetlen, 2);
	switch (decoderes) {
	default:
	case DecodeFatalError:
		if (errno != 0)
			log4cplus_info("decode fatal error, msg = %m");
		break;
	case DecodeDataError:
		job->response_timer_start();
		job->mark_as_hit();
		taskList[index].processed = 1;
		break;
	case DecodeDone:
		if ((err = job->prepare_process()) < 0) {
			log4cplus_error("build packed key error: %d, %s", err,
					job->resultInfo.error_message());
			taskList[index].processed = 1;
		}
		break;
	}

	job->set_owner_info(this, index, NULL);
	job->set_owner_client(this->owner_client_);

	taskList[index].job = job;

	return;
}

int AgentMultiRequest::decode_agent_request()
{
	int cursor = 0;
	log4cplus_debug("AgentMultiRequest decode_agent_request entry.");

	taskList = new DecodedTask[packetCnt];
	if (NULL == taskList) {
		log4cplus_error("no mem new taskList");
		return -1;
	}
	memset((void *)taskList, 0, sizeof(DecodedTask) * packetCnt);

	log4cplus_debug("packet cnt:%d", packetCnt);

	/* whether can work, reply on input buffer's correctness */
	for (int i = 0; i < packetCnt; i++) {
		char *packetstart;
		int packetlen;

		packetstart = packets.ptr + cursor;
		if (packetVersion == 1) {
			packetlen = packet_body_len_v1(
					    *(DTC_HEADER_V1 *)packetstart) +
				    sizeof(DTC_HEADER_V1);
		} else if (packetVersion == 2) {
			packetlen = ((DTC_HEADER_V2 *)packetstart)->packet_len;
		}

		log4cplus_debug("packet len: %d", packetlen);
		DecodeOneRequest(packetstart, packetlen, i);

		cursor += packetlen;
	}

	log4cplus_debug("AgentMultiRequest decode_agent_request leave.");

	return 0;
}

void AgentMultiRequest::copy_reply_for_sub_task()
{
	for (int i = 0; i < packetCnt; i++) {
		if (taskList[i].job)
			taskList[i].job->copy_reply_path(owner);
	}
}
