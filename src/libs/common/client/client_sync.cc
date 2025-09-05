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
#include <sys/un.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

#include "client_sync.h"
#include "task/task_request.h"
#include "packet/packet.h"
#include "client/client_unit.h"
#include "log/log.h"

#include <stat_dtc.h>

class PollerBase;
static int statEnable = 0;
static StatCounter statAcceptCount;
static StatCounter statCurConnCount;

class ReplySync : public JobAnswerInterface<DTCJobOperation> {
    public:
	ReplySync(void)
	{
	}
	virtual ~ReplySync(void)
	{
	}
	virtual void job_answer_procedure(DTCJobOperation *job);
};

void ReplySync::job_answer_procedure(DTCJobOperation *job)
{
	ClientSync *cli = job->OwnerInfo<ClientSync>();
	DTCDecoderUnit *resource_owner = (DTCDecoderUnit *)job->resource_owner;

	/* cli not exist, clean&free resource */
	if (cli == NULL) {
		if (job->resource_id != 0) {
			resource_owner->clean_resource(job->resource_id);
			resource_owner->unregist_resource(job->resource_id,
							  job->resource_seq);
		}
	}
	/* normal case */
	else if (cli->send_result() == 0) {
		cli->delay_apply_events();
	} else /* send error, delete client, resource will be clean&free there */
	{
		// delete client apply delete job
		delete cli;
	}
}

static ReplySync syncReplyObject;

ClientSync::ClientSync(DTCDecoderUnit *o, int fd, void *peer, int ps)
	: EpollBase(o->owner_thread(), fd), owner(o), receiver(fd),
	  stage(IdleState), job(NULL), reply(NULL)
{
	addrLen = ps;
	addr = MALLOC(ps);
	resource_id = 0;
	resource = NULL;

	if (addr == NULL)
		throw(int) - ENOMEM;

	memcpy((char *)addr, (char *)peer, ps);

	if (!statEnable) {
		statAcceptCount = g_stat_mgr.get_stat_int_counter(ACCEPT_COUNT);
		statCurConnCount = g_stat_mgr.get_stat_int_counter(CONN_COUNT);
		statEnable = 1;
	}

	/* ClientSync deleted if allocate resource failed. clean resource allocated */
	get_resource();
	if (resource) {
		job = resource->job;
		reply = resource->packet;
	} else {
		throw(int) - ENOMEM;
	}
	rscStatus = RscClean;

	++statAcceptCount;
	++statCurConnCount;
}

ClientSync::~ClientSync()
{
	if (job) {
		if (stage == ProcReqState) {
			/* job in use, save resource to reply */
			job->clear_owner_info();
			job->resource_id = resource_id;
			job->resource_owner = owner;
			job->resource_seq = resource_seq;
		} else {
			/* job not in use, clean resource, free resource */
			if (resource) {
				clean_resource();
				rscStatus = RscClean;
				free_resource();
			}
		}
	}

	//DELETE(reply);
	FREE_IF(addr);
	--statCurConnCount;
}

int ClientSync::do_attach()
{
	enable_input();
	if (attach_poller() == -1)
		return -1;

	attach_timer(owner->idle_list());

	stage = IdleState;
	return 0;
}

//server peer
int ClientSync::recv_request()
{
	/* clean job from pool, basic init take place of old construction */
	if (RscClean == rscStatus) {
		job->set_data_table(owner->owner_table());
		job->set_hotbackup_table(owner->admin_table());
		job->set_role_as_server();
		job->begin_stage();
		receiver.erase();
		rscStatus = RscDirty;
	}

	disable_timer();

	int ret = job->do_decode(receiver);
	switch (ret) {
	default:
	case DecodeFatalError:
		if (errno != 0)
			log4cplus_info("decode fatal error, ret = %d msg = %m",
				       ret);
		return -1;

	case DecodeDataError:
		job->response_timer_start();
		job->mark_as_hit();
		if (job->result_code() < 0)
			log4cplus_info(
				"DecodeDataError, role=%d, fd=%d, result=%d",
				job->Role(), netfd, job->result_code());
		return send_result();

	case DecodeIdle:
		attach_timer(owner->idle_list());
		stage = IdleState;
		rscStatus = RscClean;
		break;

	case DecodeWaitData:
		stage = RecvReqState;
		break;

	case DecodeDone:
		if ((ret = job->prepare_process()) < 0) {
			log4cplus_debug("build packed key error: %d", ret);
			return send_result();
		}

		disable_output();
		job->set_owner_info(this, 0, (struct sockaddr *)addr);
		stage = ProcReqState;
		job->push_reply_dispatcher(&syncReplyObject);
		owner->task_dispatcher(job);
	}
	return 0;
}

/* keep job in resource slot */
int ClientSync::send_result(void)
{
	stage = SendRepState;

	owner->record_job_procedure_time(job);

	if (job->flag_keep_alive())
		job->versionInfo.set_keep_alive_timeout(owner->idle_time());
	reply->encode_result(job);

	return Response();
}

int ClientSync::Response(void)
{
	int ret = reply->Send(netfd);

	switch (ret) {
	case SendResultMoreData:
		enable_output();
		return 0;

	case SendResultDone:
		clean_resource();
		rscStatus = RscClean;
		stage = IdleState;
		disable_output();
		enable_input();
		attach_timer(owner->idle_list());
		return 0;

	default:
		log4cplus_info("send failed, return = %d, error = %m", ret);
		return -1;
	}
	return 0;
}

void ClientSync::input_notify(void)
{
	log4cplus_debug("enter input_notify.");
	if (stage == IdleState || stage == RecvReqState) {
		if (recv_request() < 0) {
			delete this;
			return;
		}
	}

	/* receive input events again. */
	else {
		/*  check whether client close connection. */
		if (check_link_status()) {
			log4cplus_debug(
				"client close connection, delete ClientSync obj, stage=%d",
				stage);
			delete this;
			return;
		} else {
			disable_input();
		}
	}

	delay_apply_events();
	log4cplus_debug("leave input_notify.");
}

void ClientSync::output_notify(void)
{
	log4cplus_debug("enter output_notify.");
	if (stage == SendRepState) {
		if (Response() < 0)
			delete this;
	} else {
		disable_output();
		log4cplus_info("Spurious output_notify, stage=%d", stage);
	}
	log4cplus_debug("leave output_notify.");
}

int ClientSync::get_resource()
{
	return owner->regist_resource(&resource, resource_id, resource_seq);
}

void ClientSync::free_resource()
{
	job = NULL;
	reply = NULL;
	owner->unregist_resource(resource_id, resource_seq);
}

void ClientSync::clean_resource()
{
	owner->clean_resource(resource_id);
}
