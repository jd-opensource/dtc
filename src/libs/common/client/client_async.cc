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
#include <sys/socket.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

#include "client_async.h"
#include "client_unit.h"
#include "poll/poller_base.h"
#include "log/log.h"

static int statEnable = 0;
static StatCounter statAcceptCount;
static StatCounter statCurConnCount;

inline AsyncInfo::AsyncInfo(ClientAsync *c, DTCJobOperation *r)
	: cli(c), req(r), pkt(NULL)

{
	c->numReq++;
	/* move to waiting list */
	ListAddTail(&c->waitList);
}

inline AsyncInfo::~AsyncInfo()
{
	if (req)
		req->clear_owner_info();
	DELETE(pkt);
	if (cli)
		cli->numReq--;
}

class CReplyAsync : public JobAnswerInterface<DTCJobOperation> {
    public:
	CReplyAsync(void)
	{
	}
	virtual ~CReplyAsync(void);
	virtual void job_answer_procedure(DTCJobOperation *job);
};

CReplyAsync::~CReplyAsync(void)
{
}

void CReplyAsync::job_answer_procedure(DTCJobOperation *job)
{
	AsyncInfo *info = job->OwnerInfo<AsyncInfo>();
	if (info == NULL) {
		delete job;
	} else if (info->cli == NULL) {
		log4cplus_error("info->cli is NULL, possible memory corrupted");
		delete job;
	} else {
		info->cli->queue_result(info);
	}
}

static CReplyAsync replyAsync;

ClientAsync::ClientAsync(DTCDecoderUnit *o, int fd, int m, void *peer, int ps)
	: EpollBase(o->owner_thread(), fd), owner(o), receiver(fd),
	  curReq(NULL), curRes(NULL), maxReq(m), numReq(0)
{
	addrLen = ps;
	addr = MALLOC(ps);
	memcpy((char *)addr, (char *)peer, ps);

	if (!statEnable) {
		statAcceptCount = g_stat_mgr.get_stat_int_counter(ACCEPT_COUNT);
		statCurConnCount = g_stat_mgr.get_stat_int_counter(CONN_COUNT);
		statEnable = 1;
	}
	++statAcceptCount;
	++statCurConnCount;
}

ClientAsync::~ClientAsync()
{
	while (!done_list.ListEmpty()) {
		AsyncInfo *info = done_list.NextOwner();
		delete info;
	}
	while (!waitList.ListEmpty()) {
		AsyncInfo *info = waitList.NextOwner();
		delete info;
	}
	--statCurConnCount;

	FREE_IF(addr);
}

int ClientAsync::do_attach()
{
	enable_input();
	if (attach_poller() == -1)
		return -1;

	return 0;
}

//server peer
int ClientAsync::recv_request()
{
	if (curReq == NULL) {
		curReq = new DTCJobOperation(owner->owner_table());
		if (NULL == curReq) {
			log4cplus_error(
				"%s",
				"create DTCJobOperation object failed, msg[no enough memory]");
			return -1;
		}
		curReq->set_hotbackup_table(owner->admin_table());
		receiver.erase();
	}

	int ret = curReq->do_decode(receiver);
	switch (ret) {
	default:
	case DecodeFatalError:
		if (errno != 0)
			log4cplus_info("decode fatal error, ret = %d msg = %m",
				       ret);
		DELETE(curReq);
		return -1;

	case DecodeDataError:
		curReq->response_timer_start();
		curReq->mark_as_hit();
		if (curReq->result_code() < 0)
			log4cplus_info(
				"DecodeDataError, role=%d, fd=%d, result=%d",
				curReq->Role(), netfd, curReq->result_code());
		return queue_error();

	case DecodeDone:
		if (curReq->prepare_process() < 0)
			return queue_error();
		curReq->set_owner_info(new AsyncInfo(this, curReq), 0,
				       (struct sockaddr *)addr);
		curReq->push_reply_dispatcher(&replyAsync);
		owner->task_dispatcher(curReq);
		curReq = NULL;
	}
	return 0;
}

int ClientAsync::queue_error(void)
{
	AsyncInfo *info = new AsyncInfo(this, curReq);
	curReq = NULL;
	return queue_result(info);
}

int ClientAsync::queue_result(AsyncInfo *info)
{
	if (info->req == NULL) {
		delete info;
		return 0;
	}

	DTCJobOperation *const job_operation = info->req;
	owner->record_job_procedure_time(curReq);

	/* move to sending list */
	info->list_move_tail(&done_list);
	/* convert request to result */
	info->pkt = new Packet;
	if (info->pkt == NULL) {
		delete info->req;
		info->req = NULL;
		delete info;
		log4cplus_error("create Packet object failed");
		return 0;
	}

	job_operation->versionInfo.set_keep_alive_timeout(owner->idle_time());
	info->pkt->encode_result(info->req);
	DELETE(info->req);

	Response();
	adjust_events();
	return 0;
}

int ClientAsync::response_one(void)
{
	if (curRes == NULL)
		return 0;

	int ret = curRes->Send(netfd);

	switch (ret) {
	case SendResultMoreData:
		return 0;

	case SendResultDone:
		DELETE(curRes);
		numReq--;
		return 0;

	default:
		log4cplus_info("send failed, return = %d, error = %m", ret);
		return -1;
	}
	return 0;
}

int ClientAsync::Response(void)
{
	do {
		int ret;
		if (curRes == NULL) {
			// All result sent
			if (done_list.ListEmpty())
				break;

			AsyncInfo *info = done_list.NextOwner();
			curRes = info->pkt;
			info->pkt = NULL;
			delete info;
			numReq++;
			if (curRes == NULL)
				continue;
		}
		ret = response_one();
		if (ret < 0)
			return ret;
	} while (curRes == NULL);
	return 0;
}

void ClientAsync::input_notify(void)
{
	log4cplus_debug("enter input_notify.");
	if (recv_request() < 0)
		delete this;
	else
		adjust_events();
	log4cplus_debug("leave input_notify.");
}

void ClientAsync::output_notify(void)
{
	log4cplus_debug("enter output_notify.");
	if (Response() < 0)
		delete this;
	else
		adjust_events();
	log4cplus_debug("leave output_notify.");
}

int ClientAsync::adjust_events(void)
{
	if (curRes == NULL && done_list.ListEmpty())
		disable_output();
	else
		enable_output();
	if (numReq >= maxReq)
		disable_input();
	else
		enable_input();
	return apply_events();
}
