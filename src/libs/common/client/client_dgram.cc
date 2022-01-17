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
#include <stdio.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

#include "client_dgram.h"
#include "task/task_request.h"
#include "client/client_unit.h"
#include "protocol.h"
#include "log/log.h"

class PollerBase;
static int GetSocketFamily(int fd)
{
	struct sockaddr addr;
	bzero(&addr, sizeof(addr));
	socklen_t alen = sizeof(addr);
	getsockname(fd, &addr, &alen);
	return addr.sa_family;
}

class ReplyDgram : public JobAnswerInterface<DTCJobOperation> {
    public:
	ReplyDgram(void)
	{
	}
	virtual ~ReplyDgram(void);
	virtual void job_answer_procedure(DTCJobOperation *job);
};

ReplyDgram::~ReplyDgram(void)
{
}

void ReplyDgram::job_answer_procedure(DTCJobOperation *job)
{
	DgramInfo *info = job->OwnerInfo<DgramInfo>();
	if (info == NULL) {
		delete job;
	} else if (info->cli == NULL) {
		log4cplus_error("info->cli is NULL, possible memory corrupted");
		FREE(info);
		delete job;
	} else {
		info->cli->send_result(job, info->addr, info->len);
		FREE(info);
	}
}

static ReplyDgram replyDgram;

ClientDgram::ClientDgram(DTCDecoderUnit *o, int fd)
	: EpollBase(o->owner_thread(), fd), owner(o), hastrunc(0), mru(0),
	  mtu(0), alen(0), abuf(NULL)
{
}

ClientDgram::~ClientDgram()
{
	FREE_IF(abuf);
}

int ClientDgram::init_socket_info(void)
{
	switch (GetSocketFamily(netfd)) {
	default:
		mru = 65508;
		mtu = 65507;
		alen = sizeof(struct sockaddr);
		break;
	case AF_UNIX:
		mru = 16 << 20;
		mtu = 16 << 20;
		alen = sizeof(struct sockaddr_un);
		break;
	case AF_INET:
		hastrunc = 1;
		mru = 65508;
		mtu = 65507;
		alen = sizeof(struct sockaddr_in);
		break;
	case AF_INET6:
		hastrunc = 1;
		mru = 65508;
		mtu = 65507;
		alen = sizeof(struct sockaddr_in6);
		break;
	}
	return 0;
}

int ClientDgram::allocate_dgram_info(void)
{
	if (abuf != NULL)
		return 0;

	abuf = (DgramInfo *)MALLOC(sizeof(DgramInfo) + alen);
	if (abuf == NULL)
		return -1;

	abuf->cli = this;
	return 0;
}

int ClientDgram::do_attach()
{
	init_socket_info();

	enable_input();
	if (attach_poller() == -1)
		return -1;

	return 0;
}

// server peer
// return value: -1, fatal error or no more packets
// return value: 0, more packet may be pending
int ClientDgram::recv_request(int noempty)
{
	if (allocate_dgram_info() < 0) {
		log4cplus_error(
			"%s",
			"create DgramInfo object failed, msg[no enough memory]");
		return -1;
	}

	int data_len;
	if (hastrunc) {
		char dummy[1];
		abuf->len = alen;
		data_len = recvfrom(netfd, dummy, 1,
				    MSG_DONTWAIT | MSG_TRUNC | MSG_PEEK,
				    (sockaddr *)abuf->addr, &abuf->len);
		if (data_len < 0) {
			// NO ERROR, and assume NO packet pending
			return -1;
		}
	} else {
		data_len = -1;
		ioctl(netfd, SIOCINQ, &data_len);
		if (data_len < 0) {
			log4cplus_error("%s", "next packet size unknown");
			return -1;
		}

		if (data_len == 0) {
			if (noempty) {
				// NO ERROR, and assume NO packet pending
				return -1;
			}
			/* treat 0 as max packet, because we can't ditiguish the no-packet & zero-packet */
			data_len = mru;
		}
	}

	char *buf = (char *)MALLOC(data_len);
	if (buf == NULL) {
		log4cplus_error(
			"allocate packet buffer[%d] failed, msg[no enough memory]",
			data_len);
		return -1;
	}

	abuf->len = alen;
	data_len = recvfrom(netfd, buf, data_len, MSG_DONTWAIT,
			    (sockaddr *)abuf->addr, &abuf->len);
	if (abuf->len <= 1) {
		log4cplus_info("recvfrom error: no source address");
		free(buf);
		// more packet pending
		return 0;
	}

	if (data_len <= (int)sizeof(DTC_HEADER_V1)) {
		int err = data_len == -1 ? errno : 0;
		if (err != EAGAIN)
			log4cplus_info("recvfrom error: size=%d errno=%d",
				       data_len, err);
		free(buf);
		// more packet pending
		return 0;
	}

	DTCJobOperation *job = new DTCJobOperation(owner->owner_table());
	if (NULL == job) {
		log4cplus_error(
			"%s",
			"create DTCJobOperation object failed, msg[no enough memory]");
		return -1;
	}

	job->set_hotbackup_table(owner->admin_table());

	int ret = job->do_decode(buf, data_len, 1 /*eat*/);
	switch (ret) {
	default:
	case DecodeFatalError:
		if (errno != 0)
			log4cplus_info("decode fatal error, ret = %d msg = %m",
				       ret);
		free(buf); // buf not eatten
		break;

	case DecodeDataError:
		job->response_timer_start();
		if (job->result_code() < 0)
			log4cplus_info(
				"DecodeDataError, role=%d, fd=%d, result=%d",
				job->Role(), netfd, job->result_code());
		send_result(job, (void *)abuf->addr, abuf->len);
		break;

	case DecodeDone:
		if (job->prepare_process() < 0) {
			send_result(job, (void *)abuf->addr, abuf->len);
			break;
		}

		job->set_owner_info(abuf, 0, (sockaddr *)abuf->addr);
		abuf = NULL; // eat abuf

		job->push_reply_dispatcher(&replyDgram);
		owner->task_dispatcher(job);
	}
	return 0;
}

int ClientDgram::send_result(DTCJobOperation *job, void *addr, int len)
{
	if (job == NULL)
		return 0;

	owner->record_job_procedure_time(job);
	Packet *reply = new Packet;
	if (reply == NULL) {
		delete job;
		log4cplus_error("create Packet object failed");
		return 0;
	}

	job->versionInfo.set_keep_alive_timeout(owner->idle_time());
	reply->encode_result(job, mtu);
	delete job;

	int ret = reply->send_to(netfd, (struct sockaddr *)addr, len);

	delete reply;

	if (ret != SendResultDone) {
		log4cplus_info("send failed, return = %d, error = %m", ret);
		return 0;
	}
	return 0;
}

void ClientDgram::input_notify(void)
{
	log4cplus_debug("enter input_notify.");
	const int batchsize = 64;
	for (int i = 0; i < batchsize; ++i) {
		if (recv_request(i) < 0)
			break;
	}
	log4cplus_debug("leave input_notify.");
}
