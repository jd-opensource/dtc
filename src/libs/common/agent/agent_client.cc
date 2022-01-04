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
#include <errno.h>

#include "agent/agent_client.h"
#include "job_entrance_ask_chain.h"
#include "agent_receiver.h"
#include "agent_sender.h"
#include "log/log.h"
#include "poll/poller_base.h"
#include "agent/agent_multi_request.h"
#include "packet/packet.h"
#include "task/task_request.h"
#include "agent/agent_multi_request.h"
#include "stat_dtc.h"
#include "table/table_def_manager.h"

extern DTCTableDefinition *g_table_def[];

static StatCounter stat_agent_accept_count;
static StatCounter stat_agent_cur_conn_count;
static StatCounter stat_agent_expore_count;
static StatCounter stat_task_client_timeout;

AgentResultQueue::~AgentResultQueue()
{
	Packet *p;

	while (NULL != (p = packet.Pop())) {
		p->free_result_buff();
		delete p;
	}
}

class AgentReply : public JobAnswerInterface<DTCJobOperation> {
    public:
	AgentReply()
	{
		init_stat_flag = false;
	}
	virtual ~AgentReply()
	{
	}
	virtual void job_answer_procedure(DTCJobOperation *job);

    private:
	bool init_stat_flag;
};

void AgentReply::job_answer_procedure(DTCJobOperation *job)
{
	log4cplus_debug("AgentReply::job_answer_procedure start");

	ClientAgent *client = job->owner_client();
	if (client == NULL) {
		/* client gone, finish this job */
		job->done_one_agent_sub_request();
		return;
	}

	client->record_request_process_time(job);

	int client_timeout = job->requestInfo.tag_present(1) == 0 ?
				     job->default_expire_time() :
				     job->requestInfo.get_expire_time(
					     job->versionInfo.CTLibIntVer());
	int req_delaytime = 0;

	if (!init_stat_flag) {
		stat_agent_expore_count =
			g_stat_mgr.get_stat_int_counter(INCOMING_EXPIRE_REQ);
		stat_task_client_timeout =
			g_stat_mgr.get_stat_int_counter(TASK_CLIENT_TIMEOUT);
		init_stat_flag = true;
	}

	stat_task_client_timeout = client_timeout;
	log4cplus_debug("job client_timeout: %d", client_timeout);

	if ((req_delaytime / 1000) >= client_timeout) //ms
	{
		log4cplus_debug(
			"AgentReply::job_answer_procedure client_timeout[%d]ms, req delay time[%d]us",
			client_timeout, req_delaytime);
		job->done_one_agent_sub_request();
		stat_agent_expore_count++;
		return;
	}
	Packet *packet = new Packet();
	if (packet == NULL) {
		/* make response error, finish this job */
		job->done_one_agent_sub_request();
		log4cplus_error("no mem new Packet");
		return;
	}

	packet->encode_result(job);
	job->detach_result_in_result_writer();
	job->done_one_agent_sub_request();

	client->add_packet(packet);
	if (client->send_result() < 0) {
		log4cplus_error("cliengAgent send_result error");
		delete client;
		return;
	}

	log4cplus_debug("AgentReply::job_answer_procedure stop");
}

static AgentReply agent_reply;

/* sender and receiver should inited ok */
ClientAgent::ClientAgent(PollerBase *o, JobEntranceAskChain *u, int fd)
	: EpollBase(o, fd), ownerThread(o), owner(u), tlist(NULL)
{
	tlist = u->get_timer_list();
	sender = new AgentSender(fd);
	if (NULL == sender) {
		log4cplus_error("no mem to new sender");
		throw(int) - ENOMEM;
	}

	if (sender && sender->initialization() < 0) {
		delete sender;
		sender = NULL;
		log4cplus_error("no mem to init sender");
		throw(int) - ENOMEM;
	}

	if (sender) {
		receiver = new AgentReceiver(fd);
		if (NULL == receiver) {
			log4cplus_error("no mem to new receiver");
			throw(int) - ENOMEM;
		}

		if (receiver && receiver->initialization() < 0) {
			log4cplus_error("no mem to init receiver");
			throw(int) - ENOMEM;
		}
	}

	stat_agent_accept_count =
		g_stat_mgr.get_stat_int_counter(AGENT_ACCEPT_COUNT);
	stat_agent_cur_conn_count =
		g_stat_mgr.get_stat_int_counter(AGENT_CONN_COUNT);

	stat_agent_accept_count++;
	stat_agent_cur_conn_count++;
}

ClientAgent::~ClientAgent()
{
	log4cplus_debug("~ClientAgent start");
	ListObject<AgentMultiRequest> *node = rememberReqHeader.ListNext();
	AgentMultiRequest *req;

	/* notify all request of this client I'm gone */
	while (node != &rememberReqHeader) {
		req = node->ListOwner();
		req->clear_owner_info();
		req->detach_from_owner_client();
		node = rememberReqHeader.ListNext();
	}

	if (receiver)
		delete receiver;
	if (sender)
		delete sender;

	detach_poller();

	stat_agent_cur_conn_count--;
	log4cplus_debug("~ClientAgent end");
}

int ClientAgent::attach_thread()
{
	disable_output();
	enable_input();

	if (attach_poller() < 0) {
		log4cplus_error("client agent attach agengInc thread failed");
		return -1;
	}

	/* no idle test */
	return 0;
}

void ClientAgent::remember_request(DTCJobOperation *request)
{
	request->link_to_owner_client(rememberReqHeader);
}

DTCJobOperation *ClientAgent::parse_job_message(char *recvbuff, int recvlen,
						int pktcnt, uint8_t pktver)
{
	DTCJobOperation *job;

	job = new DTCJobOperation(
		TableDefinitionManager::instance()->get_cur_table_def());
	if (NULL == job) {
		free(recvbuff);
		log4cplus_error("no mem allocate for new agent request");
		return NULL;
	}

	job->set_hotbackup_table(
		TableDefinitionManager::instance()->get_hot_backup_table_def());
	job->set_owner_info(this, 0, NULL);
	job->set_owner_client(this);
	job->push_reply_dispatcher(&agent_reply);
	job->save_recved_result(recvbuff, recvlen, pktcnt, pktver);

	/* assume only a few sub request decode error */
	if (job->decode_agent_request() < 0) {
		delete job;
		return NULL;
	}

	/* no mem new job case */
	if (job->is_agent_request_completed()) {
		delete job;
		return NULL;
	}

	remember_request(job);

	return job;
}

int ClientAgent::recv_request()
{
	RecvedPacket packets;
	char *recvbuff = NULL;
	int recvlen = 0;
	int pktcnt = 0;
	DTCJobOperation *job_operation = NULL;

	packets = receiver->receive_network_packet();

	if (packets.err < 0)
		return -1;
	else if (packets.pktCnt == 0)
		return 0;

	recvbuff = packets.buff;
	recvlen = packets.len;
	pktcnt = packets.pktCnt;

	job_operation =
		parse_job_message(recvbuff, recvlen, pktcnt, packets.version);
	if (job_operation != NULL)
		owner->start_job_ask_procedure(job_operation);

	return 0;
}

/* exit when recv error*/
void ClientAgent::input_notify()
{
	log4cplus_debug("enter input_notify.");
	if (recv_request() < 0) {
		log4cplus_debug("erro when recv");
		delete this;
		return;
	}
	delay_apply_events();
	log4cplus_debug("leave input_notify.");
	return;
}

/*
return error if sender broken
*/
int ClientAgent::send_result()
{
	if (sender->is_broken()) {
		log4cplus_error("sender broken");
		return -1;
	}

	while (1) {
		Packet *frontPkt = resQueue.Front();
		if (NULL == frontPkt) {
			break;
		}

		if (frontPkt->vec_count() + sender->vec_count() >
		    SENDER_MAX_VEC) {
			/*这个地方打印error，如果在10s内会5次走入此次分支的话，统计子进程会上报告警*/
			log4cplus_error(
				"the sum value of front packet veccount[%d] and sender veccount[%d]is greater than SENDER_MAX_VEC[%d]",
				frontPkt->vec_count(), sender->vec_count(),
				SENDER_MAX_VEC);
			break;
		} else {
			Packet *pkt;
			pkt = resQueue.Pop();
			if (NULL == pkt) {
				break;
			}
			if (sender->add_packet(pkt) < 0) {
				return -1;
			}
		}
	}

	if (sender->send_packet() < 0) {
		log4cplus_error("agent client send error");
		return -1;
	}

	if (sender->left_len() != 0)
		enable_output();
	else
		disable_output();

	delay_apply_events();

	return 0;
}

void ClientAgent::output_notify()
{
	log4cplus_debug("enter output_notify.");
	if (send_result() < 0) {
		log4cplus_debug("error when response");
		delete this;
		return;
	}
	log4cplus_debug("leave output_notify.");

	return;
}

void ClientAgent::record_request_process_time(DTCJobOperation *job)
{
	owner->record_job_procedure_time(job);
}
