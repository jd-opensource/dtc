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
#ifndef __HELPER_CLIENT_H__
#define __HELPER_CLIENT_H__

#include "poll/poller.h"
#include "packet/packet.h"
#include "timer/timer_list.h"
#include "task/task_request.h"
#include "stop_watch.h"

enum HelperState {
	HelperDisconnected = 0,
	HelperConnecting,
	HelperIdleState,
	HelperRecvRepState, //wait for recv response, client side
	HelperSendReqState, //wait for send request, client side
	HelperSendVerifyState,
	HelperRecvVerifyState,
	HelperSendNotifyReloadConfigState,
	HelperRecvNotifyReloadConfigState,
	HelperSendNotifyCheckState,
	HelperRecvNotifyCheckState,
};

class ConnectorGroup;

/*
  State Machine: ConnectorClient object is static, beyond reconnect
 	Disconnected
		wait retryTimeout --> trying reconnect
 	reconnect
		If connected --> IdleState
        	If inprogress --> ConnectingState
	ConnectingState
		If hangup_notify --> Disconnected
		If output_notify --> SendVerifyState
		If job_timer_procedure:connTimeout --> Disconnected
	SendVerifyState
		If hangup_notify --> Disconnected
		If output_notify --> Trying Sending
		If job_timer_procedure:connTimeout --> Disconnected
	RecvVerifyStat
		If hangup_notify --> complete_task(error) -->reconnect
		If input_notify --> DecodeDone --> IdleState
	IdleState
		If hangup_notify --> reconnect
		If attach_task --> Trying Sending
	Trying Sending
		If Sent --> RecvRepState
		If MoreData --> SendRepState
		If SentError --> PushBackTask --> reconnect
	SendRepState
		If hangup_notify --> PushBackTask -->reconnect
		If output_notify --> Trying Sending
	RecvRepState
		If hangup_notify --> complete_task(error) -->Reconnect
		If input_notify --> do_decode Reply
	DecodeReply
		If DecodeDone --> IdleState
		If MoreData --> RecvRepState
		If FatalError --> complete_task(error) --> reconnect
		If DataError --> complete_task(error) --> reconnect
	
 */
class ConnectorClient : public EpollBase, private TimerObject {
    public:
	friend class ConnectorGroup;

	ConnectorClient(EpollOperation *, ConnectorGroup *hg, int id , int i_enable_check);
	virtual ~ConnectorClient();

	int attach_task(DTCJobOperation *, Packet *);

	int support_batch_key(void) const
	{
		return supportBatchKey;
	}

    private:
	int Reset();
	int reconnect();

	int send_verify();
	int recv_verify();

	int client_notify_helper_reload_config();
	int send_notify_helper_reload_config();
	int recv_notify_helper_reload_config();

	int client_notify_helper_check();
	int send_notify_helper_check();
	int recv_notify_helper_check();

	int Ready();
	int connect_error();

	void complete_task(void);
	void queue_back_task(void);
	void set_error(int err, const char *msg, const char *msg1)
	{
		job->set_error(err, msg, msg1);
	}
	void set_error_dup(int err, const char *msg, const char *msg1)
	{
		job->set_error_dup(err, msg, msg1);
	}

    public:
	const char *state_string(void)
	{
		return this == NULL ?
			       "NULL" :
			       ((const char *[]){ "DISC", "CONN", "IDLE",
						  "RECV", "SEND", "SND_VER",
						  "RECV_VER", "BAD" })[stage];
	}

    private:
	virtual void input_notify(void);
	virtual void output_notify(void);
	virtual void hangup_notify(void);
	virtual void job_timer_procedure(void);

    private:
	int recv_response();
	int send_request();
	int connect_server(const char *path);

	SimpleReceiver receiver;
	DTCJobOperation *job;
	DTCJobOperation* check_job;
	Packet *packet;

	DTCJobOperation *verify_task;
	Packet *verify_packet;

	ConnectorGroup *helperGroup;
	int helperIdx;

	HelperState stage;

	int supportBatchKey;
	static const unsigned int maxTryConnect = 10;
	uint64_t connectErrorCnt;
	int ready;
	stopwatch_usec_t stopWatch;
	int i_enable_check_;
};
#endif
