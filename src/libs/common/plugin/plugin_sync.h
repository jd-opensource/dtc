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
#ifndef __PLUGIN_SYNC_H__
#define __PLUGIN_SYNC_H__

#include "../poll/poller.h"
#include "../timer/timer_list.h"
#include "plugin_unit.h"
#include "plugin_decoder.h"

class PluginDecoderUnit;

typedef enum {
	PLUGIN_IDLE,
	PLUGIN_RECV, //wait for recv request, server side
	PLUGIN_SEND, //wait for send response, server side
	PLUGIN_PROC //IN processing
} plugin_state_t;

class PluginSync : public EpollBase, private TimerObject {
    public:
	PluginSync(PluginDecoderUnit *plugin_decoder, int fd, void *peer,
		   int peer_size);
	virtual ~PluginSync();

	int do_attach(void);
	virtual void input_notify(void);

	inline int send_response(void)
	{
		owner->record_job_procedure_time(
			_plugin_request->_response_timer.live());
		if (Response() < 0) {
			delete this;
		} else {
			delay_apply_events();
		}

		return 0;
	}

	inline void set_stage(plugin_state_t stage)
	{
		_plugin_stage = stage;
	}

    private:
	virtual void output_notify(void);

    protected:
	plugin_state_t _plugin_stage;

	int recv_request(void);
	int Response(void);

    private:
	int create_request(void);
	int proc_multi_request(void);

    private:
	PluginDecoderUnit *owner;
	PluginStream *_plugin_request;
	worker_notify_t *_worker_notifier;
	void *_addr;
	int _addr_len;
	PluginReceiver _plugin_receiver;
	PluginSender _plugin_sender;
};

#endif
