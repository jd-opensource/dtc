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
#ifndef __PLUGIN__DGRAM_H__
#define __PLUGIN__DGRAM_H__

#include <sys/socket.h>

#include "../poll/poller.h"
#include "../timer/timer_list.h"
#include "plugin_decoder.h"
#include "plugin_unit.h"

class PluginDatagram;

class PluginDgram : public EpollBase {
    public:
	PluginDgram(PluginDecoderUnit *, int fd);
	virtual ~PluginDgram();

	virtual int do_attach(void);
	inline int send_response(PluginDatagram *plugin_request)
	{
		int ret = 0;
		_owner->record_job_procedure_time(
			plugin_request->_response_timer.live());

		if (!plugin_request->recv_only()) {
			ret = _plugin_sender.sendto(plugin_request);
		}

		DELETE(plugin_request);
		return ret;
	}

    private:
	virtual void input_notify(void);

    protected:
	int recv_request(void);
	int init_request(void);

    private:
	int mtu;
	int _addr_len;
	PluginDecoderUnit *_owner;
	worker_notify_t *_worker_notifier;
	PluginReceiver _plugin_receiver;
	PluginSender _plugin_sender;
	uint32_t _local_ip;

    private:
	int init_socket_info(void);
};

#endif
