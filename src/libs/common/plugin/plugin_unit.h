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
#ifndef __H_DTC_PLUGIN_UNIT_H__
#define __H_DTC_PLUGIN_UNIT_H__

#include "stat_dtc.h"
#include "decode/decoder_base.h"
#include "plugin_request.h"
#include "../poll/poller_base.h"

class PluginDecoderUnit : public DecoderBase {
    public:
	PluginDecoderUnit(PollerBase *owner, int idletimeout);
	virtual ~PluginDecoderUnit();

	virtual int process_stream(int fd, int req, void *peer, int peerSize);
	virtual int process_dgram(int fd);

	inline void record_job_procedure_time(unsigned int msec)
	{
	}

	inline incoming_notify_t *get_incoming_notifier(void)
	{
		return &_incoming_notify;
	}

	inline int attach_incoming_notifier(void)
	{
		return _incoming_notify.attach_poller(owner);
	}

    private:
	incoming_notify_t _incoming_notify;
};

#endif
