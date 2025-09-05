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
#ifndef __DTC_PLUGIN_DECODER_H__
#define __DTC_PLUGIN_DECODER_H__

#include "algorithm/non_copyable.h"
#include "plugin_global.h"
#include "plugin_request.h"

typedef enum {
	NET_IDLE,
	NET_FATAL_ERROR,
	NET_DISCONNECT,
	NET_RECVING,
	NET_SENDING,
	NET_SEND_DONE,
	NET_RECV_DONE,

} net_state_t;

class PluginReceiver : private noncopyable {
    public:
	PluginReceiver(int fd, dll_func_t *dll)
		: _fd(fd), _stage(NET_IDLE), _dll(dll), _all_len_changed(0)
	{
	}
	~PluginReceiver(void)
	{
	}

	int recv(PluginStream *request);
	int recvfrom(PluginDatagram *request, int mtu);
	inline int proc_remain_packet(PluginStream *request)
	{
		request->recalc_multipacket();
		return parse_protocol(request);
	}

	inline void set_stage(net_state_t stage)
	{
		_stage = stage;
	}

    public:
    protected:
    protected:
    private:
	int parse_protocol(PluginStream *request);

    private:
	int _fd;
	net_state_t _stage;
	dll_func_t *_dll;
	int _all_len_changed;
};

class PluginSender : private noncopyable {
    public:
	PluginSender(int fd, dll_func_t *dll)
		: _fd(fd), _stage(NET_IDLE), _dll(dll)
	{
	}

	~PluginSender(void)
	{
	}

	int send(PluginStream *request);
	int sendto(PluginDatagram *request);

	inline void set_stage(net_state_t stage)
	{
		_stage = stage;
	}

    public:
    protected:
    protected:
    private:
    private:
	int _fd;
	net_state_t _stage;
	dll_func_t *_dll;
};

#endif
