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
#include <assert.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>

#include "plugin_agent_mgr.h"
#include "plugin_sync.h"
#include "plugin_unit.h"
#include "../poll/poller_base.h"
#include "../log/log.h"
#include "stat_dtc.h"
#include "mem_check.h"

//static int statEnable=0;
static StatCounter statPluginAcceptCount;
static StatCounter statPluginCurConnCount;
extern "C" {
extern unsigned int get_local_ip();
}

PluginSync::PluginSync(PluginDecoderUnit *plugin_decoder, int fd, void *peer,
		       int peer_size)
	: EpollBase(plugin_decoder->owner_thread(), fd),
	  _plugin_stage(PLUGIN_IDLE), owner(plugin_decoder),
	  _plugin_request(NULL), _worker_notifier(NULL),
	  _plugin_receiver(fd, PluginAgentManager::instance()->get_dll()),
	  _plugin_sender(fd, PluginAgentManager::instance()->get_dll())
{
	_addr_len = peer_size;
	_addr = MALLOC(peer_size);
	memcpy((char *)_addr, (char *)peer, peer_size);

	++statPluginAcceptCount;
	++statPluginCurConnCount;
}

PluginSync::~PluginSync(void)
{
	--statPluginCurConnCount;

	if (PLUGIN_PROC == _plugin_stage) {
		_plugin_request->_plugin_sync = NULL;
	} else {
		DELETE(_plugin_request);
	}

	FREE_CLEAR(_addr);
}

int PluginSync::create_request(void)
{
	if (NULL != _plugin_request) {
		DELETE(_plugin_request);
		_plugin_request = NULL;
	}

	NEW(PluginStream(this, PluginAgentManager::instance()->get_dll()),
	    _plugin_request);
	if (NULL == _plugin_request) {
		log4cplus_error("create plugin request object failed, msg:%s",
				strerror(errno));
		return -1;
	}

	//set plugin request info
	_plugin_request->_skinfo.sockfd = netfd;
	_plugin_request->_skinfo.type = SOCK_STREAM;
	_plugin_request->_skinfo.local_ip = get_local_ip();
	_plugin_request->_skinfo.local_port = 0;
	_plugin_request->_skinfo.remote_ip =
		((struct sockaddr_in *)_addr)->sin_addr.s_addr;
	_plugin_request->_skinfo.remote_port =
		((struct sockaddr_in *)_addr)->sin_port;
	_plugin_request->_incoming_notifier = owner->get_incoming_notifier();

	return 0;
}

int PluginSync::do_attach()
{
	if (create_request() != 0) {
		log4cplus_error("create request object failed");
		return -1;
	}

	//get worker notifier
	_worker_notifier =
		PluginAgentManager::instance()->get_worker_notifier();
	if (NULL == _worker_notifier) {
		log4cplus_error("get worker notifier failed.");
		return -1;
	}

	enable_input();

	if (attach_poller() == -1) {
		return -1;
	}

	attach_timer(owner->idle_list());
	_plugin_stage = PLUGIN_IDLE;

	return 0;
}

int PluginSync::recv_request()
{
	disable_timer();

	int ret_stage = _plugin_receiver.recv(_plugin_request);

	switch (ret_stage) {
	default:
	case NET_FATAL_ERROR:
		return -1;

	case NET_IDLE:
		attach_timer(owner->idle_list());
		_plugin_stage = PLUGIN_IDLE;
		break;

	case NET_RECVING:
		//If partial packet received, need to add to idle list to prevent connection hang
		attach_timer(owner->idle_list());
		_plugin_stage = PLUGIN_RECV;
		break;

	case NET_RECV_DONE:
		_plugin_request->set_time_info();
		if (_worker_notifier->Push(_plugin_request) != 0) {
			log4cplus_error("push plugin request failed, fd[%d]",
					netfd);
			return -1;
		}
		_plugin_stage = PLUGIN_PROC;
		break;
	}

	return 0;
}

int PluginSync::proc_multi_request(void)
{
	_plugin_receiver.set_stage(NET_IDLE);
	switch (_plugin_receiver.proc_remain_packet(_plugin_request)) {
	default:
	case NET_FATAL_ERROR:
		return -1;

	case NET_RECVING:
		_plugin_stage = PLUGIN_RECV;
		_plugin_sender.set_stage(NET_IDLE);
		_plugin_receiver.set_stage(NET_RECVING);
		break;

	case NET_RECV_DONE:
		_plugin_request->set_time_info();
		if (_worker_notifier->Push(_plugin_request) != 0) {
			log4cplus_error("push plugin request failed, fd[%d]",
					netfd);
			return -1;
		}
		_plugin_stage = PLUGIN_PROC;
		break;
	}

	return 0;
}

int PluginSync::Response(void)
{
	if (_plugin_request->recv_only()) {
		goto proc_multi;
	}

	switch (_plugin_sender.send(_plugin_request)) {
	default:
	case NET_FATAL_ERROR:
		return -1;

	case NET_SENDING:
		_plugin_stage = PLUGIN_SEND;
		enable_output();
		return 0;

	case NET_SEND_DONE:
		break;
	}

proc_multi:
	//multi request process logic
	if (_plugin_request->multi_packet()) {
		if (proc_multi_request() != 0) {
			return -1;
		}
	} else {
		_plugin_request->release_buffer();
		_plugin_sender.set_stage(NET_IDLE);
		_plugin_receiver.set_stage(NET_IDLE);
		_plugin_stage = PLUGIN_IDLE;
	}

	//Prevent one extra output event trigger
	disable_output();
	enable_input();
	attach_timer(owner->idle_list());

	return 0;
}

void PluginSync::input_notify(void)
{
	log4cplus_debug("enter input_notify!");
	if (_plugin_stage == PLUGIN_IDLE || _plugin_stage == PLUGIN_RECV) {
		if (recv_request() < 0) {
			delete this;
			return;
		}
	} else /* receive input events again. */
	{
		/*  check whether client close connection. */
		if (check_link_status()) {
			log4cplus_debug(
				"client close connection, delete PluginSync obj, plugin stage=%d",
				_plugin_stage);
			delete this;
			return;
		} else {
			disable_input();
		}
	}

	apply_events();

	log4cplus_debug("leave input_notify!");
	return;
}

void PluginSync::output_notify(void)
{
	log4cplus_debug("enter output_notify.");
	if (_plugin_stage == PLUGIN_SEND) {
		if (Response() < 0) {
			delete this;
		}
	} else {
		disable_output();
		log4cplus_info(
			"Spurious PluginSync::output_notify, plugin stage=%d",
			_plugin_stage);
	}

	log4cplus_debug("leave output_notify.");
	return;
}
