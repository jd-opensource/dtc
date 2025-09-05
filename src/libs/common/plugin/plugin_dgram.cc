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
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>

#include "plugin_agent_mgr.h"
#include "plugin_dgram.h"
#include "plugin_unit.h"
#include "../poll/poller_base.h"
#include "mem_check.h"
#include "../log/log.h"

extern "C" {
extern unsigned int get_local_ip();
}

static int GetSocketFamily(int fd)
{
	struct sockaddr addr;
	bzero(&addr, sizeof(addr));
	socklen_t alen = sizeof(addr);
	getsockname(fd, &addr, &alen);
	return addr.sa_family;
}

PluginDgram::PluginDgram(PluginDecoderUnit *plugin_decoder, int fd)
	: EpollBase(plugin_decoder->owner_thread(), fd), mtu(0), _addr_len(0),
	  _owner(plugin_decoder), _worker_notifier(NULL),
	  _plugin_receiver(fd, PluginAgentManager::instance()->get_dll()),
	  _plugin_sender(fd, PluginAgentManager::instance()->get_dll()),
	  _local_ip(0)
{
}

PluginDgram::~PluginDgram()
{
}

int PluginDgram::do_attach()
{
	/* init local ip */
	_local_ip = get_local_ip();

	switch (GetSocketFamily(netfd)) {
	default:
	case AF_UNIX:
		mtu = 16 << 20;
		_addr_len = sizeof(struct sockaddr_un);
		break;
	case AF_INET:
		mtu = 65535;
		_addr_len = sizeof(struct sockaddr_in);
		break;
	case AF_INET6:
		mtu = 65535;
		_addr_len = sizeof(struct sockaddr_in6);
		break;
	}

	//get worker notifier
	_worker_notifier =
		PluginAgentManager::instance()->get_worker_notifier();
	if (NULL == _worker_notifier) {
		log4cplus_error("worker notifier is invalid.");
		return -1;
	}

	enable_input();

	return attach_poller();
}

//server peer
int PluginDgram::recv_request(void)
{
	//create dgram request
	PluginDatagram *dgram_request = NULL;
	NEW(PluginDatagram(this, PluginAgentManager::instance()->get_dll()),
	    dgram_request);
	if (NULL == dgram_request) {
		log4cplus_error("create PluginRequest for dgram failed, msg:%s",
				strerror(errno));
		return -1;
	}

	//set request info
	dgram_request->_skinfo.sockfd = netfd;
	dgram_request->_skinfo.type = SOCK_DGRAM;
	dgram_request->_skinfo.local_ip = _local_ip;
	dgram_request->_skinfo.local_port = 0;
	dgram_request->_incoming_notifier = _owner->get_incoming_notifier();
	dgram_request->_addr_len = _addr_len;
	dgram_request->_addr = MALLOC(_addr_len);
	if (NULL == dgram_request->_addr) {
		log4cplus_error("malloc failed, msg:%m");
		DELETE(dgram_request);
		return -1;
	}

	if (_plugin_receiver.recvfrom(dgram_request, mtu) != 0) {
		DELETE(dgram_request);
		return -1;
	}

	dgram_request->set_time_info();
	if (_worker_notifier->Push(dgram_request) != 0) {
		log4cplus_error("push plugin request failed, fd[%d]", netfd);
		DELETE(dgram_request);
		return -1;
	}

	return 0;
}

void PluginDgram::input_notify(void)
{
	if (recv_request() < 0)
		/* */;
}
