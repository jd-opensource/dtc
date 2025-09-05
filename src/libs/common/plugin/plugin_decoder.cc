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

#include "plugin_decoder.h"
#include "mem_check.h"
#include "plugin_request.h"
#include "../log/log.h"
#include "plugin_mgr.h"

int PluginReceiver::parse_protocol(PluginStream *request)
{
	log4cplus_debug("enter PluginReceiver::parse_protocol!!!!!!!!");
	const int max_recv_len = PluginGlobal::_max_plugin_recv_len;
	int &remain_len = request->_recv_remain_len;
	char *&recv_buf = request->_recv_buf;
	int &recv_len = request->_recv_len;
	int &real_len = request->_real_len;
	int &all_len = request->_all_len;

	//get real length
	if (0 == real_len) {
		real_len = _dll->handle_input(recv_buf, recv_len,
					      &(request->_skinfo));
	}

	if (real_len < 0) {
		_stage = NET_FATAL_ERROR;
		log4cplus_error(
			"invoke handle_input failed, fd[%d], length[%d], net stage[%d]",
			_fd, recv_len, _stage);
		return _stage;
	}

	if (0 == real_len) {
		if (recv_len < max_recv_len) {
			remain_len = max_recv_len - recv_len;
		} else {
			all_len = max_recv_len + recv_len;
			_all_len_changed = 1;
		}

		_stage = NET_RECVING;
		goto parse_out;
	}

	if (real_len > recv_len) {
		remain_len = real_len - recv_len;

		if (all_len < real_len) {
			all_len = real_len;
			_all_len_changed = 1;
		}

		_stage = NET_RECVING;
		goto parse_out;
	}

	if (real_len < recv_len) {
		request->mark_multi_packet();
	} else {
		request->mark_single_packet();
	}

	if (all_len < recv_len) {
		all_len = recv_len;
		_all_len_changed = 1;
	}

	remain_len = 0;
	_stage = NET_RECV_DONE;

parse_out:
	if (_all_len_changed) {
		_all_len_changed = 0;
		if (REALLOC(recv_buf, all_len) == NULL) {
			_stage = NET_FATAL_ERROR;
			log4cplus_error(
				"recv error, fd[%d], length[%d], net stage[%d], msg:%s",
				_fd, recv_len, _stage, strerror(errno));
		}
	}

	return _stage;
}

int PluginReceiver::recv(PluginStream *request)
{
	const int max_recv_len = PluginGlobal::_max_plugin_recv_len;
	int &remain_len = request->_recv_remain_len;
	char *&recv_buf = request->_recv_buf;
	int &recv_len = request->_recv_len;
	int &all_len = request->_all_len;
	int curr_len = 0;

	//init changed flag
	_all_len_changed = 0;

	if ((NET_IDLE == _stage) && (NULL == recv_buf)) {
		recv_buf = (char *)MALLOC(max_recv_len);
		if (NULL == recv_buf) {
			_stage = NET_FATAL_ERROR;
			log4cplus_error(
				"malloc memory failed, size[%d], msg:%s",
				max_recv_len, strerror(errno));
			return _stage;
		}

		all_len = max_recv_len;
		remain_len = max_recv_len;
		_all_len_changed = 1;
	}

	curr_len = ::read(_fd, recv_buf + recv_len, remain_len);
	switch (curr_len) {
	case -1:
		//read error
		if (errno != EAGAIN && errno != EINTR && errno != EINPROGRESS) {
			_stage = NET_FATAL_ERROR;
			log4cplus_error(
				"recv failed from fd[%d], net stage[%d], msg[%s]",
				_fd, _stage, strerror(errno));
			return _stage;
		}
		return _stage;

	case 0:
		//disconnect by user
		_stage = NET_DISCONNECT;
		return NET_DISCONNECT;

	default:
		recv_len += curr_len;
		parse_protocol(request);
		break;
	}

	return _stage;
}

int PluginReceiver::recvfrom(PluginDatagram *request, int mtu)
{
	char *&recv_buf = request->_recv_buf;
	int &recv_len = request->_recv_len;
	int &real_len = request->_real_len;

	int max_len = -1;
	ioctl(_fd, SIOCINQ, &max_len);
	if (max_len < 0) {
		log4cplus_error("%s", "next packet size unknown");
		return -1;
	}

	/* treat 0 as max packet, because we can't ditiguish the no-packet & zero-packet */
	if (0 == max_len) {
		max_len = mtu;
	}

	recv_buf = (char *)MALLOC(max_len);
	if (NULL == recv_buf) {
		log4cplus_error(
			"allocate packet buffer[%d] failed, msg[no enough memory]",
			recv_len);
		return -1;
	}

	void *&req_addr = request->_addr;
	socklen_t &req_addr_len = request->_addr_len;
	recv_len = ::recvfrom(_fd, recv_buf, max_len, MSG_DONTWAIT | MSG_TRUNC,
			      (struct sockaddr *)req_addr, &req_addr_len);

	if (recv_len <= 0) {
		if (errno != EAGAIN) {
			log4cplus_info("recvfrom error: size=%d errno=%d",
				       recv_len, errno);
		}

		return -1;
	}

	request->_skinfo.remote_ip =
		((struct sockaddr_in *)req_addr)->sin_addr.s_addr;
	request->_skinfo.remote_port =
		((struct sockaddr_in *)req_addr)->sin_port;

	real_len = _dll->handle_input(recv_buf, recv_len, &(request->_skinfo));

	if ((real_len <= 0) || (real_len > recv_len)) {
		log4cplus_info("the recv buffer is invalid.");
		return -1;
	}

	return 0;
}

int PluginSender::send(PluginStream *request)
{
	const int send_len = request->_send_len;
	int &sent_len = request->_sent_len;
	const char *send_buf = request->_send_buf;

	if (0 == send_len || NULL == send_buf) {
		_stage = NET_FATAL_ERROR;
		log4cplus_error(
			"send length[%d] and send buffer[%p] is invalid.",
			send_len, send_buf);
		return _stage;
	}

	int ret = ::write(_fd, send_buf + sent_len, send_len - sent_len);
	if (ret < 0) {
		if (errno == EAGAIN || errno == EINTR || errno == EINPROGRESS) {
			_stage = NET_SENDING;
			log4cplus_debug(
				"*STEP: send data pending, _fd[%d], net stage[%d], sendlen[%d], sentlen[%d]",
				_fd, _stage, send_len, sent_len);

			return _stage;
		}

		_stage = NET_FATAL_ERROR;
		log4cplus_error(
			"*STEP: send data error, _fd[%d], net stage[%d], msg[%m]",
			_fd, _stage);
		return _stage;
	}

	if (sent_len + ret == send_len) {
		sent_len += ret;
		_stage = NET_SEND_DONE;
		log4cplus_debug(
			"*STEP: send data done, fd[%d], net stage[%d], sent len[%d], send len[%d]",
			_fd, _stage, sent_len, send_len);
		return _stage;
	}

	if (sent_len + ret < send_len) {
		sent_len += ret;
		_stage = NET_SENDING;
		log4cplus_debug(
			"*STEP: sending data, fd[%d], net stage[%d], sent len[%d], send len[%d]",
			_fd, _stage, sent_len, send_len);
		return _stage;
	}

	_stage = NET_FATAL_ERROR;
	log4cplus_error(
		"*STEP: send data exception, fd[%d], net stage[%d], sent len[%d], send len[%d]",
		_fd, _stage, sent_len + ret, send_len);

	return _stage;
}

int PluginSender::sendto(PluginDatagram *request)
{
	char *&send_buf = request->_send_buf;
	int send_len = request->_send_len;
	int &sent_len = request->_sent_len;
	struct iovec iov;
	struct msghdr msgh;

	/* init send buffer & send length*/
	iov.iov_base = send_buf;
	iov.iov_len = send_len;

	/* init msghdr */
	msgh.msg_name = (struct sockaddr_in *)(request->_addr);
	msgh.msg_namelen = request->_addr_len;
	msgh.msg_iov = &iov;
	msgh.msg_iovlen = 1;
	msgh.msg_control = NULL;
	msgh.msg_controllen = 0;
	msgh.msg_flags = 0;

	sent_len = sendmsg(_fd, &msgh, MSG_DONTWAIT | MSG_NOSIGNAL);

	if ((sent_len <= 0) || (sent_len < send_len)) {
		log4cplus_error(
			"sendto data to client failed, sent length:%d, send length:%d, msg:%s",
			sent_len, send_len, strerror(errno));
		return -1;
	}

	return 0;
}
