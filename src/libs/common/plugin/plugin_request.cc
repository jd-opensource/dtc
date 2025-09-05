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

#include "plugin_request.h"
#include "plugin_sync.h"
#include "plugin_dgram.h"
#include "../log/log.h"
#include "plugin_mgr.h"

int PluginStream::handle_process(void)
{
	if (0 != _dll->handle_process(_recv_buf, _real_len, &_send_buf,
				      &_send_len, &_skinfo)) {
		mark_handle_fail();
		log4cplus_error("invoke handle_process failed, worker[%d]",
				_gettid_());
	} else {
		mark_handle_succ();
	}

	if (_incoming_notifier->Push(this) != 0) {
		log4cplus_error(
			"push plugin request to incoming failed, worker[%d]",
			_gettid_());
		delete this;
		return -1;
	}

	return 0;
}

int PluginDatagram::handle_process(void)
{
	if (0 != _dll->handle_process(_recv_buf, _real_len, &_send_buf,
				      &_send_len, &_skinfo)) {
		mark_handle_fail();
		log4cplus_error("invoke handle_process failed, worker[%d]",
				_gettid_());
	} else {
		mark_handle_succ();
	}

	if (_incoming_notifier->Push(this) != 0) {
		log4cplus_error(
			"push plugin request to incoming failed, worker[%d]",
			_gettid_());
		delete this;
		return -1;
	}

	return 0;
}

int PluginStream::job_ask_procedure(void)
{
	if (disconnect()) {
		DELETE(_plugin_sync);
		delete this;
		return -1;
	}

	if (!handle_succ()) {
		DELETE(_plugin_sync);
		delete this;
		return -1;
	}

	if (NULL == _plugin_sync) {
		delete this;
		return -1;
	}

	_plugin_sync->set_stage(PLUGIN_SEND);
	_plugin_sync->send_response();

	return 0;
}

int PluginDatagram::job_ask_procedure(void)
{
	if (!handle_succ()) {
		delete this;
		return -1;
	}

	if (NULL == _plugin_dgram) {
		delete this;
		return -1;
	}

	_plugin_dgram->send_response(this);

	return 0;
}
