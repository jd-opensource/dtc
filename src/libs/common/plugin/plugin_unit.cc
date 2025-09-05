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
#include <errno.h>
#include <unistd.h>

#include "plugin_unit.h"
#include "plugin_sync.h"
#include "plugin_dgram.h"
#include "../log/log.h"
#include "mem_check.h"

PluginDecoderUnit::PluginDecoderUnit(PollerBase *o, int it) : DecoderBase(o, it)
{
}

PluginDecoderUnit::~PluginDecoderUnit()
{
}

int PluginDecoderUnit::process_stream(int newfd, int req, void *peer,
				      int peerSize)
{
	PluginSync *plugin_client = NULL;
	NEW(PluginSync(this, newfd, peer, peerSize), plugin_client);

	if (0 == plugin_client) {
		log4cplus_error(
			"create PluginSync object failed, errno[%d], msg[%m]",
			errno);
		return -1;
	}

	if (plugin_client->do_attach() == -1) {
		log4cplus_error("Invoke PluginSync::do_attach() failed");
		delete plugin_client;
		return -1;
	}

	/* recv immediately after accept wakeup */
	plugin_client->input_notify();

	return 0;
}

int PluginDecoderUnit::process_dgram(int newfd)
{
	PluginDgram *plugin_dgram = NULL;
	NEW(PluginDgram(this, newfd), plugin_dgram);

	if (0 == plugin_dgram) {
		log4cplus_error(
			"create PluginDgram object failed, errno[%d], msg[%m]",
			errno);
		return -1;
	}

	if (plugin_dgram->do_attach() == -1) {
		log4cplus_error("Invoke PluginDgram::do_attach() failed");
		delete plugin_dgram;
		return -1;
	}

	return 0;
}
