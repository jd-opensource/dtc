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
#include <stdlib.h>
#include <string.h>

#include "mem_check.h"
#include "plugin_global.h"

const char *PluginGlobal::_internal_name[2] = { (const char *)"dtc_internal",
						NULL };
const int PluginGlobal::_max_worker_number = 1024;
const int PluginGlobal::_default_worker_number = 1;
const int PluginGlobal::_max_plugin_recv_len = 512;
int PluginGlobal::saved_argc = 0;
char **PluginGlobal::saved_argv = NULL;
const char *PluginGlobal::_plugin_conf_file = NULL;
int PluginGlobal::_idle_timeout = 10;
int PluginGlobal::_single_incoming_thread = 0;
int PluginGlobal::_max_listen_count = 256;
int PluginGlobal::_max_request_window = 0;
char *PluginGlobal::_bind_address = NULL;
int PluginGlobal::_udp_recv_buffer_size = 0;
int PluginGlobal::_udp_send_buffer_size = 0;

void PluginGlobal::dup_argv(int argc, char **argv)
{
	saved_argc = argc;
	saved_argv = (char **)MALLOC(sizeof(char *) * (argc + 1));

	if (!saved_argv)
		return;

	saved_argv[argc] = NULL;

	while (--argc >= 0) {
		saved_argv[argc] = STRDUP(argv[argc]);
	}
}

void PluginGlobal::free_argv(void)
{
	char **argv;

	for (argv = saved_argv; *argv; argv++)
		free(*argv);

	FREE_IF(saved_argv);
	saved_argv = NULL;
}
