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
#ifndef __DTC_PLUGIN_GLOBAL_H__
#define __DTC_PLUGIN_GLOBAL_H__

#include <stdlib.h>
#include <stdint.h>

#include "algorithm/non_copyable.h"

#define PLUGIN_STOP_REQUEST (plugin_request_t *)0xFFFFFFFF
#define SERVER_BENCH_SO_FILE "../lib/sb_interface.so"
#define DTC_API_SO_FILE "../bin/libdtc.so"

#define DLFUNC_NO_ERROR(h, v, type, name)                                      \
	do {                                                                   \
		v = (type)dlsym(h, name);                                      \
		dlerror();                                                     \
	} while (0)

#define DLFUNC(h, v, type, name)                                               \
	do {                                                                   \
		v = (type)dlsym(h, name);                                      \
		if ((error = dlerror()) != NULL) {                             \
			log4cplus_error("dlsym error, %s", error);             \
			dlclose(h);                                            \
			h = NULL;                                              \
			goto out;                                              \
		}                                                              \
	} while (0)

typedef enum plugin_thread_type {
	PROC_MAIN = 0,
	PROC_CONN,
	PROC_WORK
} plugin_thread_type_t;

typedef enum plugin_client_type {
	PLUGIN_STREAM,
	PLUGIN_DGRAM,
	PLUGIN_UNKNOW
} plugin_client_type_t;

enum { PLUGIN_RECV_ONLY = 1,
       PLUGIN_DISCONNECT = 2,
};

typedef struct skinfo_struct {
	int sockfd; //fd
	int type; //type
	int64_t recvtm; //receive time
	int64_t sendtm; //send time

	time_t tasktm; //task start time

	u_int local_ip; //local ip
	u_short local_port; //local port
	u_int remote_ip; //remote ip
	u_short remote_port; //remote port
	uint64_t flags; //reserved flag bits, bit operations, 0x01:no response packet, 0x00:need response packet
} skinfo_t;

typedef int (*handle_init_t)(int, char **, int);
typedef int (*handle_input_t)(const char *, int, const skinfo_t *);
typedef int (*handle_process_t)(char *, int, char **, int *, const skinfo_t *);
typedef int (*handle_open_t)(char **, int *, const skinfo_t *);
typedef int (*handle_close_t)(const skinfo_t *);
typedef void (*handle_fini_t)(int);
typedef int (*handle_timer_notify_t)(int, void **);
typedef int (*plugin_log_init_t)(const char *, int, u_int, const char *);

typedef struct dll_func_struct {
	void *handle;
	handle_init_t handle_init;
	handle_input_t handle_input;
	handle_process_t handle_process;
	handle_open_t handle_open;
	handle_close_t handle_close;
	handle_fini_t handle_fini;
	handle_timer_notify_t handle_timer_notify;
} dll_func_t;

class PluginGlobal : private noncopyable {
    public:
	PluginGlobal(void);
	~PluginGlobal(void);

	void dup_argv(int argc, char **argv);
	void free_argv(void);

    public:
	static const char *_internal_name[2];
	static const int _max_worker_number;
	static const int _default_worker_number;
	static const int _max_plugin_recv_len;
	static int saved_argc;
	static char **saved_argv;
	static const char *_plugin_conf_file;
	// for agent_plugin
	static int _idle_timeout;
	static int _single_incoming_thread;
	static int _max_listen_count;
	static int _max_request_window;
	static char *_bind_address;
	static int _udp_recv_buffer_size;
	static int _udp_send_buffer_size;

    protected:
    protected:
    private:
    private:
};

#endif
