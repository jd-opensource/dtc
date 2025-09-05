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
#ifndef __PLUGIN_WORKER_THREAD_H__
#define __PLUGIN_WORKER_THREAD_H__

#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <linux/unistd.h>

#include "algorithm/timestamp.h"
#include "../thread/thread.h"
#include "plugin_request.h"

class PluginWorkerThread : public Thread {
    public:
	PluginWorkerThread(const char *name, worker_notify_t *worker_notify,
			   int seq_no);
	virtual ~PluginWorkerThread();

	inline int get_seq_no(void)
	{
		return _seq_no;
	}

    protected:
	virtual void *do_process(void);
	virtual int initialize(void);
	virtual void Prepare(void);

    private:
	worker_notify_t *_worker_notify;
	dll_func_t *_dll;
	int _seq_no;
};

#endif
