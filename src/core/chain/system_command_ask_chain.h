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

#ifndef __SYSTEM_COMMAND_ASK_CHAIN__
#define __SYSTEM_COMMAND_ASK_CHAIN__

#include <task/task_request.h>
#include <stat_dtc.h>

class SystemCommandAskChain : public JobAskInterface<DTCJobOperation> {
    protected:
	static SystemCommandAskChain *system_command_instance;
	SystemCommandAskChain(PollerBase *o);

    public:
	//Return instance, if instance not yet constructed, construct a new instance and return
	static SystemCommandAskChain *get_instance(PollerBase *o);
	//Just return, if instance not yet constructed, return null
	static SystemCommandAskChain *get_instance();
	virtual ~SystemCommandAskChain(void);
	void register_next_chain(JobAskInterface<DTCJobOperation> *p)
	{
		main_chain.register_next_chain(p);
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}
	bool is_read_only();

    private:
	ChainJoint<DTCJobOperation> main_chain;
	//Whether server is in read-only state
	atomic8_t read_only_;
	//Read-only statistics object
	StatCounter stat_read_only;

    private:
	virtual void job_ask_procedure(DTCJobOperation *);

	//Handle serveradmin commands
	void deal_server_admin(DTCJobOperation *job_operation);
	void query_mem_info(DTCJobOperation *job_operation);
};

#endif
