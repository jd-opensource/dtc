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
#ifndef __H_DTC_CLIENT_UNIT_H__
#define __H_DTC_CLIENT_UNIT_H__

#include "request/request_base_all.h"
#include "stat_dtc.h"
#include "decode/decoder_base.h"
#include "stop_watch.h"
#include "client_resource_pool.h"

class DTCJobOperation;
class DTCTableDefinition;

class DTCDecoderUnit : public DecoderBase {
    public:
	DTCDecoderUnit(PollerBase *, DTCTableDefinition **, int);
	virtual ~DTCDecoderUnit();

	virtual int process_stream(int fd, int req, void *, int);
	virtual int process_dgram(int fd);

	void register_next_chain(JobAskInterface<DTCJobOperation> *p)
	{
		main_chain.register_next_chain(p);
	}
	ChainJoint<DTCJobOperation> *get_main_chain()
	{
		return &main_chain;
	}
	void task_dispatcher(DTCJobOperation *job)
	{
		main_chain.job_ask_procedure(job);
	}
	DTCTableDefinition *owner_table(void) const
	{
		return table_definition_[0];
	}
	DTCTableDefinition *admin_table(void) const
	{
		return table_definition_[1];
	}

	void record_job_procedure_time(int hit, int type, unsigned int usec);
	// DTCJobOperation *p must nonnull
	void record_job_procedure_time(DTCJobOperation *p);
	void record_rcv_cnt(void);
	void record_snd_cnt(void);
	int regist_resource(ClientResourceSlot **res, unsigned int &id,
			    uint32_t &seq);
	void unregist_resource(unsigned int id, uint32_t seq);
	void clean_resource(unsigned int id);

    private:
	DTCTableDefinition **table_definition_;
	ChainJoint<DTCJobOperation> main_chain;
	ClientResourcePool clientResourcePool;

	StatSample stat_job_procedure_time[8];
};

#endif
