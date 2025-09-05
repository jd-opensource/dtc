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

#include "client/client_unit.h"
#include "client_sync.h"
#include "client_async.h"
#include "client_dgram.h"
#include "poll/poller_base.h"
#include "task/task_request.h"
#include "log/log.h"

DTCDecoderUnit::DTCDecoderUnit(PollerBase *o, DTCTableDefinition **tdef, int it)
	: DecoderBase(o, it), table_definition_(tdef), main_chain(o)
{
	stat_job_procedure_time[0] = g_stat_mgr.get_sample(REQ_USEC_ALL);
	stat_job_procedure_time[1] = g_stat_mgr.get_sample(REQ_USEC_GET);
	stat_job_procedure_time[2] = g_stat_mgr.get_sample(REQ_USEC_INS);
	stat_job_procedure_time[3] = g_stat_mgr.get_sample(REQ_USEC_UPD);
	stat_job_procedure_time[4] = g_stat_mgr.get_sample(REQ_USEC_DEL);
	stat_job_procedure_time[5] = g_stat_mgr.get_sample(REQ_USEC_FLUSH);
	stat_job_procedure_time[6] = g_stat_mgr.get_sample(REQ_USEC_HIT);
	stat_job_procedure_time[7] = g_stat_mgr.get_sample(REQ_USEC_REPLACE);

	if (clientResourcePool.do_init() < 0)
		throw(int) - ENOMEM;
}

DTCDecoderUnit::~DTCDecoderUnit()
{
}

void DTCDecoderUnit::record_job_procedure_time(int hit, int type,
					       unsigned int usec)
{
	static const unsigned char cmd2type[] = {
		/*TYPE_PASS*/ 0,
		/*result_code*/ 0,
		/*DTCResultSet*/ 0,
		/*HelperAdmin*/ 0,
		/*Get*/ 1,
		/*Purge*/ 5,
		/*Insert*/ 2,
		/*Update*/ 3,
		/*Delete*/ 4,
		/*Other*/ 0,
		/*Other*/ 0,
		/*Other*/ 0,
		/*Replace*/ 7,
		/*Flush*/ 5,
		/*Other*/ 0,
		/*Other*/ 0,
	};
	stat_job_procedure_time[0].push(usec);
	unsigned int t = hit ? 6 : cmd2type[type];
	if (t)
		stat_job_procedure_time[t].push(usec);
}

void DTCDecoderUnit::record_job_procedure_time(DTCJobOperation *req)
{
	record_job_procedure_time(req->flag_is_hit(), req->request_code(),
				  req->responseTimer.live());
}

int DTCDecoderUnit::process_stream(int newfd, int req, void *peer, int peerSize)
{
	if (req <= 1) {
		ClientSync *cli = NULL;
		try {
			cli = new ClientSync(this, newfd, peer, peerSize);
		} catch (int err) {
			DELETE(cli);
			return -1;
		}

		if (0 == cli) {
			log4cplus_error(
				"create CClient object failed, errno[%d], msg[%m]",
				errno);
			return -1;
		}

		if (cli->do_attach() == -1) {
			log4cplus_error("Invoke CClient::do_attach() failed");
			delete cli;
			return -1;
		}

		/* Immediately recv after accept wakeup */
		cli->input_notify();
	} else {
		ClientAsync *cli =
			new ClientAsync(this, newfd, req, peer, peerSize);

		if (0 == cli) {
			log4cplus_error(
				"create CClient object failed, errno[%d], msg[%m]",
				errno);
			return -1;
		}

		if (cli->do_attach() == -1) {
			log4cplus_error("Invoke CClient::do_attach() failed");
			delete cli;
			return -1;
		}

		/* Immediately recv after accept wakeup */
		cli->input_notify();
	}
	return 0;
}

int DTCDecoderUnit::process_dgram(int newfd)
{
	ClientDgram *cli = new ClientDgram(this, newfd);

	if (0 == cli) {
		log4cplus_error(
			"create CClient object failed, errno[%d], msg[%m]",
			errno);
		return -1;
	}

	if (cli->do_attach() == -1) {
		log4cplus_error("Invoke CClient::do_attach() failed");
		delete cli;
		return -1;
	}
	return 0;
}

int DTCDecoderUnit::regist_resource(ClientResourceSlot **res, unsigned int &id,
				    uint32_t &seq)
{
	if (clientResourcePool.Alloc(id, seq) < 0) {
		id = 0;
		*res = NULL;
		return -1;
	}

	*res = clientResourcePool.Slot(id);
	return 0;
}

void DTCDecoderUnit::unregist_resource(unsigned int id, uint32_t seq)
{
	clientResourcePool.Free(id, seq);
}

void DTCDecoderUnit::clean_resource(unsigned int id)
{
	clientResourcePool.Clean(id);
}
