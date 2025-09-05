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
#include "connector/connector_group.h"
#include "stat_dtc_def.h"

#include "remote_dtc_ask_answer_chain.h"

RemoteDtcAskAnswerChain::RemoteDtcAskAnswerChain(PollerBase *owner,
						 int clientPerGroup)
	: JobAskInterface<DTCJobOperation>(owner), m_recv_timer_list(0),
	  m_conn_timer_list(0), m_retry_timer_list(0),
	  m_client_per_group(clientPerGroup)
{
}

RemoteDtcAskAnswerChain::~RemoteDtcAskAnswerChain()
{
	for (HelperMapType::iterator iter = m_groups.begin();
	     iter != m_groups.end(); ++iter) {
		delete iter->second.group;
	}

	m_groups.clear();
}

void RemoteDtcAskAnswerChain::job_ask_procedure(DTCJobOperation *t)
{
	log4cplus_debug("enter job_ask_procedure");
	log4cplus_debug("t->remote_addr: %s", t->remote_addr());
	HelperMapType::iterator iter = m_groups.find(t->remote_addr());
	if (iter == m_groups.end()) {
		ConnectorGroup *g = new ConnectorGroup(
			t->remote_addr(), /* sock path */
			t->remote_addr(), /* name */
			m_client_per_group, /* helper client count */
			m_client_per_group * 32 /* queue size */,
			DTC_FORWARD_USEC_ALL);
		g->set_timer_handler(m_recv_timer_list, m_conn_timer_list,
				     m_retry_timer_list);
		g->do_attach(owner, &m_task_queue_allocator);
		helper_group hg = { g, 0 };
		m_groups[t->remote_addr()] = hg;
		iter = m_groups.find(t->remote_addr());
	}
	t->push_reply_dispatcher(this);
	iter->second.group->job_ask_procedure(t);
	iter->second.used = 1;
	log4cplus_debug("leave job_ask_procedure");
}

void RemoteDtcAskAnswerChain::job_answer_procedure(DTCJobOperation *t)
{
	if (t->result_code() == 0) {
		log4cplus_debug(
			"reply from remote dtc success,append result start ");

		if (t->result) {
			t->prepare_result();
			int iRet = t->pass_all_result(t->result);
			if (iRet < 0) {
				log4cplus_info("job append_result error: %d",
					       iRet);
				t->set_error(iRet, "RemoteDtcAskAnswerChain",
					     "append_result() error");
				t->turn_around_job_answer();
				return;
			}
		}
		t->turn_around_job_answer();
		return;
	} else {
		log4cplus_debug("reply from remote dtc error:%d",
				t->result_code());
		t->turn_around_job_answer();
		return;
	}
}

void RemoteDtcAskAnswerChain::set_timer_handler(TimerList *recv,
						TimerList *conn,
						TimerList *retry)
{
	m_recv_timer_list = recv;
	m_conn_timer_list = conn;
	m_retry_timer_list = retry;

	attach_timer(m_retry_timer_list);
}

void RemoteDtcAskAnswerChain::job_timer_procedure()
{
	log4cplus_debug("enter timer procedure");
	for (HelperMapType::iterator i = m_groups.begin();
	     i != m_groups.end();) {
		if (i->second.used == 0) {
			delete i->second.group;
			m_groups.erase(i++);
		} else {
			i->second.used = 0;
			++i;
		}
	}
	log4cplus_debug("leave timer procedure");
}
