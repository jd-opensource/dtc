/*
* Copyright [2021] JD.com, Inc.
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
* 
*/

#include "rocksdb_direct_worker.h"
#include "db_process_base.h"
#include "poll/poller_base.h"
#include "log/log.h"

#include <algorithm>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

RocksdbDirectWorker::RocksdbDirectWorker(HelperProcessBase *processor,
					 PollerBase *poll, int fd)
	: EpollBase(poll, fd), m_db_process_rocks(processor)
{
}

RocksdbDirectWorker::~RocksdbDirectWorker()
{
}

int RocksdbDirectWorker::add_event_to_poll()
{
	enable_input();
	int ret = EpollBase::attach_poller();
	if (ret < 0) {
		log4cplus_error("add event to poll failed.");
		return -1;
	}

	log4cplus_info("add rocksdb worker to event poll successful, fd:%d",
		       netfd);

	return 0;
}

int RocksdbDirectWorker::remove_from_event_poll()
{
	EpollBase::detach_poller();

	log4cplus_error(
		"delete rocksdb direct worker from poll successful, fd:%d",
		netfd);

	delete this;

	return 0;
}

// because not thinking about the split or delay of data package, so if
// recieving unexpected message, just close the socket. the client will
// reconnect during the next invoking
void RocksdbDirectWorker::input_notify()
{
	int ret = receive_request();
	if (ret != 0) {
		remove_from_event_poll();
		return;
	}

	proc_direct_request();

	send_response();

	return;
}

void RocksdbDirectWorker::proc_direct_request()
{
	m_response_context.free();

	m_db_process_rocks->process_direct_query(&m_request_context,
						 &m_response_context);
	return;
}

int RocksdbDirectWorker::receive_request()
{
	static const int max_context_len = 16 << 10; // 16k
	static char data_buffer[max_context_len];

	m_request_context.reset();

	int data_len;
	int ret = receive_message((char *)&data_len, sizeof(data_len));
	if (ret != 0)
		return -1;
	assert(data_len <= max_context_len);

	ret = receive_message(data_buffer, data_len);
	if (ret != 0)
		return -1;

	m_request_context.serialize_from(data_buffer, data_len);

	// priority of query conditions
	std::sort(
		m_request_context.s_field_conds.begin(),
		m_request_context.s_field_conds.end(),
		[this](const QueryCond &cond1, const QueryCond &cond2) -> bool {
			return cond1.s_field_index < cond2.s_field_index ||
			       (cond1.s_field_index == cond2.s_field_index &&
				condtion_priority(
					(ConditionOperation)cond1.s_cond_opr,
					(ConditionOperation)cond2.s_cond_opr));
		});
	assert(m_request_context.s_field_conds[0].s_field_index == 0);

	return 0;
}

void RocksdbDirectWorker::send_response()
{
	m_response_context.s_magic_num = m_request_context.s_magic_num;
	m_response_context.s_sequence_id = m_request_context.s_sequence_id;

	std::string rowValue;
	m_response_context.serialize_to(rowValue);

	int valueLen = rowValue.length();
	int ret = send_message((char *)&valueLen, sizeof(int));
	if (ret < 0) {
		log4cplus_error(
			"send response failed, close the connection. netfd:%d, sequenceId:%" PRIu64,
			netfd, m_response_context.s_sequence_id);
		remove_from_event_poll();
		return;
	}

	// send row value
	ret = send_message(rowValue.c_str(), valueLen);
	if (ret < 0) {
		log4cplus_error(
			"send response failed, close the connection. netfd:%d, sequenceId:%" PRIu64,
			netfd, m_response_context.s_sequence_id);
		remove_from_event_poll();
		return;
	}

	log4cplus_info(
		"send response successful. netfd:%d, sequenceId:%" PRIu64,
		netfd, m_response_context.s_sequence_id);

	return;
}

int RocksdbDirectWorker::receive_message(char *data, int data_len)
{
	int readNum = 0;
	int nRead = 0;
	int ret = 0;
	do {
		ret = read(netfd, data + nRead, data_len - nRead);
		if (ret > 0) {
			nRead += ret;
		} else if (ret == 0) {
			// close the connection
			log4cplus_error("peer close the socket, fd:%d", netfd);
			return -1;
		} else {
			if (readNum < 1000 &&
			    (errno == EAGAIN || errno == EINTR)) {
				readNum++;
				continue;
			} else {
				// close the connection
				log4cplus_error(
					"read socket failed, fd:%d, errno:%d",
					netfd, errno);
				return -1;
			}
		}
	} while (nRead < data_len);

	return 0;
}

int RocksdbDirectWorker::send_message(const char *data, int data_len)
{
	int send_num = 0;
	int n_write = 0;
	int ret = 0;
	do {
		ret = write(netfd, data + n_write, data_len - n_write);
		if (ret > 0) {
			n_write += ret;
		} else if (ret < 0) {
			if (send_num < 1000 &&
			    (errno == EINTR || errno == EAGAIN)) {
				send_num++;
				continue;
			} else {
				// connection has issue, need to close the socket
				log4cplus_error(
					"write socket failed, fd:%d, errno:%d",
					netfd, errno);
				return -1;
			}
		} else {
			if (data_len == 0)
				return 0;

			log4cplus_error(
				"unexpected error!!!!!!!, fd:%d, errno:%d",
				netfd, errno);
			return -1;
		}
	} while (n_write < data_len);

	return 0;
}

// judge the query condition priority those with the same field index
// (==) > (>, >=) > (<, <=) > (!=)
bool RocksdbDirectWorker::condtion_priority(const ConditionOperation lc,
					    const ConditionOperation rc)
{
	switch (lc) {
	case ConditionOperation::EQ:
		return true;
	case ConditionOperation::NE:
		return false;
	case ConditionOperation::LT:
	case ConditionOperation::LE:
		if (rc == ConditionOperation::NE)
			return true;
		return false;
	case ConditionOperation::GT:
	case ConditionOperation::GE:
		if (rc == ConditionOperation::EQ)
			return false;
		return true;
	default:
		log4cplus_error("unkonwn condtion opr:%d", (int)lc);
	}

	return false;
}
