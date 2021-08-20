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
#include "rocksdb_direct_process.h"
#include "rocksdb_direct_listener.h"
#include "poll/poller_base.h"
#include "log/log.h"

#include <assert.h>

RocksdbDirectProcess::RocksdbDirectProcess(const std::string &name,
					   HelperProcessBase *processor)
	: m_domain_socket_path(name), m_rocksdb_process(processor),
	  m_rocks_direct_poll(new PollerBase("RocksdbDirectAccessPoll")),
	  m_listener(NULL)
{
}

int RocksdbDirectProcess::init()
{
	assert(m_rocks_direct_poll);

	int ret = m_rocks_direct_poll->initialize_thread();
	if (ret < 0) {
		log4cplus_error("initialize thread poll failed.");
		return -1;
	}

	// add listener to poll
	ret = add_listener_to_poll();
	if (ret != 0)
		return -1;

	// add worker to poll
	// ret = addRocksdbWorkToPoll();
	// if ( ret != 0 ) return -1;

	return 0;
}

int RocksdbDirectProcess::add_listener_to_poll()
{
	m_listener = new RocksdbDirectListener(
		m_domain_socket_path, m_rocksdb_process, m_rocks_direct_poll);
	if (!m_listener) {
		log4cplus_error("create listener instance failed");
		return -1;
	}

	int ret = m_listener->do_bind();
	if (ret < 0) {
		log4cplus_error("bind address failed.");
		return -1;
	}

	ret = m_listener->attach_thread();
	if (ret < 0) {
		log4cplus_error("add listener to poll failed.");
		return -1;
	}

	return 0;
}

/*
int RocksdbDirectProcess::addRocksdbWorkToPoll()
{
  mDirectWorker = new RocksdbDirectWorker(m_rocks_direct_poll);
  if ( !mDirectWorker )
  {
    log4cplus_error("create rocksdb direct worker failed.");
    return -1;
  }
  
  int ret = mDirectWorker->attach_thread();
  if ( ret < 0 )
  {
    log4cplus_error("add rocksdb direct worker to poll failed.");
    return -1;
  }
  
  return true;
}*/

int RocksdbDirectProcess::run_process()
{
	m_rocks_direct_poll->running_thread();
	log4cplus_error("start rocksdb direct process!");
	return 0;
}
