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
#include "rocksdb_direct_listener.h"
#include "rocksdb_direct_worker.h"
#include "poll/poller_base.h"
#include "log/log.h"

#include <errno.h>
#include <sstream>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

RocksdbDirectListener::RocksdbDirectListener(const std::string &name,
					     HelperProcessBase *processor,
					     PollerBase *poll)
	: EpollBase(poll, 0), m_domain_socket_path(name),
	  m_rocksdb_process(processor), m_poller_thread(poll)
{
	// init();
}

RocksdbDirectListener::~RocksdbDirectListener()
{
	unlink(m_domain_socket_path.c_str());
}

int RocksdbDirectListener::do_bind()
{
	struct sockaddr_un local_addr;
	if (m_domain_socket_path.length() >= (int)sizeof(local_addr.sun_path)) {
		log4cplus_error("unix socket path is too long! path:%s",
				m_domain_socket_path.c_str());
		return -1;
	}

	memset((void *)&local_addr, 0, sizeof(local_addr));
	local_addr.sun_family = AF_UNIX;
	strncpy(local_addr.sun_path, m_domain_socket_path.c_str(),
		m_domain_socket_path.length());

	netfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (netfd <= 0) {
		log4cplus_error("create unix sockst failed.");
		return -1;
	}

	int optval = 1;
	setsockopt(netfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	setsockopt(netfd, SOL_TCP, TCP_NODELAY, &optval, sizeof(optval));

	optval = 60;
	setsockopt(netfd, SOL_TCP, TCP_DEFER_ACCEPT, &optval, sizeof(optval));

	// bind addr, remove the maybe existen unique addr first
	unlink(m_domain_socket_path.c_str());
	int ret = bind(netfd, (sockaddr *)&local_addr, sizeof(local_addr));
	if (ret < 0) {
		log4cplus_error("bind addr failed!, addr:%s, errno:%d",
				m_domain_socket_path.c_str(), errno);
		close(netfd);
		return -1;
	}

	// listen for the connection
	ret = listen(netfd, 256);
	if (ret < 0) {
		log4cplus_error(
			"listen to the socket failed!, addr:%s, errno:%d",
			m_domain_socket_path.c_str(), errno);
		close(netfd);
		return -1;
	}

	return 0;
}

int RocksdbDirectListener::attach_thread()
{
	enable_input();
	int ret = EpollBase::attach_poller();
	if (ret < 0) {
		log4cplus_error("add rocksdb direct listener to poll failed.");
		return -1;
	}

	log4cplus_info("add rocksdb direct listener to poll successful, fd:%d",
		       netfd);

	return 0;
}

// handle client connection
void RocksdbDirectListener::input_notify()
{
	int newFd, ret;
	struct sockaddr_un peer;
	socklen_t peerSize = sizeof(peer);

	// extracts all the connected connections in the pending queue until return EAGAIN
	while (true) {
		newFd = accept(netfd, (struct sockaddr *)&peer, &peerSize);
		if (-1 == newFd) {
			if (errno == EINTR) {
				// system call "accept" was interrupted by signal before a valid connection
				// arrived, go on accept
				continue;
			}

			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				// no remaining connection on the pending queue, break out
				// log4cplus_info("accept new client error: %m, %d", errno);
				return;
			}

			// accept error
			log4cplus_error(
				"accept new client failed, netfd:%d, errno:%d",
				netfd, errno);
			return;
		}

		log4cplus_error(
			"accept new client to rocksdb direct process, newFd:%d",
			newFd);

		// add the handler vote event to another poll driver
		RocksdbDirectWorker *worker = new RocksdbDirectWorker(
			m_rocksdb_process, m_poller_thread, newFd);
		if (!worker) {
			log4cplus_error("create rocsdb direct workder failed!");
			continue;
		}

		worker->add_event_to_poll();
	}

	return;
}
