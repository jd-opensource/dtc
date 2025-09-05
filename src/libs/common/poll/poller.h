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
#ifndef __POLLER_H__
#define __POLLER_H__

#include <arpa/inet.h>
#include <sys/poll.h>
#include "myepoll.h"
#include "list/list.h"

#define EPOLL_DATA_SLOT(x) ((x)->data.u64 & 0xFFFFFFFF)
#define EPOLL_DATA_SEQ(x) ((x)->data.u64 >> 32)

class EpollOperation;
class EpollBase;

struct EpollSlot {
	EpollBase *poller;
	uint32_t seq;
	uint32_t next;
};

struct EventSlot {
	EpollBase *poller;
};

class EpollBase {
    public:
	EpollBase(EpollOperation *o = NULL, int fd = 0)
		: owner_unit(o), netfd(fd), new_events(0), old_events(0),
		  epslot(0)
	{
	}

	virtual ~EpollBase();

	virtual void input_notify(void);
	virtual void output_notify(void);
	virtual void hangup_notify(void);

	void enable_input(void)
	{
		new_events |= EPOLLIN;
	}
	void enable_output(void)
	{
		new_events |= EPOLLOUT;
	}
	void disable_input(void)
	{
		new_events &= ~EPOLLIN;
	}
	void disable_output(void)
	{
		new_events &= ~EPOLLOUT;
	}

	void enable_input(bool i)
	{
		if (i)
			new_events |= EPOLLIN;
		else
			new_events &= ~EPOLLIN;
	}
	void enable_output(bool o)
	{
		if (o)
			new_events |= EPOLLOUT;
		else
			new_events &= ~EPOLLOUT;
	}

	int attach_poller(EpollOperation *thread = NULL);
	int detach_poller(void);
	int apply_events();
	int delay_apply_events();
	void clean_slot_event()
	{
		event_slot = NULL;
	}
	int check_link_status(void);

	void init_poll_fd(struct pollfd *);

	friend class EpollOperation;

    protected:
	EpollOperation *owner_unit;
	int netfd;
	int new_events;
	int old_events;
	int epslot;
	struct EventSlot *event_slot;
};

class EpollOperation {
    public:
	friend class EpollBase;
	EpollOperation(int mp);
	virtual ~EpollOperation();

	int set_max_pollers(int mp);
	int get_max_pollers(void) const
	{
		return max_pollers;
	}
	int initialize_poller_unit(void);
	int wait_poller_events(int);
	void process_poller_events(void);
	int get_fd(void)
	{
		return epfd;
	}
	int delay_apply_events();

    private:
	int verify_events(struct epoll_event *);
	int epoll_control(int op, int fd, struct epoll_event *events);
	struct EpollSlot *get_slot(int n)
	{
		return &poller_list[n];
	}
	const struct EpollSlot *get_slot(int n) const
	{
		return &poller_list[n];
	}

	void remove_epoll_slot(int n);
	int insert_epoll_slot(void);
	struct EventSlot *add_delay_event_poller(EpollBase *p)
	{
		if (event_cnt == total_event_slot)
			return NULL;
		event_slot[event_cnt++].poller = p;
		return &event_slot[event_cnt - 1];
	}

    private:
	struct EpollSlot *poller_list;
	struct epoll_event *ep_events;
	int epfd;
	int eevent_size;
	int current_pos;
	int max_pollers;
	int used_pollers;
	/* FIXME: maybe too small */
	static int total_event_slot;
	struct EventSlot event_slot[40960];
	int event_cnt;

    protected:
	int need_request_events_count;
};

#endif
