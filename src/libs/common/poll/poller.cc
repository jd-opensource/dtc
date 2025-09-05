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
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>
#include "mem_check.h"

#include "poller_base.h"
#include "poller.h"
#include "../log/log.h"

EpollBase::~EpollBase()
{
	if (owner_unit && epslot)
		owner_unit->remove_epoll_slot(epslot);

	if (netfd > 0) {
		log4cplus_debug("%d fd been closed!", netfd);
		close(netfd);
		netfd = 0;
	}
	if (event_slot) {
		event_slot->poller = NULL;
		event_slot = NULL;
	}
}

int EpollBase::attach_poller(EpollOperation *unit)
{
	if (unit) {
		if (owner_unit == NULL)
			owner_unit = unit;
		else
			return -1;
	}
	if (netfd < 0)
		return -1;

	if (epslot <= 0) {
		epslot = owner_unit->insert_epoll_slot();
		if (epslot < 0)
			return -1;
		struct EpollSlot *slot = owner_unit->get_slot(epslot);
		slot->poller = this;

		int flag = fcntl(netfd, F_GETFL);
		fcntl(netfd, F_SETFL, O_NONBLOCK | flag);
		struct epoll_event ev;
		memset(&ev, 0x0, sizeof(ev));
		ev.events = new_events;
		slot->seq++;
		ev.data.u64 = ((unsigned long long)slot->seq << 32) + epslot;
		if (owner_unit->epoll_control(EPOLL_CTL_ADD, netfd, &ev) == 0)
			old_events = new_events;
		else {
			owner_unit->remove_epoll_slot(epslot);
			log4cplus_warning("epoll_control: %m");
			return -1;
		}
		return 0;
	}
	return apply_events();
}

int EpollBase::detach_poller()
{
	if (epslot) {
		struct epoll_event ev;
		memset(&ev, 0x0, sizeof(ev));
		if (owner_unit->epoll_control(EPOLL_CTL_DEL, netfd, &ev) == 0)
			old_events = new_events;
		else {
			log4cplus_warning("epoll_control: %m");
			return -1;
		}
		owner_unit->remove_epoll_slot(epslot);
		epslot = 0;
	}
	return 0;
}

int EpollBase::apply_events()
{
	if (epslot <= 0 || old_events == new_events)
		return 0;

	struct epoll_event ev;
	memset(&ev, 0x0, sizeof(ev));

	ev.events = new_events;
	struct EpollSlot *slot = owner_unit->get_slot(epslot);
	slot->seq++;
	ev.data.u64 = ((unsigned long long)slot->seq << 32) + epslot;
	if (owner_unit->epoll_control(EPOLL_CTL_MOD, netfd, &ev) == 0)
		old_events = new_events;
	else {
		log4cplus_warning("epoll_control: %m");
		return -1;
	}

	return 0;
}

int EpollBase::delay_apply_events()
{
	if (epslot <= 0 || old_events == new_events)
		return 0;

	if (event_slot)
		return 0;

	event_slot = owner_unit->add_delay_event_poller(this);
	if (event_slot == NULL) {
		log4cplus_error("max events!!!!!!");
		struct epoll_event ev;

		ev.events = new_events;
		struct EpollSlot *slot = owner_unit->get_slot(epslot);
		slot->seq++;
		ev.data.u64 = ((unsigned long long)slot->seq << 32) + epslot;
		if (owner_unit->epoll_control(EPOLL_CTL_MOD, netfd, &ev) == 0)
			old_events = new_events;
		else {
			log4cplus_warning("epoll_control: %m");
			return -1;
		}
	}

	return 0;
}

int EpollBase::check_link_status(void)
{
	char msg[1] = { 0 };
	int err = 0;

	err = recv(netfd, msg, sizeof(msg), MSG_DONTWAIT | MSG_PEEK);

	/* client already close connection. */
	if (err == 0 || (err < 0 && errno != EAGAIN))
		return -1;
	return 0;
}

void EpollBase::init_poll_fd(struct pollfd *pfd)
{
	pfd->fd = netfd;
	pfd->events = new_events;
	pfd->revents = 0;
}

void EpollBase::input_notify(void)
{
	log4cplus_debug("enter input_notify.");
	enable_input(false);
	log4cplus_debug("leave input_notify.");
}

void EpollBase::output_notify(void)
{
	log4cplus_debug("enter output_notify.");
	enable_output(false);
	log4cplus_debug("leave output_notify.");
}

void EpollBase::hangup_notify(void)
{
	log4cplus_debug("enter hangup_notify.");
	delete this;
	log4cplus_debug("leave hangup_notify.");
}

int EpollOperation::total_event_slot = 40960;
EpollOperation::EpollOperation(int mp)
{
	max_pollers = mp;

	eevent_size = max_pollers > 1024 ? 1024 : max_pollers;
	epfd = -1;
	ep_events = NULL;
	poller_list = NULL;
	current_pos = 0;
	used_pollers = 0;
	//not initailize event_cnt variable may crash, fix crash bug by linjinming 2014-05-18
	event_cnt = 0;
}

EpollOperation::~EpollOperation()
{
	// skip first one
	for (int i = 1; i < max_pollers; i++) {
		if (poller_list[i].next)
			continue;
	}

	FREE_CLEAR(poller_list);

	if (epfd != -1) {
		close(epfd);
		epfd = -1;
	}

	FREE_CLEAR(ep_events);
}

int EpollOperation::set_max_pollers(int mp)
{
	if (epfd >= 0)
		return -1;
	max_pollers = mp;
	return 0;
}

int EpollOperation::initialize_poller_unit(void)
{
	poller_list =
		(struct EpollSlot *)CALLOC(max_pollers, sizeof(*poller_list));

	if (!poller_list) {
		log4cplus_error("calloc failed, num=%d, %m", max_pollers);
		return -1;
	}

	// already zero-ed
	for (int i = 1; i < max_pollers - 1; i++) {
		poller_list[i].next = i + 1;
	}

	poller_list[max_pollers - 1].next = 0;
	current_pos = 1;

	ep_events = (struct epoll_event *)CALLOC(eevent_size,
						 sizeof(struct epoll_event));

	if (!ep_events) {
		log4cplus_error("malloc failed, %m");
		return -1;
	}

	if ((epfd = epoll_create(max_pollers)) == -1) {
		log4cplus_warning("epoll_create failed, %m");
		return -1;
	}
	fcntl(epfd, F_SETFD, FD_CLOEXEC);
	return 0;
}

inline int EpollOperation::verify_events(struct epoll_event *ev)
{
	int idx = EPOLL_DATA_SLOT(ev);

	if ((idx >= max_pollers) ||
	    (EPOLL_DATA_SEQ(ev) != poller_list[idx].seq)) {
		return -1;
	}

	if (poller_list[idx].poller == NULL || poller_list[idx].next != 0) {
		log4cplus_info(
			"receive invalid epoll event. idx=%d seq=%d poller=%p freelist=%d event=%x",
			idx, (int)EPOLL_DATA_SEQ(ev), poller_list[idx].poller,
			poller_list[idx].next, ev->events);
		return -1;
	}
	return 0;
}

void EpollOperation::remove_epoll_slot(int n)
{
	if (n <= 0)
		return;
	poller_list[n].next = current_pos;
	current_pos = n;
	used_pollers--;
	poller_list[n].seq++;
	poller_list[n].poller = NULL;
}

int EpollOperation::insert_epoll_slot()
{
	if (0 == current_pos) {
		log4cplus_error("no free epoll slot, used_pollers = %d",
				used_pollers);
		return -1;
	}

	int n = current_pos;

	used_pollers++;
	current_pos = poller_list[n].next;
	poller_list[n].next = 0;

	return n;
}

int EpollOperation::epoll_control(int op, int fd, struct epoll_event *events)
{
	if (epoll_ctl(epfd, op, fd, events) == -1) {
		log4cplus_warning("epoll_ctl error, epfd=%d, fd=%d", epfd, fd);

		return -1;
	}

	return 0;
}

int EpollOperation::wait_poller_events(int timeout)
{
	need_request_events_count =
		epoll_wait(epfd, ep_events, eevent_size, timeout);
	return need_request_events_count;
}

void EpollOperation::process_poller_events(void)
{
	for (int i = 0; i < need_request_events_count; i++) {
		if (verify_events(ep_events + i) == -1) {
			log4cplus_info(
				"verify_events failed, ep_events[%d].data.u64 = %llu",
				i, (unsigned long long)ep_events[i].data.u64);
			continue;
		}

		EpollSlot *s = &poller_list[EPOLL_DATA_SLOT(ep_events + i)];
		EpollBase *p = s->poller;

		p->new_events = p->old_events;
		if (ep_events[i].events & (EPOLLHUP | EPOLLERR)) {
			p->hangup_notify();
			continue;
		}

		if (ep_events[i].events & EPOLLIN)
			p->input_notify();

		s = &poller_list[EPOLL_DATA_SLOT(ep_events + i)];
		if (s->poller == p && ep_events[i].events & EPOLLOUT)
			p->output_notify();

		s = &poller_list[EPOLL_DATA_SLOT(ep_events + i)];
		if (s->poller == p)
			p->delay_apply_events();
	}
}

int EpollOperation::delay_apply_events()
{
	for (int i = 0; i < event_cnt; i++) {
		EpollBase *p = event_slot[i].poller;
		if (p) {
			p->apply_events();
			event_slot[i].poller = NULL;
			p->clean_slot_event();
		}
	}
	event_cnt = 0;
	return 0;
}
