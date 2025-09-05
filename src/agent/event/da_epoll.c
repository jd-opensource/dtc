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

#include<inttypes.h>
#include<fcntl.h>
#include "da_event.h"
#include "../da_time.h"

struct event_base *event_base_create(int nevent, event_cb_t cb) {

	ASSERT(nevent > 0);
	ASSERT(cb != NULL);

	struct event_base *evb;
	int status, ep;
	struct epoll_event *event;

	ep = epoll_create(nevent);
	if (ep < 0) {
		return NULL;
	}

	status = fcntl(ep, F_SETFD, FD_CLOEXEC);
	if (status < 0) {
		log_error("fcntl for FD_CLOEXEC on e %d failed: %s", ep,
				strerror(errno));
		status = close(ep);
		if (status < 0) {
			log_error("close e %d failed, ignored: %s", ep, strerror(errno));
		}
		return NULL;
	}

	event = (struct epoll_event *) malloc(nevent * sizeof(*event));
	if (event == NULL) {
		return NULL;
	}
	evb = (struct event_base *) malloc(sizeof(*evb));
	if (evb == NULL) {
		status = close(ep);
		free(event);
		return NULL;
	}
	evb->ep = ep;
	evb->event = event;
	evb->nevent = nevent;
	evb->cb = cb;

	return evb;
}

void event_base_destroy(struct event_base *evb) {
	if (evb == NULL) {
		return;
	}
	free(evb->event);
	int status = close(evb->ep);
	if (status < 0) {
		log_error("close epoll fd: %d failed", evb->ep);
	}
	evb->ep = -1;
	free(evb);
}

int event_add_in(struct event_base *evb, struct conn *c) {

	ASSERT(evb != NULL);
	ASSERT(evb->ep > 0);
	ASSERT(c != NULL);
	ASSERT(c->fd > 0);

	int status;
	struct epoll_event event;
	int ep = evb->ep;
	ASSERT(ep > 0);

	if (c->flag & RECV_ACTIVE) {
		return 0;
	}
	event.events = (uint32_t) (EPOLLIN | EPOLLET);
	event.data.ptr = c;
	status = epoll_ctl(ep, EPOLL_CTL_MOD, c->fd, &event);
	if (status < 0) {
		log_error("epoll ctl on e %d sd %d failed: %s", ep, c->fd,
				strerror(errno));
	} else {
		c->flag |= RECV_ACTIVE;
	}
	return status;
}

int event_del_in(struct event_base *evb, struct conn *c) {

	return 0;
}

int event_add_out(struct event_base *evb, struct conn *c) {

	ASSERT(evb != NULL);
	ASSERT(c != NULL);
	ASSERT(c->fd > 0);

	int status;
	struct epoll_event event;
	int ep = evb->ep;
	ASSERT(ep > 0);
	if (c->flag & SEND_ACTIVE) {
		return 0;
	}
	event.events = (uint32_t) (EPOLLIN | EPOLLOUT | EPOLLET);
	event.data.ptr = c;

	status = epoll_ctl(ep, EPOLL_CTL_MOD, c->fd, &event);
	if (status < 0) {
		log_error("epoll ctl on e %d sd %d failed: %s", ep, c->fd,
				strerror(errno));
	} else {
		c->flag |= SEND_ACTIVE;
	}
	return status;
}

int event_del_out(struct event_base *evb, struct conn *c) {

	ASSERT(evb != NULL);
	ASSERT(c != NULL);
	ASSERT(c->fd > 0);

	int status;
	struct epoll_event event;
	int ep = evb->ep;
	ASSERT(ep > 0);

	if (!(c->flag & SEND_ACTIVE)) {
		return 0;
	}

	event.events = (uint32_t) (EPOLLIN | EPOLLET);
	event.data.ptr = c;

	status = epoll_ctl(ep, EPOLL_CTL_MOD, c->fd, &event);
	if (status < 0) {
		log_error("epoll ctl on e %d sd %d failed: %s", ep, c->fd,
				strerror(errno));
	} else {
		c->flag &= ~SEND_ACTIVE;
	}
	return status;
}

int event_add_conn(struct event_base *evb, struct conn *c) {

	ASSERT(evb != NULL);
	ASSERT(c != NULL);
	ASSERT(c->fd > 0);

	int status;
	struct epoll_event event;
	int ep = evb->ep;
	ASSERT(ep > 0);

	event.events = (uint32_t) (EPOLLIN | EPOLLOUT | EPOLLET);
	event.data.ptr = c;

	status = epoll_ctl(ep, EPOLL_CTL_ADD, c->fd, &event);
	if (status < 0) {
		log_error("epoll ctl on e %d sd %d failed: %s", ep, c->fd,
				strerror(errno));
	} else {
		c->flag |= RECV_ACTIVE;
		c->flag |= SEND_ACTIVE;
	}
	return status;
}

int event_del_conn(struct event_base *evb, struct conn *c) {

	ASSERT(evb != NULL);
	ASSERT(c != NULL);
	ASSERT(c->fd > 0);

	int status;
	int ep = evb->ep;
	ASSERT(ep > 0);

	status = epoll_ctl(ep, EPOLL_CTL_DEL, c->fd, NULL);
	if (status < 0) {
		log_error("epoll ctl on e %d sd %d failed: %s", ep, c->fd,
				strerror(errno));
	} else {
		c->flag &= ~RECV_ACTIVE;
		c->flag &= ~SEND_ACTIVE;
	}
	return status;
}

int event_wait(struct event_base *evb, int timeout) {

	int ep = evb->ep;
	struct epoll_event *event = evb->event;
	int nevent = evb->nevent;

	ASSERT(ep > 0);
	ASSERT(event != NULL);
	ASSERT(nevent > 0);

	for (;;) {
		int i, nsd;
		nsd = epoll_wait(ep, event, nevent, timeout);
		tv_update_date(timeout, nsd);
		if (nsd > 0) {
			for (i = 0; i < nsd; i++) {
				struct epoll_event *ev = &evb->event[i];
				uint32_t events = 0;

				log_debug("epoll %04"PRIX32" triggered on conn %p", ev->events,
						ev->data.ptr);

				if (ev->events & EPOLLERR) {
					events |= EVENT_ERR;
				}

				if (ev->events & (EPOLLIN | EPOLLHUP)) {
					events |= EVENT_READ;
				}

				if (ev->events & EPOLLOUT) {
					events |= EVENT_WRITE;
				}

				if (evb->cb != NULL) {
					evb->cb(ev->data.ptr, events);
				}
			}
			return nsd;
		}

		if (nsd == 0) {
			if (timeout == -1) {
				log_error("epoll wait on e %d with %d events and %d timeout "
						"returned no events", ep, nevent, timeout);
				return -1;
			}
			return 0;
		}

		if (errno == EINTR) {
			continue;
		}

		log_error("epoll wait on e %d with %d events failed: %s", ep, nevent,
				strerror(errno));
		return -1;
	}
}
