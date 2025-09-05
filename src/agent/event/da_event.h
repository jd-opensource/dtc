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

#ifndef DA_EVENT_H_
#define DA_EVENT_H_

#include "../da_conn.h"
#include "../da_log.h"
#include "../da_util.h"
#include "errno.h"
#include <stdio.h>
#include <sys/epoll.h>

#define EVENT_SIZE 1024

#define EVENT_READ 0x0000ff
#define EVENT_WRITE 0x00ff00
#define EVENT_ERR 0xff0000

struct conn;

typedef int (*event_cb_t)(void *, uint32_t);

struct event_base {
  int ep;
  struct epoll_event *event;
  int nevent;
  event_cb_t cb;
};

struct event_base *event_base_create(int nevent, event_cb_t cb);
void event_base_destroy(struct event_base *evb);
int event_add_in(struct event_base *evb, struct conn *c);
int event_del_in(struct event_base *evb, struct conn *c);
int event_add_out(struct event_base *evb, struct conn *c);
int event_del_out(struct event_base *evb, struct conn *c);
int event_add_conn(struct event_base *evb, struct conn *c);
int event_del_conn(struct event_base *evb, struct conn *c);
int event_wait(struct event_base *evb, int timeout);

#endif /* DA_EVENT_H_ */
