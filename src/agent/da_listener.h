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

#ifndef DA_LISTENER_H_
#define DA_LISTENER_H_

#include "da_util.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

struct conn;
struct context;

void listener_ref(struct conn *l, void *owner);
void listener_unref(struct conn *l);
void listener_close(struct context *ctx, struct conn *l);

int listener_each_init(void *elem, void *data);
int listener_each_deinit(void *elem, void *data);

int listener_init(struct context *ctx);
void listener_deinit(struct context *ctx);
int listener_recv(struct context *ctx, struct conn *conn);

#endif /* DA_LISTENER_H_ */
