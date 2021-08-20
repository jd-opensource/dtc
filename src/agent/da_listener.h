/*
 * da_listener.h
 *
 *  Created on: 2014Äê12ÔÂ3ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_LISTENER_H_
#define DA_LISTENER_H_

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "da_util.h"

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
