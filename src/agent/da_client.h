/*
 * da_client.h
 *
 *  Created on: 2014Äê12ÔÂ3ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_CLIENT_H_
#define DA_CLIENT_H_
#include <stdbool.h>

struct context;
struct conn;

int client_active(struct conn *conn);
void client_ref(struct conn *conn, void *owner);
void client_unref(struct conn *conn);
void client_close(struct context *ctx, struct conn *conn);

#endif /* DA_CLIENT_H_ */
