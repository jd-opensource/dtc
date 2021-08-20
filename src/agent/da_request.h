/*
 * da_request.h
 *
 *  Created on: 2014Äê12ÔÂ9ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_REQUEST_H_
#define DA_REQUEST_H_
#include <stdbool.h>

struct msg;
struct context;
struct conn;

void req_put(struct msg *msg);
struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,struct msg *nmsg);
void req_client_enqueue_omsgq(struct context *ctx, struct conn *conn,struct msg *msg);
void req_client_dequeue_omsgq(struct context *ctx, struct conn *conn,struct msg *msg);
void req_client_enqueue_imsgq(struct context *ctx, struct conn *conn,struct msg *msg);
void req_client_dequeue_imsgq(struct context *ctx, struct conn *conn,struct msg *msg);
bool req_done(struct conn *c, struct msg *msg);
void req_server_enqueue_imsgq(struct context *ctx, struct conn *conn,struct msg *msg);
void req_server_dequeue_imsgq(struct context *ctx, struct conn *conn,struct msg *msg);
struct msg *req_send_next(struct context *ctx, struct conn *conn);
void req_send_done(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_de_msgtree(struct context *ctx, struct conn *conn,
		struct msg *msg);
void req_server_en_msgtree(struct context *ctx, struct conn *conn,
		struct msg *msg);
bool req_error(struct conn *conn, struct msg *msg);
#endif /* DA_REQUEST_H_ */
