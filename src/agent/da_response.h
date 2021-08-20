/*
 * da_response.h
 *
 *  Created on: 2014Äê12ÔÂ9ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_RESPONSE_H_
#define DA_RESPONSE_H_

struct conn;
struct msg;
struct context;


void rsp_put(struct msg *msg);
struct msg *rsp_get(struct conn *conn);
struct msg *rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,struct msg *nmsg);
void rsp_forward(struct context *ctx, struct conn *s_conn, struct msg *req);
struct msg *rsp_send_next(struct context *ctx, struct conn *conn);
void rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg);

#endif /* DA_RESPONSE_H_ */
