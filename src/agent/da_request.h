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

#ifndef DA_REQUEST_H_
#define DA_REQUEST_H_
#include <stdbool.h>

struct msg;
struct context;
struct conn;

void req_put(struct msg *msg);
struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
		   struct msg *nmsg);
void req_client_enqueue_omsgq(struct context *ctx, struct conn *conn,
			      struct msg *msg);
void req_client_dequeue_omsgq(struct context *ctx, struct conn *conn,
			      struct msg *msg);
void req_client_enqueue_imsgq(struct context *ctx, struct conn *conn,
			      struct msg *msg);
void req_client_dequeue_imsgq(struct context *ctx, struct conn *conn,
			      struct msg *msg);
bool req_done(struct conn *c, struct msg *msg);
void req_server_enqueue_imsgq(struct context *ctx, struct conn *conn,
			      struct msg *msg);
void req_server_dequeue_imsgq(struct context *ctx, struct conn *conn,
			      struct msg *msg);
struct msg *req_send_next(struct context *ctx, struct conn *conn);
void req_send_done(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_de_msgtree(struct context *ctx, struct conn *conn,
			   struct msg *msg);
void req_server_en_msgtree(struct context *ctx, struct conn *conn,
			   struct msg *msg);
bool req_error(struct conn *conn, struct msg *msg);

static void req_forward(struct context *ctx, struct conn *c_conn,
			struct msg *msg);

void error_reply(struct msg *msg, struct conn *conn, struct context *ctx, int errcode);

#endif /* DA_REQUEST_H_ */
