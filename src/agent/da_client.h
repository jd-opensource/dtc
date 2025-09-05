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
