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

#ifndef DA_BUF_H_
#define DA_BUF_H_

#include "da_queue.h"
#include "stdbool.h"

struct instance;
struct mbuf;

typedef void (*mbuf_copy_t)(struct mbuf *, void *);

struct mbuf {
  uint32_t magic;          /* mbuf magic (const) */
  STAILQ_ENTRY(mbuf) next; /* next mbuf */
  uint8_t *pos;            /* read marker */
  uint8_t *last;           /* write marker */
  uint8_t *start;          /* start of buffer (const) */
  uint8_t *end;            /* end of buffer (const) */
};

STAILQ_HEAD(buf_stqh, mbuf);

#define MBUF_MAGIC 0xdeadbeef
/*
 * The minimum length should receive the msg header
 */
#define MBUF_MIN_SIZE 256
#define MBUF_MAX_SIZE 16777216
#define DEF_MBUF_SIZE 16384

static inline bool mbuf_empty(struct mbuf *mbuf) {
  return mbuf->pos == mbuf->last ? true : false;
}

static inline bool mbuf_full(struct mbuf *mbuf) {
  return mbuf->last == mbuf->end ? true : false;
}

int mbuf_init(struct instance *ins);
int mbuf_deinit(void);
struct mbuf *mbuf_get();
void mbuf_put(struct mbuf *mbuf);
void mbuf_rewind(struct mbuf *mbuf);
uint32_t mbuf_length(struct mbuf *mbuf);
uint32_t mbuf_size(struct mbuf *mbuf);
size_t mbuf_data_size(void);
void mbuf_insert(struct buf_stqh *mhdr, struct mbuf *mbuf);
void mbuf_remove(struct buf_stqh *mhdr, struct mbuf *mbuf);
void mbuf_copy(struct mbuf *mbuf, uint8_t *pos, size_t n);
struct mbuf *mbuf_split(struct mbuf *mbuf, uint8_t *pos, mbuf_copy_t cb,
                        void *cbarg);
struct mbuf *_mbuf_split(struct mbuf *mbuf, uint8_t *pos, mbuf_copy_t cb,
                         void *cbarg);
#endif /* DA_BUF_H_ */
