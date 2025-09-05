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
#include "da_buf.h"
#include "stdlib.h"
#include "da_mem_pool.h"
#include "da_util.h"
#include "da_queue.h"
#include "compiler.h"
#include "da_core.h"
#include <stddef.h>
#include "da_string.h"

static size_t mbuf_offset; /* mbuf offset in chunk (const) */
struct pool_head *pool2_buf = NULL;

int mbuf_init(struct instance *ins) {
	pool2_buf = create_pool("mbuf", ins->mbuf_chunk_size, MEM_F_SHARED);
	if (pool2_buf == NULL) {
		log_error("init pool2_buf error");
		return -1;
	}
	mbuf_offset = ins->mbuf_chunk_size - sizeof(struct mbuf);
	log_debug("mbuf hsize %lu chunk size %zu offset %zu length %zu",
			sizeof(struct mbuf), ins->mbuf_chunk_size, mbuf_offset,
			mbuf_offset);
	return 0;
}

int mbuf_deinit(void) {
	void *res = pool_destroy(pool2_buf);
	if (res == NULL) {
		log_debug("free buf pool success!");
		return 0;
	} else {
		log_error("buf pool is in use,can't be free!");
		return -1;
	}
}

static struct mbuf *_mbuf_get() {
	struct mbuf *mbuf;
	uint8_t *buf;
	buf = pool_alloc(pool2_buf);
	if (buf == NULL) {
		return NULL;
	}
	mbuf = (struct mbuf *) (buf + mbuf_offset);
	mbuf->magic = MBUF_MAGIC;

	STAILQ_NEXT(mbuf, next) = NULL;
	return mbuf;
}

struct mbuf *mbuf_get() {
	struct mbuf *mbuf;
	uint8_t *buf;

	mbuf = _mbuf_get();
	if (mbuf == NULL) {
		log_error("get mbuf error,lack of memory");
		return NULL;
	}
	buf = (uint8_t *) mbuf - mbuf_offset;
	mbuf->start = buf;
	mbuf->end = buf + mbuf_offset;

	ASSERT(mbuf->end - mbuf->start == (int )mbuf_offset);ASSERT(mbuf->start < mbuf->end);

	mbuf->pos = mbuf->start;
	mbuf->last = mbuf->start;

	log_debug("get mbuf %p", mbuf);
	return mbuf;
}

/*
 * Return mbuf to memory pool
 */
void mbuf_put(struct mbuf *mbuf) {
	uint8_t *buf;
	if (mbuf == NULL) {
		return;
	}
	buf = (uint8_t *) mbuf - mbuf_offset;
	pool_free(pool2_buf, buf);
}

/*
 * Reset mbuf
 */
void mbuf_rewind(struct mbuf *mbuf) {
	mbuf->pos = mbuf->start;
	mbuf->last = mbuf->start;
}

/*
 * Return existing data in mbuf
 */
uint32_t mbuf_length(struct mbuf *mbuf) {
	ASSERT(mbuf->last >= mbuf->pos);
	return (uint32_t) (mbuf->last - mbuf->pos);
}

uint32_t mbuf_size(struct mbuf *mbuf) {
	ASSERT(mbuf->end >= mbuf->last);
	return (uint32_t) (mbuf->end - mbuf->last);
}

size_t mbuf_data_size(void) {
	return mbuf_offset;
}

void mbuf_insert(struct buf_stqh *mhdr, struct mbuf *mbuf) {
	STAILQ_INSERT_TAIL(mhdr, mbuf, next);
	log_debug("insert mbuf %p len %ld", mbuf, mbuf->last - mbuf->pos);
}

void mbuf_remove(struct buf_stqh *mhdr, struct mbuf *mbuf) {
	log_debug("remove mbuf %p len %ld", mbuf, mbuf->last - mbuf->pos);

	STAILQ_REMOVE(mhdr, mbuf, mbuf, next);
	STAILQ_NEXT(mbuf, next) = NULL;
}

/*
 * copy data to buf,not over a buf
 */
void mbuf_copy(struct mbuf *mbuf, uint8_t *pos, size_t n) {
	if (n == 0) {
		return;
	}
	/* mbuf has space for n bytes */
	ASSERT(!mbuf_full(mbuf) && n <= mbuf_size(mbuf));
	/* no overlapping copy */
	ASSERT(pos < mbuf->start || pos >= mbuf->end);
	da_memcpy(mbuf->last, pos, (size_t ) (n));
	mbuf->last += n;
}

struct mbuf* mbuf_split(struct mbuf *mbuf, uint8_t *pos, mbuf_copy_t cb,
		void *cbarg) {
	struct mbuf *nbuf;
	size_t size;

	nbuf = mbuf_get();
	if (nbuf == NULL) {
		return NULL;
	}
	if (cb != NULL) {
		/* precopy nbuf */
		cb(nbuf, cbarg);
	}
	/* copy data from mbuf to nbuf */
	size = (size_t) (mbuf->last - pos);
	mbuf_copy(nbuf, pos, size);
	/* adjust mbuf */
	mbuf->last = pos;
	return nbuf;
}

struct mbuf* _mbuf_split(struct mbuf *mbuf, uint8_t *pos, mbuf_copy_t cb,
		void *cbarg) {
	struct mbuf *nbuf;
	size_t size;

	nbuf = mbuf_get();
	if (nbuf == NULL) {
		return NULL;
	}
	if (cb != NULL) {
		/* precopy nbuf */
		cb(nbuf, cbarg);
	}
	/* copy data from mbuf to nbuf */
	size = (size_t)(pos-mbuf->pos);
	mbuf_copy(nbuf, mbuf->pos, size);
	mbuf->pos=pos;
	return nbuf;
}
