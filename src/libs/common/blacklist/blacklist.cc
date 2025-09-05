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
#include <time.h>
#include "log/log.h"
#include "blacklist.h"
#include "key/keycodec.h"

DTC_USING_NAMESPACE

BlackList::BlackList()
	: current_blslot_count(0), max_blslot_count(0), blslot_expired_time(0)
{
}

BlackList::~BlackList()
{
}

int BlackList::init_blacklist(const unsigned max, const unsigned keytype,
			      const unsigned expired)
{
	for (int i = 0; i < MAX_HASH_DEPTH; ++i) {
		INIT_HLIST_HEAD(hash_list + i);
	}

	INIT_LIST_HEAD(&time_list);

	key_type = keytype;
	current_blslot_count = 0;
	max_blslot_count =
		max < 1000000 ? max : 1000000; /* max:1000000 slots */
	blslot_expired_time = expired;

	return 0;
}

int BlackList::add_blacklist(const char *ptr, const unsigned vsize)
{
	if (!ptr)
		return 0;

	KeyCodec key_codec(key_type);

	struct hlist_head *h =
		hash_list + key_codec.key_hash(ptr) % MAX_HASH_DEPTH;
	struct hlist_node *pos = 0;
	struct blslot *tpos = 0;

	hlist_for_each_entry (tpos, pos, h, hash) {
		/* hit */
		if (0 == key_codec.key_compare(ptr, tpos->key)) {
			tpos->vsize = vsize;
			tpos->expired = time(NULL) + blslot_expired_time;
			/* adjust time list */
			_list_move_tail(&tpos->time, &time_list);
			return 0;
		}
	}

	if (current_blslot_count >= max_blslot_count)
		/* overflow */
		return -1;

	tpos = (struct blslot *)malloc(offsetof(struct blslot, key) +
				       key_codec.total_length(ptr));
	if (NULL == tpos) {
		log4cplus_info("allocate blacklist slot failed");
		return -1;
	}

	tpos->vsize = vsize;
	tpos->expired = time(NULL) + blslot_expired_time;
	memcpy(tpos->key, ptr, key_codec.total_length(ptr));

	list_add_tail(&tpos->time, &time_list);
	hlist_add_head(&tpos->hash, h);

	stat_everything(tpos, 1);
	return 0;
}

int BlackList::in_blacklist(const char *ptr)
{
	if (!ptr)
		return 0;

	KeyCodec key_codec(key_type);

	struct hlist_head *h =
		hash_list + key_codec.key_hash(ptr) % MAX_HASH_DEPTH;
	struct hlist_node *pos = 0;
	struct blslot *tpos = 0;

	hlist_for_each_entry (tpos, pos, h, hash) {
		/* found */
		if (0 == key_codec.key_compare(ptr, tpos->key))
			return 1;
	}

	return 0;
}

int BlackList::try_expired_blacklist(void)
{
	unsigned now = time(NULL);
	struct blslot *pos = 0;

	/* time->next is the oldest slot */
	while (!list_empty(&time_list)) {
		pos = list_entry(time_list.next, struct blslot, time);
		if (pos->expired > now)
			break;

		list_del(&pos->time);
		hlist_del(&pos->hash);
		stat_everything(pos, 0);
		free(pos);
	}

	return 0;
}

/* TODO: Count top10 */
void BlackList::stat_everything(const struct blslot *slot, const int add)
{
	/* add */
	if (add)
		++current_blslot_count;
	/* delete */
	else
		--current_blslot_count;
	return;
}

void BlackList::dump_all_blslot(void)
{
	KeyCodec key_codec(key_type);

	struct blslot *pos = 0;
	list_for_each_entry (pos, &time_list, time) {
		switch (key_type) {
		case 1:
			log4cplus_debug(
				"key: %u size: %u expired: %u hash: %u",
				*(uint8_t *)key_codec.key_pointer(pos->key),
				pos->vsize, pos->expired,
				key_codec.key_hash(pos->key) % MAX_HASH_DEPTH);
			break;
		case 2:
			log4cplus_debug(
				"key: %u size: %u expired: %u hash: %u",
				*(uint16_t *)key_codec.key_pointer(pos->key),
				pos->vsize, pos->expired,
				key_codec.key_hash(pos->key) % MAX_HASH_DEPTH);
			break;
		case 4:
			log4cplus_debug(
				"key: %u size: %u expired: %u hash: %u",
				*(uint32_t *)key_codec.key_pointer(pos->key),
				pos->vsize, pos->expired,
				key_codec.key_hash(pos->key) % MAX_HASH_DEPTH);
			break;
		default:
			log4cplus_debug(
				"key: %10s size: %u expired: %u hash: %u",
				key_codec.key_pointer(pos->key), pos->vsize,
				pos->expired,
				key_codec.key_hash(pos->key) % MAX_HASH_DEPTH);
			break;
		}
	}

	return;
}
