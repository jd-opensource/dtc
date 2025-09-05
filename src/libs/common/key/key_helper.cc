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
#include <string.h>
#include "mem_check.h"

#include "key_helper.h"

static int keycmp_1(const char *a, const char *b, int ks)
{
	return *a - *b;
}

static int keycmp_2(const char *a, const char *b, int ks)
{
	return *(uint16_t *)a - *(uint16_t *)b;
}

static int keycmp_4(const char *a, const char *b, int ks)
{
	return *(uint32_t *)a - *(uint32_t *)b;
}

static int keycmp_n(const char *a, const char *b, int ks)
{
	return memcmp(a, b, ks);
}

static int keycmp_b(const char *a, const char *b, int ks)
{
	return *a != *b ? 1 : memcmp(a + 1, b + 1, *a);
}

KeyHelper::KeyHelper(int ks, int exp)
{
	keySize = ks;
	timeout = exp;
	switch (keySize) {
	case 0:
		keycmp = keycmp_b;
	case 1:
		keycmp = keycmp_1;
	case 2:
		keycmp = keycmp_2;
	case 4:
		keycmp = keycmp_4;
	default:
		keycmp = keycmp_n;
	}
	for (int i = 0; i < HASHBASE; i++) {
		INIT_LIST_HEAD(hashList + i);
	}
	INIT_LIST_HEAD(&lru);
};

KeyHelper::~KeyHelper()
{
}

void KeyHelper::try_expire(void)
{
	unsigned int deadline = time(NULL) - timeout;
	while (!list_empty(&lru)) {
		struct keyslot *k = list_entry(lru.next, struct keyslot, tlist);
		if (k->timestamp >= deadline)
			break;
		list_del(&k->hlist);
		list_del(&k->tlist);
		FREE(k);
	}
}

void KeyHelper::add_key(unsigned long barrier_hash, const char *ptrKey)
{
	struct list_head *h =
		hashList + ((unsigned long)barrier_hash) % HASHBASE;
	struct keyslot *k;
	list_for_each_entry (k, h, hlist) {
		if (keycmp(ptrKey, k->data, keySize) == 0) {
			k->timestamp = time(NULL);
			_list_move_tail(&k->tlist, &lru);
			return;
		}
	}

	int size = keySize;
	if (size == 0)
		size = 1 + *(unsigned char *)ptrKey;
	k = (struct keyslot *)MALLOC(offsetof(struct keyslot, data) + size);
	memcpy(k->data, ptrKey, size);
	k->timestamp = time(NULL);
	list_add(&k->hlist, h);
	list_add(&k->tlist, &lru);
}

bool KeyHelper::in_set(unsigned long barrier_hash, const char *ptrKey)
{
	struct list_head *h =
		hashList + ((unsigned long)barrier_hash) % HASHBASE;
	struct keyslot *k;
	list_for_each_entry (k, h, hlist) {
		if (keycmp(ptrKey, k->data, keySize) == 0)
			return true;
	}

	return false;
}
