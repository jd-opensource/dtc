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

#ifndef __DTC_BLACKLIST_H
#define __DTC_BLACKLIST_H

#include "list/list.h"
#include "list/hlist.h"
#include <stdint.h>
#include "namespace.h"

#define MAX_HASH_DEPTH 65535

DTC_BEGIN_NAMESPACE

struct blslot {
	struct hlist_node hash; /* link to hash_list */
	struct list_head time; /* link to time_list */
	unsigned int vsize;
	unsigned int expired;
	char key[0];
} __attribute__((__aligned__(1)));

class BlackList {
    public:
	BlackList();
	~BlackList();

	int init_blacklist(const unsigned max, const unsigned type,
			   const unsigned expired);
	int add_blacklist(const char *packedkey, const unsigned vsize);
	int in_blacklist(const char *packedkey);

	/* dump all blslot in blacklist, debug only */
	void dump_all_blslot(void);

    protected:
	/* try expire all expired slot */
	int try_expired_blacklist(void);

	unsigned current_blslot_count;

    private:
	/* double linked hash list with single pointer list head */
	struct hlist_head hash_list[MAX_HASH_DEPTH];

	/* time list */
	struct list_head time_list;

	unsigned max_blslot_count;
	unsigned blslot_expired_time;
	unsigned key_type;
	void stat_everything(const struct blslot *, const int add);
};

DTC_END_NAMESPACE
#endif
