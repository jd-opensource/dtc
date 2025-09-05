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
#ifndef __LQUEUE_H__
#define __LQUEUE_H__

#include <malloc.h>
#include "compiler.h"

template <class T> class LinkQueue {
    protected:
	struct slot_t {
		slot_t *next;
		T data;
	};

    public:
	class allocator {
	    public:
		inline allocator(int mc = 1024)
		{
			free_list = NULL;
			freecount = 0;
			freemax = mc > 60000 ? 60000 : mc;
		}
		inline ~allocator()
		{
			free_cache();
		}

	    private:
		friend class LinkQueue<T>;
		unsigned short freecount;
		unsigned short freemax;
		slot_t *free_list;

		__INLINE void free_cache()
		{
			while (free_list) {
				slot_t *s = free_list;
				free_list = s->next;
				freecount--;
				free(s);
			}
		}
		__INLINE slot_t *getslot(void)
		{
			slot_t *s;
			if (likely(free_list)) {
				s = free_list;
				free_list = s->next;
				freecount--;
			} else {
				s = (slot_t *)malloc(sizeof(slot_t));
			}
			return s;
		}
		__INLINE void putslot(slot_t *s)
		{
			if (unlikely(freecount >= freemax)) {
				free(s);
			} else {
				s->next = free_list;
				free_list = s;
				freecount++;
			}
		}
	};

    public:
	inline LinkQueue(allocator *a = 0)
	{
		count = 0;
		head = NULL;
		tail = &head;
		alloc = a;
	}

	inline ~LinkQueue()
	{
		while (head) {
			slot_t *s = head;
			head = s->next;
			free(s);
		}
		head = NULL;
		tail = NULL;
	}

	__INLINE void SetAllocator(allocator *a)
	{
		alloc = a;
	}
	__INLINE int queue_empty() const
	{
		return count == 0;
	}

	__INLINE int Count() const
	{
		return count;
	}

	__INLINE T Front(void) const
	{
		return head == NULL ? NULL : head->data;
	}

	__INLINE int Push(T p)
	{
		slot_t *s = getslot();
		if (unlikely(s == NULL))
			return -1;

		count++;
		s->data = p;
		s->next = NULL;
		*tail = s;
		tail = &s->next;
		return 0;
	}

	__INLINE int Unshift(T p)
	{
		slot_t *s = getslot();
		if (s == NULL)
			return -1;
		count++;

		s->data = p;
		s->next = head;
		if (head == NULL)
			tail = &s->next;
		head = s;
		return 0;
	}

	__INLINE T Pop()
	{
		if (head == NULL)
			return (T)0;
		slot_t *s = head;
		head = s->next;
		if (head == NULL)
			tail = &head;
		T ret = s->data;
		putslot(s);
		count--;
		return ret;
	}

    protected:
	allocator *alloc;
	slot_t *head;
	slot_t **tail;
	int count;

    private:
	__INLINE slot_t *getslot(void)
	{
		if (alloc)
			return alloc->getslot();
		return (slot_t *)malloc(sizeof(slot_t));
	}
	__INLINE void putslot(slot_t *p)
	{
		if (alloc)
			return alloc->putslot(p);
		return free(p);
	}
};

#endif
