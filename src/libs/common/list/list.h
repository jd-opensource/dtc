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
#ifndef _H_LIST_H_
#define _H_LIST_H_
#include <sys/cdefs.h>
#include <stddef.h>

__BEGIN_DECLS
/*
 * Simple doubly linked list implementation.
 *
 * Some of the internal functions ("__xxx") are useful when
 * manipulating whole lists rather than single entries, as
 * sometimes we already know the next/prev entries and we can
 * generate better code by using them directly rather than
 * using the generic single-entry routines.
 */
#define __builtin_prefetch(x, y, z) (void)1
struct list_head {
	struct list_head *next, *prev;
};
typedef struct list_head list_head_t;

#define LIST_HEAD_INIT(name)                                                   \
	{                                                                      \
		&(name), &(name)                                               \
	}

#define LIST_HEAD(name) struct list_head name = LIST_HEAD_INIT(name)

#define INIT_LIST_HEAD(ptr)                                                    \
	do {                                                                   \
		(ptr)->next = (ptr);                                           \
		(ptr)->prev = (ptr);                                           \
	} while (0)

/*
 * Insert a p entry between two known consecutive entries. 
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __list_add(struct list_head *p, struct list_head *prev,
			      struct list_head *next)
{
	next->prev = p;
	p->next = next;
	p->prev = prev;
	prev->next = p;
}

/**
 * list_add - add a p entry
 * @p: p entry to be added
 * @head: list head to add it after
 *
 * Insert a p entry after the specified head.
 * This is good for implementing stacks.
 */
static inline void list_add(struct list_head *p, struct list_head *head)
{
	__list_add(p, head, head->next);
}
/**
 * list_add_tail - add a p entry
 * @p: p entry to be added
 * @head: list head to add it before
 *
 * Insert a p entry before the specified head.
 * This is useful for implementing queues.
 */
static inline void list_add_tail(struct list_head *p, struct list_head *head)
{
	__list_add(p, head->prev, head);
}

/*
 * Delete a list entry by making the prev/next entries
 * point to each other.
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __list_del(struct list_head *prev, struct list_head *next)
{
	next->prev = prev;
	prev->next = next;
}

/**
 * list_del - deletes entry from list.
 * @entry: the element to delete from the list.
 * Note: list_empty on entry does not return true after this, the entry is in an undefined state.
 */
static inline void list_del(struct list_head *entry)
{
	__list_del(entry->prev, entry->next);
	entry->next = 0;
	entry->prev = 0;
}

/**
 * list_del_init - deletes entry from list and reinitialize it.
 * @entry: the element to delete from the list.
 */
static inline void list_del_init(struct list_head *entry)
{
	__list_del(entry->prev, entry->next);
	INIT_LIST_HEAD(entry);
}

/**
 * list_move - delete from one list and add as another's head
 * @list: the entry to move
 * @head: the head that will precede our entry
 */
static inline void list_move(struct list_head *list, struct list_head *head)
{
	__list_del(list->prev, list->next);
	list_add(list, head);
}
/**
 * list_move_tail - delete from one list and add as another's tail
 * @list: the entry to move
 * @head: the head that will follow our entry
 */
static inline void _list_move_tail(struct list_head *list,
				   struct list_head *head)
{
	__list_del(list->prev, list->next);
	list_add_tail(list, head);
}

/**
 * list_empty - tests whether a list is empty
 * @head: the list to test.
 */
static inline int list_empty(const struct list_head *head)
{
	return head->next == head;
}

static inline void __list_splice(struct list_head *list, struct list_head *head)
{
	struct list_head *first = list->next;
	struct list_head *last = list->prev;
	struct list_head *at = head->next;

	first->prev = head;
	head->next = first;

	last->next = at;
	at->prev = last;
}

/**
 * list_splice - join two lists
 * @list: the p list to add.
 * @head: the place to add it in the first list.
 */
static inline void list_splice(struct list_head *list, struct list_head *head)
{
	if (!list_empty(list))
		__list_splice(list, head);
}

/**
 * list_splice_init - join two lists and reinitialise the emptied list.
 * @list: the p list to add.
 * @head: the place to add it in the first list.
 *
 * The list at @list is reinitialised
 */
static inline void list_splice_init(struct list_head *list,
				    struct list_head *head)
{
	if (!list_empty(list)) {
		__list_splice(list, head);
		INIT_LIST_HEAD(list);
	}
}

#ifndef offsetof
#if __GNUC__ >= 4
#define offsetof(type, member) __builtin_offsetof(type, member)
#else
#define offsetof(type, member) (unsigned long)(&((type *)0)->member)
#endif
#endif

/**
 * list_entry - get the struct for this entry
 * @ptr:	the &struct list_head pointer.
 * @type:	the type of the struct this is embedded in.
 * @member:	the name of the list_struct within the struct.
 */
#define list_entry(ptr, type, member)                                          \
	((type *)((char *)(ptr)-offsetof(type, member)))

/**
 * list_for_each	-	iterate over a list
 * @pos:	the &struct list_head to use as a loop counter.
 * @head:	the head for your list.
 */
#define list_for_each(pos, head)                                               \
	for (pos = (head)->next, __builtin_prefetch(pos->next, 0, 1);          \
	     pos != (head);                                                    \
	     pos = pos->next, __builtin_prefetch(pos->next, 0, 1))

#define __list_for_each(pos, head)                                             \
	for (pos = (head)->next; pos != (head); pos = pos->next)
/**
 * list_for_each_prev	-	iterate over a list backwards
 * @pos:	the &struct list_head to use as a loop counter.
 * @head:	the head for your list.
 */
#define list_for_each_prev(pos, head)                                          \
	for (pos = (head)->prev, __builtin_prefetch(pos->prev, 0, 1);          \
	     pos != (head);                                                    \
	     pos = pos->prev, __builtin_prefetch(pos->prev, 0, 1))

#define __list_for_each_prev(pos, head)                                        \
	for (pos = (head)->prev; pos != (head); pos = pos->prev)

/**
 * list_for_each_safe	-	iterate over a list safe against removal of list entry
 * @pos:	the &struct list_head to use as a loop counter.
 * @n:		another &struct list_head to use as temporary storage
 * @head:	the head for your list.
 */
#define list_for_each_safe(pos, n, head)                                       \
	for (pos = (head)->next, n = pos->next; pos != (head);                 \
	     pos = n, n = pos->next)

/**
 * list_for_each_entry	-	iterate over list of given type
 * @pos:	the type * to use as a loop counter.
 * @head:	the head for your list.
 * @member:	the name of the list_struct within the struct.
 */
#define list_for_each_entry(pos, head, member)                                 \
	for (pos = list_entry((head)->next, typeof(*pos), member),             \
	    __builtin_prefetch(pos->member.next, 0, 1);                        \
	     &pos->member != (head);                                           \
	     pos = list_entry(pos->member.next, typeof(*pos), member),         \
	    __builtin_prefetch(pos->member.next, 0, 1))

__END_DECLS
#ifdef __cplusplus

// internal, used by ListObject<> only
class ListHead {
    public:
	struct list_head objlist;

	inline void InitList(void)
	{
		INIT_LIST_HEAD(&objlist);
	}
	inline void ResetList(void)
	{
		list_del_init(&objlist);
	}
	inline int ListEmpty(void) const
	{
		return list_empty(&objlist);
	}
	inline ListHead *ListNext(void)
	{
		return list_entry(objlist.next, ListHead, objlist);
	}
	inline ListHead *ListPrev(void)
	{
		return list_entry(objlist.prev, ListHead, objlist);
	}

	inline void ListAdd(ListHead &n)
	{
		list_add(&objlist, &n.objlist);
	}
	inline void ListAdd(ListHead *n)
	{
		list_add(&objlist, &n->objlist);
	}
	inline void ListAddTail(ListHead &n)
	{
		list_add_tail(&objlist, &n.objlist);
	}
	inline void ListAddTail(ListHead *n)
	{
		list_add_tail(&objlist, &n->objlist);
	}
	inline void list_del(void)
	{
		ResetList();
	}
	inline void ListMove(ListHead &n)
	{
		list_move(&objlist, &n.objlist);
	}
	inline void ListMove(ListHead *n)
	{
		list_move(&objlist, &n->objlist);
	}
	inline void list_move_tail(ListHead &n)
	{
		_list_move_tail(&objlist, &n.objlist);
	}
	inline void list_move_tail(ListHead *n)
	{
		_list_move_tail(&objlist, &n->objlist);
	}
	inline void FreeList(void)
	{
		while (!ListEmpty())
			ListNext()->ResetList();
	}
};

// T is container class
// I is list index, if container inherit multiple ListObject
template <class T, int I = 0> class ListObject : public ListHead {
    public:
	inline ListObject(void)
	{
		InitList();
	}
	inline ~ListObject(void)
	{
		ResetList();
	}
	inline ListObject<T, I> *ListNext(void)
	{
		return (ListObject<T, I> *)ListHead::ListNext();
	}
	inline ListObject<T, I> *ListPrev(void)
	{
		return (ListObject<T, I> *)ListHead::ListPrev();
	}
	// inline T *ListOwner(void) { return static_cast<T *>(this); }
	inline T *ListOwner(void)
	{
		return (T *)this;
	}
	inline T *NextOwner(void)
	{
		return ListNext()->ListOwner();
	}
	inline T *PrevOwner(void)
	{
		return ListPrev()->ListOwner();
	}
};

#endif
#endif
