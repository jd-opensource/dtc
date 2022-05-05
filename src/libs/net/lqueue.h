#ifndef __LQUEUE_H__
#define __LQUEUE_H__

#include <malloc.h>

#include "compiler.h"

template<class T>
class CLinkQueue {
protected:
	struct slot_t
	{
		slot_t *next;
		T data;
	};

public:
	class allocator {
	public:
		inline allocator(int mc=1024){
			freeList = NULL;
			freecount = 0;
			freemax = mc>60000?60000:mc;
		}
		inline ~allocator(){
			free_cache();
		}
	private:
		friend class CLinkQueue<T>;
		unsigned short freecount;
		unsigned short freemax;
		slot_t *freeList;

		__INLINE void free_cache() {
			while(freeList) {
				slot_t *s = freeList;
				freeList = s->next;
				freecount--;
				free(s);
			}
		}
		__INLINE slot_t *getslot(void)
		{
			slot_t *s;
			if (likely(freeList)) {
				s = freeList;
				freeList = s->next;
				freecount --;	
			} else {
				s = (slot_t *)malloc(sizeof(slot_t));
			}
			return s;
		}
		__INLINE void putslot(slot_t *s)
		{
			if(unlikely(freecount >= freemax)) {
				free(s);
			} else {
				s->next = freeList;
				freeList = s;
				freecount++;
			}
		}
	};
public:
	inline CLinkQueue (allocator *a=0)
	{
		count = 0;
		head = NULL;
		tail = &head;
		alloc = a;
	}

	inline ~CLinkQueue () {
		while(head) {
			slot_t *s = head;
			head = s->next;
			free(s);
		}
		head = NULL;
		tail = NULL;
	}

	__INLINE void SetAllocator(allocator *a) { alloc = a; }
	__INLINE int QueueEmpty () const {
		return count==0;
	}

	__INLINE int Count() const {
		return count;
	}

	__INLINE T Front(void) const {
		return head==NULL ? NULL : head->data;
	}

	__INLINE int Push (T p)
	{
		slot_t *s = getslot();
		if(unlikely(s==NULL))
			return -1;
	
		count++;
		s->data = p;
		s->next = NULL;
		*tail = s;
		tail = &s->next;
		return 0;
	}

	__INLINE int Unshift (T p)
	{
		slot_t *s = getslot();
		if(s==NULL)
			return -1;
		count++;
	
		s->data = p;
		s->next = head;
		if(head == NULL)
			tail = &s->next;
		head = s;
		return 0;
	}


	__INLINE T Pop ()
	{
		if (head==NULL)
			return (T)0;
		slot_t *s = head;
		head = s->next;
		if(head == NULL)
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
	__INLINE slot_t *getslot(void) {
		if(alloc) return alloc->getslot();
		return (slot_t *)malloc(sizeof(slot_t));
	}
	__INLINE void putslot(slot_t *p) {
		if(alloc){
			alloc->putslot(p);
			return;
		};
		free(p);
	}
};

#endif
