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

#ifndef T_TREE_H
#define T_TREE_H

#include <stdint.h>
#include "mem/mallocator.h"

int64_t KeyCompare(const char *pchKey, void *pCmpCookie, MallocBase &stMalloc,
		   ALLOC_HANDLE_T hOtherKey);
int Visit(MallocBase &stMalloc, ALLOC_HANDLE_T &hRecord, void *pCookie);

typedef int64_t (*KeyComparator)(const char *pchKey, void *pCmpCookie,
				 MallocBase &stMalloc,
				 ALLOC_HANDLE_T hOtherKey);
typedef int (*ItemVisit)(MallocBase &stMalloc, ALLOC_HANDLE_T &hRecord,
			 void *pCookie);

class Ttree {
    protected:
	ALLOC_HANDLE_T root_handle_;
	MallocBase &m_stMalloc;
	char err_message_[4096];

    public:
	Ttree(MallocBase &stMalloc);
	~Ttree();

	const char *get_err_msg()
	{
		return err_message_;
	}
	const ALLOC_HANDLE_T Root() const
	{
		return root_handle_;
	}
	ALLOC_HANDLE_T first_node();

	/*************************************************
	  Description:	attach a block of pre-formatted memory
	  Input:		
	  Output:		
	  Return:		
	*************************************************/
	void do_attach(ALLOC_HANDLE_T hRoot)
	{
		root_handle_ = hRoot;
	}

	/*************************************************
	  Description:	Insert key into the tree, hRecord is the data corresponding to the key (including key)
	  Input:		pchKey		Key to insert
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				hRecord		Handle storing the key to insert and other data
	  Output:		
	  Return:		0 for success, EC_NO_MEM for insufficient memory, EC_KEY_EXIST for key already exists, other values for error
	*************************************************/
	int do_insert(const char *pchKey, void *pCmpCookie,
		      KeyComparator pfComp, ALLOC_HANDLE_T hRecord,
		      bool &isAllocNode);

	/*************************************************
	  Description:	Delete key and corresponding data (but will not automatically release memory for the key)
	  Input:		pchKey		Key to delete
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int Delete(const char *pchKey, void *pCmpCookie, KeyComparator pfComp,
		   bool &isFreeNode);

	int find_handle(ALLOC_HANDLE_T hRecord);

	/*************************************************
	  Description:	Find data corresponding to the key
	  Input:		pchKey		Key to search for
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
	  Output:		hRecord		Handle storing the found key and other data
	  Return:		0 if not found, 1 if data found
	*************************************************/
	int do_find(const char *pchKey, void *pCmpCookie, KeyComparator pfComp,
		    ALLOC_HANDLE_T &hRecord);

	/*************************************************
	  Description:	Find data corresponding to the key
	  Input:		pchKey		Key to search for
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
	  Output:		phRecord		Pointer to tree node item
	  Return:		0 if not found, 1 if data found
	*************************************************/
	int do_find(const char *pchKey, void *pCmpCookie, KeyComparator pfComp,
		    ALLOC_HANDLE_T *&phRecord);

	/*************************************************
	  Description:	Destroy the entire tree and release corresponding memory
	  Input:		
	  Output:		
	  Return:		0 for success, non-zero for failure
	*************************************************/
	int destory();

	/*************************************************
	  Description: Query how much free memory can be released by destroying the entire tree	
	  Input:		
	  Output:		
	  Return:	 >0 success, 0 failure
	*************************************************/
	unsigned ask_for_destroy_size(void);

	/*************************************************
	  Description:	Traverse the entire tree from small to large
	  Input:		pfVisit	User-defined function to visit data records
				pCookie	Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int traverse_forward(ItemVisit pfVisit, void *pCookie);

	/*************************************************
	  Description:	Traverse the entire tree from large to small
	  Input:		pfVisit	User-defined function to visit data records
				pCookie	Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int traverse_backward(ItemVisit pfVisit, void *pCookie);

	/*************************************************
	  Description:	Post-order traverse the entire tree
	  Input:		pfVisit	User-defined function to visit data records
				pCookie	Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int post_order_traverse(ItemVisit pfVisit, void *pCookie);

	/*************************************************
	  Description:	Traverse tree from small to large starting from specified key, range [key, key+iInclusion]
	  Input:		pchKey		Starting key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				iInclusion		Key range
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int traverse_forward(const char *pchKey, void *pCmpCookie,
			     KeyComparator pfComp, int64_t iInclusion,
			     ItemVisit pfVisit, void *pCookie);

	/*************************************************
	  Description:	Traverse tree from small to large starting from specified key, range [key, key1]
	  Input:		pchKey		Starting key
				pchKey1		Ending key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int traverse_forward(const char *pchKey, const char *pchKey1,
			     void *pCmpCookie, KeyComparator pfComp,
			     ItemVisit pfVisit, void *pCookie);

	/*************************************************
	  Description:	Traverse tree from small to large starting from specified key (traverse all records >= key)
	  Input:		pchKey		Starting key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int traverse_forward(const char *pchKey, void *pCmpCookie,
			     KeyComparator pfComp, ItemVisit pfVisit,
			     void *pCookie);

	/*************************************************
	  Description:	Traverse tree from large to small starting from specified key (traverse all records <= key)
	  Input:		pchKey		Starting key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int traverse_backward(const char *pchKey, void *pCmpCookie,
			      KeyComparator pfComp, ItemVisit pfVisit,
			      void *pCookie);

	/*************************************************
	  Description:	Traverse tree from large to small starting from specified key, range [key, key1]
	  Input:		pchKey		Starting key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int traverse_backward(const char *pchKey, const char *pchKey1,
			      void *pCmpCookie, KeyComparator pfComp,
			      ItemVisit pfVisit, void *pCookie);

	/*************************************************
	  Description:	Starting from specified key, left and right subtree first, then root node, range [key, key1]
	  Input:		pchKey		Starting key
				pchKey1		Ending key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int post_order_traverse(const char *pchKey, const char *pchKey1,
				void *pCmpCookie, KeyComparator pfComp,
				ItemVisit pfVisit, void *pCookie);

	/*************************************************
	  Description:	Post-order traverse tree starting from specified key (traverse all records >= key)
	  Input:		pchKey		Starting key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int post_order_traverse_ge(const char *pchKey, void *pCmpCookie,
				   KeyComparator pfComp, ItemVisit pfVisit,
				   void *pCookie);

	/*************************************************
	  Description:	Post-order traverse tree starting from specified key (traverse all records <= key)
	  Input:		pchKey		Starting key
				pCmpCookie	Input parameter when calling user-defined pfComp function to compare with tree nodes
				pfComp		User-defined key comparison function
				pfVisit		User-defined function to visit data records
				pCookie		Cookie parameter for custom function
	  Output:		
	  Return:		0 for success, other values for error
	*************************************************/
	int post_order_traverse_le(const char *pchKey, void *pCmpCookie,
				   KeyComparator pfComp, ItemVisit pfVisit,
				   void *pCookie);
};

/************************************************************
  Description:    Encapsulates various operations of T-tree node, for internal use by t-tree only   
  Version:         DTC 3.0
***********************************************************/
struct _TtreeNode {
	enum { PAGE_SIZE = 20, // How many records each node stores
	       MIN_ITEMS =
		       PAGE_SIZE - 2 // minimal number of items in internal node
	};

	ALLOC_HANDLE_T m_hLeft;
	ALLOC_HANDLE_T m_hRight;
	int8_t m_chBalance;
	uint16_t m_ushNItems;
	ALLOC_HANDLE_T m_ahItems[PAGE_SIZE];

	int do_init();
	static ALLOC_HANDLE_T Alloc(MallocBase &stMalloc,
				    ALLOC_HANDLE_T hRecord);
	static int do_insert(MallocBase &stMalloc, ALLOC_HANDLE_T &hNode,
			     const char *pchKey, void *pCmpCookie,
			     KeyComparator pfComp, ALLOC_HANDLE_T hRecord,
			     bool &isAllocNode);
	static int Delete(MallocBase &stMalloc, ALLOC_HANDLE_T &hNode,
			  const char *pchKey, void *pCmpCookie,
			  KeyComparator pfComp, bool &isFreeNode);
	static int balance_left_branch(MallocBase &stMalloc,
				       ALLOC_HANDLE_T &hNode);
	static int balance_right_branch(MallocBase &stMalloc,
					ALLOC_HANDLE_T &hNode);
	static int destory(MallocBase &stMalloc, ALLOC_HANDLE_T hNode);
	static unsigned ask_for_destroy_size(MallocBase &,
					     ALLOC_HANDLE_T hNode);

	// Find specified key. Return 1 if found, otherwise return 0
	int do_find(MallocBase &stMalloc, const char *pchKey, void *pCmpCookie,
		    KeyComparator pfComp, ALLOC_HANDLE_T &hRecord);
	int do_find(MallocBase &stMalloc, const char *pchKey, void *pCmpCookie,
		    KeyComparator pfComp, ALLOC_HANDLE_T *&phRecord);
	int find_handle(MallocBase &stMalloc, ALLOC_HANDLE_T hRecord);
	// Assume node contains keys k1~kn, find such node: k1 <= key <= kn
	int find_node(MallocBase &stMalloc, const char *pchKey,
		      void *pCmpCookie, KeyComparator pfComp,
		      ALLOC_HANDLE_T &hNode);
	int traverse_forward(MallocBase &stMalloc, ItemVisit pfVisit,
			     void *pCookie);
	int traverse_backward(MallocBase &stMalloc, ItemVisit pfVisit,
			      void *pCookie);
	int post_order_traverse(MallocBase &stMalloc, ItemVisit pfVisit,
				void *pCookie);

	int traverse_forward(MallocBase &stMalloc, const char *pchKey,
			     void *pCmpCookie, KeyComparator pfComp,
			     int iInclusion, ItemVisit pfVisit, void *pCookie);
	int traverse_forward(MallocBase &stMalloc, const char *pchKey,
			     void *pCmpCookie, KeyComparator pfComp,
			     ItemVisit pfVisit, void *pCookie);
	int traverse_forward(MallocBase &stMalloc, const char *pchKey,
			     const char *pchKey1, void *pCmpCookie,
			     KeyComparator pfComp, ItemVisit pfVisit,
			     void *pCookie);

	int traverse_backward(MallocBase &stMalloc, const char *pchKey,
			      void *pCmpCookie, KeyComparator pfComp,
			      ItemVisit pfVisit, void *pCookie);
	int traverse_backward(MallocBase &stMalloc, const char *pchKey,
			      const char *pchKey1, void *pCmpCookie,
			      KeyComparator pfComp, ItemVisit pfVisit,
			      void *pCookie);

	int post_order_traverse(MallocBase &stMalloc, const char *pchKey,
				const char *pchKey1, void *pCmpCookie,
				KeyComparator pfComp, ItemVisit pfVisit,
				void *pCookie);
	int post_order_traverse_ge(MallocBase &stMalloc, const char *pchKey,
				   void *pCmpCookie, KeyComparator pfComp,
				   ItemVisit pfVisit, void *pCookie);
	int post_order_traverse_le(MallocBase &stMalloc, const char *pchKey,
				   void *pCmpCookie, KeyComparator pfComp,
				   ItemVisit pfVisit, void *pCookie);
} __attribute__((packed));
typedef struct _TtreeNode TtreeNode;

#endif
