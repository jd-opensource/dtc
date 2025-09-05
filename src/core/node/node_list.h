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

#ifndef __DTC_NODE_LIST_H
#define __DTC_NODE_LIST_H

#include "namespace.h"
#include "global.h"
#include "node.h"

DTC_BEGIN_NAMESPACE

#define INIT_NODE_LIST_HEAD(node, id)                                          \
	do {                                                                   \
		node.lru_prev() = id;                                          \
		node.lru_next() = id;                                          \
	} while (0)

inline void __NODE_LIST_ADD(Node p, Node prev, Node next)
{
	next.lru_prev() = p.node_id();
	p.lru_next() = next.node_id();
	p.lru_prev() = prev.node_id();
	prev.lru_next() = p.node_id();
}

inline void NODE_LIST_ADD(Node p, Node head)
{
	__NODE_LIST_ADD(p, head, head.Next());
}

inline void NODE_LIST_ADD_TAIL(Node p, Node head)
{
	__NODE_LIST_ADD(p, head.Prev(), head);
}

inline void __NODE_LIST_DEL(Node prev, Node next)
{
	next.lru_prev() = prev.node_id();
	prev.lru_next() = next.node_id();
}

inline void NODE_LIST_DEL(Node p)
{
	__NODE_LIST_DEL(p.Prev(), p.Next());
	p.lru_prev() = p.node_id();
	p.lru_next() = p.node_id();
}

inline void NODE_LIST_MOVE(Node p, Node head)
{
	__NODE_LIST_DEL(p.Prev(), p.Next());
	NODE_LIST_ADD(p, head);
}

inline void NODE_LIST_MOVE_TAIL(Node p, Node head)
{
	__NODE_LIST_DEL(p.Prev(), p.Next());
	NODE_LIST_ADD_TAIL(p, head);
}

inline int NODE_LIST_EMPTY(Node head)
{
	return head.lru_next() == head.node_id();
}

/* Forward traversal */
#define NODE_LIST_FOR_EACH(pos, head)                                          \
	for (pos = head.Next(); pos != head; pos = pos.Next())

/* Reverse traversal */
#define NODE_LIST_FOR_EACH_RVS(pos, head)                                      \
	for (pos = head.Prev(); pos != head; pos = pos.Prev())

DTC_END_NAMESPACE

#endif
