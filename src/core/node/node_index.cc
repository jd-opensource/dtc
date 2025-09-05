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
#include <stdio.h>
#include "node_index.h"
#include "algorithm/singleton.h"
#include "node.h"

DTC_USING_NAMESPACE

NodeIndex::NodeIndex() : _firstIndex(NULL)
{
	memset(errmsg_, 0, sizeof(errmsg_));
}

NodeIndex::~NodeIndex()
{
}

NodeIndex *NodeIndex::instance()
{
	return Singleton<NodeIndex>::instance();
}

void NodeIndex::destroy()
{
	Singleton<NodeIndex>::destory();
}

int NodeIndex::pre_allocate_index(size_t mem_size)
{
	/* 
	 * Allocate 2-level NodeIndex assuming all nodes are empty nodes
	 * An empty node occupies 44 bytes
	 */
	uint32_t n = 65536 * 256 * 44;
	n = mem_size / n + 1;
	n = n > 256 ? 256 : n;

	for (uint32_t i = 0; i < n; ++i) {
		_firstIndex->fi_h[i] = M_CALLOC(INDEX_2_SIZE);

		if (INVALID_HANDLE == _firstIndex->fi_h[i]) {
			log4cplus_error("PANIC: PrepareNodeIndex[%u] failed",
					i);
			return DTC_CODE_FAILED;
		}
	}

	return DTC_CODE_SUCCESS;
}

int NodeIndex::do_insert(Node node)
{
	NODE_ID_T id = node.node_id();

	if (INVALID_HANDLE == _firstIndex->fi_h[OFFSET1(id)]) {
		_firstIndex->fi_h[OFFSET1(id)] = M_CALLOC(INDEX_2_SIZE);
		if (INVALID_HANDLE == _firstIndex->fi_h[OFFSET1(id)]) {
			log4cplus_error(
				"PANIC: do_insert node=%u to NodeIndex failed",
				id);
			return DTC_CODE_FAILED;
		}
	}

	SECOND_INDEX_T *p =
		M_POINTER(SECOND_INDEX_T, _firstIndex->fi_h[OFFSET1(id)]);
	p->si_used++;
	p->si_h[OFFSET2(id)] = M_HANDLE(node.Owner());

	return DTC_CODE_SUCCESS;
}

Node NodeIndex::do_search(NODE_ID_T id)
{
	if (INVALID_NODE_ID == id)
		return Node(NULL, 0);

	if (INVALID_HANDLE == _firstIndex->fi_h[OFFSET1(id)])
		return Node(NULL, 0);

	SECOND_INDEX_T *p =
		M_POINTER(SECOND_INDEX_T, _firstIndex->fi_h[OFFSET1(id)]);
	if (INVALID_HANDLE == p->si_h[OFFSET2(id)])
		return Node(NULL, 0);

	NODE_SET *NS = M_POINTER(NODE_SET, p->si_h[OFFSET2(id)]);

	int index = (id - NS->ng_nid);
	if (index < 0 || index > 255)
		return Node(NULL, 0);

	return Node(NS, index);
}

int NodeIndex::do_init(size_t mem_size)
{
	MEM_HANDLE_T v = M_CALLOC(INDEX_1_SIZE);
	if (INVALID_HANDLE == v) {
		log4cplus_error("Create get_index-1 failed");
		return DTC_CODE_FAILED;
	}

	_firstIndex = M_POINTER(FIRST_INDEX_T, v);

	return pre_allocate_index(mem_size);
}

int NodeIndex::do_attach(MEM_HANDLE_T handle)
{
	if (INVALID_HANDLE == handle) {
		log4cplus_error("attach index-1 failed, memory handle=0");
		return DTC_CODE_FAILED;
	}

	_firstIndex = M_POINTER(FIRST_INDEX_T, handle);
	return DTC_CODE_SUCCESS;
}

int NodeIndex::do_detach(void)
{
	_firstIndex = 0;
	return DTC_CODE_SUCCESS;
}
