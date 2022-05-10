/*
*  skiplist.cc
*
*  Created on: 2019.8.27
*  Author: liwujun
*/
#include "skiplist.h"
#include "mem_pool.h"
#include "log.h"
#include <cstddef>
#include <cassert>
#include <ctime>
#include <string.h>
using namespace std;

MemPool skipNodePool;

bool SkipList::InitList()
{
	if (!CreateNode(1, footer)) {
        log4cplus_error("create footer skiplist node error");
		return false;
	}
    footer->key = NULL;
    memset(footer->value, 0, MAX_DOCID_LENGTH);

    if(!CreateNode(MAX_LEVEL, header)) {
        skipNodePool.PoolFree(footer);
        footer = NULL;
        log4cplus_error("create header skiplist node error");
		return false;
    }
    for (int i = 0; i < MAX_LEVEL; ++i) {
        header->level[i].forward = footer;
    }
    header->key = NULL;
    memset(header->value, 0, MAX_DOCID_LENGTH);
    footer->backward = header;

    this->nodeCount = 0;
    this->level = 0;
    return true;
}

bool SkipList::CreateNode(int level, SkipListNode *&node) {
	node = (SkipListNode*)skipNodePool.PoolAlloc();
	if (node == NULL) {
        log4cplus_error("malloc skiplist node error");
		return false;
	}
    memset((void*)node, 0, sizeof(SkipListNode));
	for (int i = 0; i < level; i++) {
		node->level[i].forward = NULL;
	}
    node->nodeLevel = level;
    node->backward = NULL;
    return true;
}

bool SkipList::CreateNode(int level, SkipListNode *&node, double key, const char* value) {
    node = (SkipListNode*)skipNodePool.PoolAlloc();
    if (node == NULL) {
        log4cplus_error("malloc skiplist node error");
		return false;
	}
    memset((void*)node, 0, sizeof(SkipListNode));

    node->key = key;
    memset(node->value, 0, MAX_DOCID_LENGTH);
    memcpy(node->value, value, MAX_DOCID_LENGTH);
    node->nodeLevel = level;
    return true;
}

bool SkipList::InsertNode(double key, const char* value) {
    SkipListNode *update[MAX_LEVEL];

    SkipListNode *node = header;

    for (int i = level - 1; i >= 0; --i) {
        while (node->level[i].forward != footer && (node->level[i].forward->key < key || \
			(node->level[i].forward->key - key < 0.0001 && node->level[i].forward->key - key > -0.0001))) {
            node = node->level[i].forward;
        }
        update[i] = node;
    }

    #if UNIQUE_INSERT
        node = node->level[0].forward;
        if (node != footer && IsEqual(node->key, key)) {
            return true;
        }
    #endif

    int nodeLevel = rnd.getRandomLevel();

    if (nodeLevel > level) {
        nodeLevel = ++level;
        update[nodeLevel - 1] = header;
    }

    SkipListNode *newNode;
    if (!CreateNode(nodeLevel, newNode, key, value)) {
    	return false;
    }

    (update[0]->level[0].forward)->backward = newNode;
    newNode->backward = update[0];

    for (int i = nodeLevel - 1; i >= 0; --i) {
        node = update[i];
        newNode->level[i].forward = node->level[i].forward;
        node->level[i].forward = newNode;
    }
    ++nodeCount;

    return true;
}

void SkipList::FreeList() {

    SkipListNode *p = header;
    SkipListNode *q;
    while (p != NULL) {
        q = p->level[0].forward;
        skipNodePool.PoolFree(p);
        p = q;
    }
    header = NULL;
    footer = NULL;
    level = 0;
    nodeCount = 0;
}

bool SkipList::IsEqual(const double key, const double ptr)
{
    double res = key - ptr;
    if (res > -0.0001 && res < 0.00001)
        return true;
    return false;
}
