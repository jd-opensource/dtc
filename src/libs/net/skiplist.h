/*
*  skiplist.h
*
*  Created on: 2019.8.27
*  Author: liwujun
*/
#ifndef SKIPLIST_H_H
#define SKIPLIST_H_H

#include <string>
#include <stdint.h>
using namespace std;

typedef unsigned int uint32_t;

#define MAX_LEVEL 16
#define UNIQUE_INSERT 0
#define MAX_DOCID_LENGTH 32

struct SkipListNode {
    double key;
    char value[MAX_DOCID_LENGTH];
    int nodeLevel;
    SkipListNode *backward;
    struct SkipListLevel
    {
        SkipListNode *forward;
    }level[MAX_LEVEL];
};

class Random {

public:
    explicit Random(uint32_t s) : seed(s & 0x7fffffffu) {
        // Avoid bad seeds.
        if (seed == 0 || seed == 2147483647L) {
            seed = 1;
        }
    }

    uint32_t Next() {
        static const uint32_t M = 2147483647L;   // 2^31-1
        static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0

        uint64_t product = seed * A;
        seed = static_cast<uint32_t>((product >> 31) + (product & M));
        if (seed > M) {
            seed -= M;
        }
        return seed;
    }

    uint32_t Uniform(int n) { return (Next() % n); }
    bool OneIn(int n) { return (Next() % n) == 0; }
    uint32_t Skewed(int max_log) {
        return Uniform(1 << Uniform(max_log + 1));
    }

    int getRandomLevel() {
        int level = static_cast<int>(Uniform(MAX_LEVEL));
        return level + 1;
    }

private:
    uint32_t seed;
};

class SkipList {

public:
    SkipList() : rnd(0x12345678) {}
    ~SkipList() {
        FreeList();
    }

    bool InitList();
	void FreeList();
    SkipListNode* GetHeader() {return header;}
    SkipListNode* GetFooter() {return footer;}
    int GetSize() { return nodeCount; }
    int GetLevel() { return level; }
    bool InsertNode(double key, const char* value);
    bool IsEqual(const double key, const double ptr);

private:
    bool CreateNode(int level, SkipListNode *&node);
    bool CreateNode(int level, SkipListNode *&node, double key, const char* value);

private:
    int level;
    SkipListNode *header;
    SkipListNode *footer;
    uint32_t nodeCount;
    Random rnd;
};

#endif
