/*
 * da_rbtree.h
 *
 *  Created on: 2014Äê12ÔÂ2ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_RBTREE_H_
#define DA_RBTREE_H_

#include<stdint.h>
#include<stdio.h>

#define rbtree_red(_node)           ((_node)->color = 1)
#define rbtree_black(_node)         ((_node)->color = 0)
#define rbtree_is_red(_node)        ((_node)->color)
#define rbtree_is_black(_node)      (!rbtree_is_red(_node))
#define rbtree_copy_color(_n1, _n2) ((_n1)->color = (_n2)->color)

struct rbnode {
    struct rbnode *left;     /* left link */
    struct rbnode *right;    /* right link */
    struct rbnode *parent;   /* parent link */
    uint64_t       key;       /* key for ordering */
    void          *data;     /* opaque data */
    uint8_t       color;     /* red | black */
};

struct rbtree {
    struct rbnode *root;     /* root node */
    struct rbnode *sentinel; /* nil node */
};

void rbtree_node_init(struct rbnode *node);
void rbtree_init(struct rbtree *tree, struct rbnode *node);
struct rbnode *rbtree_min(struct rbtree *tree);
struct rbnode *rbtree_search(struct rbtree *tree,struct rbnode *tnode);
void rbtree_insert(struct rbtree *tree, struct rbnode *node);
void rbtree_delete(struct rbtree *tree, struct rbnode *node);

#endif /* DA_RBTREE_H_ */
