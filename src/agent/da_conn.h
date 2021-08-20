/*
 * da_conn.h
 *
 *  Created on: 2014Äê11ÔÂ30ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_CONN_H_
#define DA_CONN_H_

#include <sys/socket.h>
#include <stdint.h>
#include <stdbool.h>
#include "da_queue.h"
#include "da_rbtree.h"
#include "compiler.h"
#include "da_util.h"
#include "da_msg.h"
#include "da_errno.h"


struct conn;
struct context;
struct msg;

//connection type
#define FRONTWORK 0x0001
#define BACKWORK  0x0002
#define LISTENER  0x0004

//connection epoll flag
#define RECV_ACTIVE 0x0001
#define SEND_ACTIVE 0x0002
#define RECV_READY  0x0004
#define SEND_READY  0x0008

typedef int (*conn_recv_t)(struct context *, struct conn*);
typedef struct msg* (*conn_recv_next_t)(struct context *, struct conn *, bool);
typedef void (*conn_recv_done_t)(struct context *, struct conn *, struct msg *,struct msg *);

typedef int (*conn_send_t)(struct context *, struct conn*);
typedef struct msg* (*conn_send_next_t)(struct context *, struct conn *);
typedef void (*conn_send_done_t)(struct context *, struct conn *, struct msg *);

typedef void (*conn_close_t)(struct context *, struct conn *);
typedef int (*conn_active_t)(struct conn *);

typedef void (*conn_ref_t)(struct conn *, void *);
typedef void (*conn_unref_t)(struct conn *);
typedef void (*conn_msgq_t)(struct context *, struct conn *, struct msg *);

typedef void (*conn_msgtree_t)(struct context *, struct conn *, struct msg *);

struct conn {
	TAILQ_ENTRY(conn) conn_tqe; 	/*list linked in server or server pool*/
	void *owner;					/*owner server server_pool*/

	int fd;							/*socket fd*/
	int family;						/*socket family*/
	socklen_t addrlen;				/*socket addr len*/
	struct sockaddr *addr; 			/*socket address (ref in server)*/

	uint32_t type; 					/*front conn,back conn,or listener*/
	uint32_t events; 				/*the event need process*/
	uint32_t flag; 					/*epool flag*/

	struct rbtree msg_tree; 		/*tree for message search*/
	struct rbnode msg_rbs;			/*sentinel for msg_tree	*/
	struct msg_tqh imsg_q; 			/*request msgq */
	struct msg_tqh omsg_q; 			/*outstanding request Q */


	struct msg *rmsg; 				/* current message being rcvd */
	struct msg *smsg; 				/* current message being sent */

	conn_recv_t recv; 				/* recv (read) handler */
	conn_recv_next_t recv_next; 	/* recv next message handler */
	conn_recv_done_t recv_done; 	/* read done handler */
	conn_send_t send; 				/* send (write) handler */
	conn_send_next_t send_next; 	/* write next message handler */
	conn_send_done_t send_done; 	/* write done handler */
	conn_close_t close; 			/* close handler */
	conn_active_t active; 			/* active? handler */
	conn_ref_t ref; 				/* connection reference handler */
	conn_unref_t unref; 			/* connection unreference handler */

	conn_msgq_t enqueue_outq;  		/* connection outq msg enqueue handler */
	conn_msgq_t dequeue_outq;  		/* connection outq msg dequeue handler */
	conn_msgq_t enqueue_inq;  		/* connection outq msg enqueue handler */
	conn_msgq_t dequeue_inq;  		/* connection outq msg dequeue handler */

	conn_msgtree_t en_msgtree;		/* connection msg tree entree handler*/
	conn_msgtree_t de_msgtree;		/* connection msg tree detree handler*/

	size_t recv_bytes; 				/* received (read) bytes */
	size_t send_bytes; 				/* sent (written) bytes */

	int err ;						/*connection error no*/
	unsigned error :1;				/*connetion err sig*/
	unsigned connecting :1;			/*connecting is on*/
	unsigned connected :1;			/*connected*/
	unsigned eof:1;					/*connection is end,half connection*/
	unsigned done :1;				/*connection is done*/
	unsigned writecached:1;
	unsigned isvalid:1;
};

TAILQ_HEAD(conn_tqh, conn);

/*
 * init the pool2_conn
 */
int conn_init();

/*
 * deinit conn pool
 */
int conn_deinit();

/*
 * put conn back to pool
 */
void conn_put(struct conn *c);

/*
 *get listener from conn pool
 */
struct conn *get_listener(void *owner);

/*
 *get client conn
 */
struct conn *get_client_conn(void *owner);

/*
 *get server conn
 */
struct conn *get_server_conn(void *owner);

/*
*get instance conn
*/
struct conn *get_instance_conn(void *owner);

/*
 * get number of total conn
 */
uint64_t get_ntotal_conn();

/*
 * get number of current conn
 */
uint32_t get_ncurr_conn();

/*
 *get number of current client conn
 */
uint32_t get_ncurr_cconn();

/*recv data from fd*/
ssize_t conn_recv(struct conn *conn, void *buf, size_t size);

/* send data to fd*/
ssize_t conn_sendv(struct conn *conn, struct array *sendv, size_t nsend);

struct context *conn_to_ctx(struct conn *conn);

#endif /* DA_CONN_H_ */
