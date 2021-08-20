/*
 * da_signal.h
 *
 *  Created on: 2014Äê12ÔÂ2ÈÕ
 *      Author: Jiansong
 */

#ifndef DA_SIGNAL_H_
#define DA_SIGNAL_H_

#include <signal.h>
#include <string.h>
#include "da_queue.h"
#include "compiler.h"

#define MAX_SIGNAL 256

/* those are highly dynamic and stored in pools */
struct sig_handler {
	TAILQ_ENTRY(sig_handler) sig_tqe;
	void *handler; /* function to call or task to wake up */
	int arg; /*arg needed when process function or signals*/
};

TAILQ_HEAD(sig_tqh, sig_handler);

/* one per signal */
struct signal_descriptor {
	int count;				 /* number of times raised */
	struct sig_tqh sig_tqh;  /* sig_handler */
};

extern volatile int signal_queue_len;
extern struct signal_descriptor signal_state[];
extern struct pool_head *pool2_sig_handlers;

void signal_handler(int sig);
void __signal_process_queue();
int signal_init();
void deinit_signals();
struct sig_handler *signal_register_fct(int sig,void (*fct)(struct sig_handler *), int arg);
void signal_unregister_handler(int sig, struct sig_handler *handler);
void signal_unregister_target(int sig, void *target);



static inline void signal_process_queue() {
	if (unlikely(signal_queue_len > 0))
		__signal_process_queue();
}

#endif /* DA_SIGNAL_H_ */
