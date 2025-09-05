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

#include "da_signal.h"
#include "da_log.h"
#include "da_mem_pool.h"
#include "da_core.h"

/* Principle : we keep an in-order list of the first occurrence of all received
 * signals. All occurrences of a same signal are grouped though. The signal
 * queue does not need to be deeper than the number of signals we can handle.
 * The handlers will be called asynchronously with the signal number. They can
 * check themselves the number of calls by checking the descriptor this signal.
 */

volatile int signal_queue_len; /* length of signal queue, <= MAX_SIGNAL (1 entry per signal max) */
int signal_queue[MAX_SIGNAL]; /* in-order queue of received signals */
struct signal_descriptor signal_state[MAX_SIGNAL];
struct pool_head *pool2_sig_handlers = NULL;
sigset_t blocked_sig;
int signal_pending = 0; /* non-zero if t least one signal remains unprocessed */

/* Common signal handler, used by all signals. Received signals are queued.
 * Signal number zero has a specific status, as it cannot be delivered by the
 * system, any function may call it to perform asynchronous signal delivery.
 */
void signal_handler(int sig) {
	if (sig < 0 || sig >= MAX_SIGNAL) {
		/* unhandled signal */
		signal(sig, SIG_IGN);
		log_error("Received unhandled signal %d. Signal has been disabled.\n",
				sig);
		return;
	}

	if (!signal_state[sig].count) {
		/* signal was not queued yet */
		if (signal_queue_len < MAX_SIGNAL)
			signal_queue[signal_queue_len++] = sig;
		else
			log_error("Signal %d : signal queue is unexpectedly full.\n", sig);
	}

	signal_state[sig].count++;
	if (sig)
		signal(sig, signal_handler); /* re-arm signal */

	log_debug("size of signal_queue_len is %d",signal_queue_len);
}

/* Call handlers of all pending signals and clear counts and queue length. The
 * handlers may unregister themselves by calling signal_register() while they
 * are called, just like it is done with normal signal handlers.
 * Note that it is more efficient to call the inline version which checks the
 * queue length before getting here.
 */
void __signal_process_queue() {
	int sig, cur_pos = 0;
	struct signal_descriptor *desc;
	sigset_t old_sig;

	/* block signal delivery during processing */
	sigprocmask(SIG_SETMASK, &blocked_sig, &old_sig);

	/* It is important that we scan the queue forwards so that we can
	 * catch any signal that would have been queued by another signal
	 * handler. That allows real signal handlers to redistribute signals
	 * to tasks subscribed to signal zero.
	 */
	for (cur_pos = 0; cur_pos < signal_queue_len; cur_pos++) {
		sig = signal_queue[cur_pos];
		desc = &signal_state[sig];
		if (desc->count) {
			struct sig_handler *sh, *shb;
			TAILQ_FOREACH_SAFE(sh,&desc->sig_tqh,sig_tqe,shb)
			{
				if (sh->handler) {
					((void (*)(struct sig_handler *)) sh->handler)(sh);
				}
			}
			desc->count = 0;
		}
	}
	signal_queue_len = 0;

	/* restore signal delivery */
	sigprocmask(SIG_SETMASK, &old_sig, NULL);
}

/* perform minimal intializations*/
int signal_init() {
	int sig;

	signal_queue_len = 0;
	memset(signal_queue, 0, sizeof(signal_queue));
	memset(signal_state, 0, sizeof(signal_state));
	sigfillset(&blocked_sig);
	sigdelset(&blocked_sig, SIGPROF);
	for (sig = 0; sig < MAX_SIGNAL; sig++)
		TAILQ_INIT(&signal_state[sig].sig_tqh);

	pool2_sig_handlers = create_pool("sig_handlers", sizeof(struct sig_handler),
	MEM_F_SHARED);
	if(pool2_sig_handlers == NULL)
	{
		return -1;
	}
	return 0;
}

/* releases all registered signal handlers */
void deinit_signals() {
	int sig;
	struct sig_handler *sh, *shb;

	for (sig = 0; sig < MAX_SIGNAL; sig++) {
		if (sig != SIGPROF)
			signal(sig, SIG_DFL);
		TAILQ_FOREACH_SAFE(sh,&signal_state[sig].sig_tqh,sig_tqe,shb)
		{
			TAILQ_REMOVE(&signal_state[sig].sig_tqh, sh, sig_tqe);
			pool_free(pool2_sig_handlers, sh);
		}
	}
}

/* Register a function and an integer argument on a signal. A pointer to the
 * newly allocated sig_handler is returned, or NULL in case of any error. The
 * caller is responsible for unregistering the function when not used anymore.
 * Note that passing a NULL as the function pointer enables interception of the
 * signal without processing, which is identical to SIG_IGN. If the signal is
 * zero (which the system cannot deliver), only internal functions will be able
 * to notify the registered functions.
 */
struct sig_handler *signal_register_fct(int sig,
		void (*fct)(struct sig_handler *), int arg) {
	log_debug("enter signal_register_fct");
	struct sig_handler *sh;

	if (sig < 0 || sig >= MAX_SIGNAL)
		return NULL;

	if (sig)
		signal(sig, fct ? signal_handler : SIG_IGN);

	if (!fct)
		return NULL;

	sh = pool_alloc(pool2_sig_handlers);
	if (!sh)
		return NULL;

	sh->handler = fct;
	sh->arg = arg;
	TAILQ_INSERT_TAIL(&signal_state[sig].sig_tqh, sh, sig_tqe);
	return sh;
}

/* Immediately unregister a handler so that no further signals may be delivered
 * to it. The struct is released so the caller may not reference it anymore.
 */
void signal_unregister_handler(int sig, struct sig_handler *handler) {
	if (handler == NULL)
		return;
	if (sig < 0 || sig >= MAX_SIGNAL)
		return;
	TAILQ_REMOVE(&signal_state[sig].sig_tqh, handler, sig_tqe);
	pool_free(pool2_sig_handlers, handler);
}

/* Immediately unregister a handler so that no further signals may be delivered
 * to it. The handler struct does not need to be known, only the function or
 * task pointer. This method is expensive because it scans all the list, so it
 * should only be used for rare cases (eg: exit). The struct is released so the
 * caller may not reference it anymore.
 */
void signal_unregister_target(int sig, void *target) {
	struct sig_handler *sh, *shb;

	if (sig < 0 || sig >= MAX_SIGNAL)
		return;

	if (!target)
		return;
	TAILQ_FOREACH_SAFE(sh,&signal_state[sig].sig_tqh,sig_tqe,shb)
	{
		if (sh->handler == target) {
			TAILQ_REMOVE(&signal_state[sig].sig_tqh, sh, sig_tqe);
			pool_free(pool2_sig_handlers, sh);
			break;
		}
	}
}
