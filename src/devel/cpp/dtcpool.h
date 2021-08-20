#ifndef __CHC_CLI_POOL_H
#define __CHC_CLI_POOL_H

#include <sys/poll.h>

#include "list/list.h"
#include "poll/poller.h"
#include "timer/timer_list.h"
#include "dtcint.h"
#include "dtc_error_code.h"

class NCServerInfo;
class NCPool;
class NCConnection;

// transaction is a internal async request
class NCTransaction :
	public ListObject<NCTransaction>
{
public:
	// constructor/destructor equiv
	NCTransaction(void);
	~NCTransaction(void);
	/* clear transaction state */
	void Clear(void);
	/* abort current transaction, zero means succ */
	void Abort(int);
	/* transaction succ with result */
	void done(NCResult *res) { result = res; Abort(0); }
	/* get transaction result */
	NCResult * get_result(void);
	/* adjust generation id */
	void round_gen_id(int m) { if(genId >= m) genId = 0; }

	/* state & info management */
	int State(void) const { return state; }
	int GenId(void) const { return genId; }
	int MatchSN(NCResult *res) const { return SN == res->versionInfo.serial_nr(); }
	/* send packet management */
	int Send(int fd) { return packet->Send(fd); }
	int attach_request(NCServerInfo *s, long long tag, NCRequest *req, DTCValue *key);
	void attach_connection(NCConnection *c);
	void send_ok(ListObject<NCTransaction>*);
	void recv_ok(ListObject<NCTransaction>*);

public: // constant member declare as static
	// owner info, associated server
	NCServerInfo *server;
	// attached connection, SEND, RECV
	NCConnection *conn;

private:// transient members is private
	// current transaction state, WAIT, SEND, RECV, DONE
	int state;
	// internal transaction generation id
	int genId;
	// associated request tag
	long long reqTag;
	// associated request SN, state SEND, RECV
	uint64_t SN;

	// sending packet
	Packet *packet;

	// do_execute result
	NCResult *result;
};

class NCConnection :
	public ListObject<NCConnection>,
	public EpollBase
{
public:
	NCConnection(NCPool *, NCServerInfo *);
	~NCConnection(void);

	int is_dgram(void) const;
	int is_async(void) const;
	/* starting state machine, by NCServerInfo::connect */
	int Connect(void);
	/* attach transaction to connection */
	void process_request(NCTransaction *);
	/* flush send channel */
	void send_request(void);
	/* flush recv channel */
	int recv_result(void);
	/* check connection hangup, recv zero bytes */
	int check_hangup(void);

	/* abort all associated request */
	void abort_requests(int err);
	/* close connection */
	void Close(int err);
	/* abort current sending request, linger recv channel */
	void abort_send_side(int err);
	/* a valid result received */
	void done_result(void);
	/* connection is idle, usable for transaction */
	void switch_to_idle(void);
	/* connection is async connecting */
	void switch_to_connecting(void);

	virtual void input_procedure(void);
	virtual void output_procedure(void);
	virtual void hang_procedure(void);
	virtual void job_timer_procedure(void);

private:
	/* associated server */
	NCServerInfo *server_info;
	/* queued transaction in RECV state */
	ListObject<NCTransaction> req_list;
	/* sending transaction */
	NCTransaction *sreq;
	/* decoding/decoded result */
	NCResult *result;
	/* connection state */
	int state;

	int NETFD(void) const { return netfd; }

private:
	TimerMember<NCConnection> timer;
	SimpleReceiver receiver;
};

typedef ListObject<NCConnection> NCConnectionList;

class NCPool :
	public EpollOperation,
	public TimerUnit
{
public:
	NCPool(int max_servers, int max_requests);
	~NCPool();

	int initialize_poller_unit(void);

	int get_epoll_fd(int maxpoller);
	int add_server(NCServer *srv, int maxReq=1, int maxConn=0);
	int add_request(NCRequest *req, long long tag, DTCValue *key=0);

	void execute_one_loop(int timeout);
	int do_execute(int timeout);
	int execute_all(int timeout);
	
	NCTransaction *Id2Req(int) const;
	int cancel_request(int);
	int cancel_all_request(int);
	int abort_request(NCTransaction *);
	int abort_request(int);
	int abort_all_request(int);
	NCResult *get_result(void);
	NCResult *get_result(int);

	int count_request_state(int) const;
	int request_state(int) const;
public:
	NCTransaction * get_transaction_slot(void);
	void transaction_finished(NCTransaction *);
	NCResult * get_transaction_result(NCTransaction *);
	int get_transaction_state(NCTransaction *);

	int server_count(void) const { return num_servers; }
	int request_count(void) const { return num_requests; }
	int done_request_count(void) const { return done_requests; }
private:
	int init_flag;
	int max_servers;
	int max_requests;
	int num_servers;
	int num_requests;
	int max_request_id;
	int done_requests;
	ListObject<NCTransaction> free_list;
	ListObject<NCTransaction> done_list;
	NCServerInfo *server_list;
	NCTransaction *trans_list;
public:
	char *buf;
};

class NCServerInfo :
	private TimerObject
{
public:
	NCServerInfo(void);
	~NCServerInfo(void);
	void do_init(NCServer *, int, int);

	/* prepare wake TimerNotify */
	void mark_as_ready() { attach_ready_timer(owner); }
	virtual void job_timer_procedure(void);
	/* four reason server has more work to do */
	/* more transaction attached */
	void more_request_and_ready(void) { req_wait++; mark_as_ready(); }
	/* more close connection, should reconnecting */
	void more_closed_connection_and_ready(void) { conn_remain++; mark_as_ready(); }
	/* more idle connection available */
	void connection_idle_and_ready(void) { mark_as_ready(); }
	/* more request can assign to idle pool */
	void request_done_and_ready(int oldstate);

	/* one request scheduled to SEND state */
	void request_scheduled(void) { req_remain--; req_send++; }
	void request_sent(void) { req_send--; req_recv++; }

	/* queue transaction to this server */
	void queue_request(NCTransaction *req)
	{
		req->ListAddTail(&req_list);
		more_request_and_ready();
	}
	/* one connecting aborted */
	void connecting_failed(void) { conn_error++; }
	void connecting_done(void) { conn_connecting--; }

	/* abort all waiting transactions */
	void abort_wait_queue(int err);

	int count_request_state(int type) const;

	/* get a waiting transaction */
	NCTransaction * get_request_from_queue(void)
	{
		NCTransaction *trans = req_list.NextOwner();
		trans->list_del();
		req_wait--;
		return trans;
	}

private:
	int Connect(void);
	ListObject<NCTransaction> req_list;

public: // constant member declare as public
	/* associated ServerPool */
	NCPool *owner;
	/* basic server info */
	NCServer *info;
	TimerList *timerList;
	int mode; // 0--TCP 1--ASYNC 2--UDP
	/* total connection */
	int connTotal;

private:// transient member is private

	/* transaction in state WAIT */
	int req_wait;
	/* transaction in state SEND */
	int req_send;
	/* transaction in state RECV */
	int req_recv;
	/* remain requests can assign to connection pool */
	int req_remain;

	/* remain connections can connect */
	int conn_remain;
	/* number connections in connecting state */
	int conn_connecting;
	/* number of connect error this round */
	int conn_error;
	/* busy connection count */
	inline int conn_working(void) const { return connTotal - conn_connecting - conn_remain; }
public: //except idle_list for code conventional
	/* idle connection list */
	NCConnectionList idle_list;
	/* busy connection list SEND,RECV,LINGER */
	NCConnectionList busy_list;
};

inline int NCConnection::is_dgram(void) const { return server_info->mode==2; }
inline int NCConnection::is_async(void) const { return server_info->mode; }

#endif
