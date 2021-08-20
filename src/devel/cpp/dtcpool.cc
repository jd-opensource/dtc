#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <linux/sockios.h>

#include "socket/unix_socket.h"
#include <fcntl.h>
#include <sys/poll.h>
#include <errno.h>
#include <netinet/in.h>
#include "poll/myepoll.h"

#include "dtcpool.h"

// must be bitwise
enum {
	RS_IDLE = 0,
	RS_WAIT = 1,
	RS_SEND = 2,
	RS_RECV = 4,
	RS_DONE = 8,
};

enum {
	SS_CONNECTED = 0,
	SS_LINGER = 1,
	SS_CLOSED = 1,
	SS_CONNECTING = 2,
};

#define MAXREQID 2147483646L

NCPool::NCPool(int ms, int mr)
	:
	EpollOperation(1024+16)
{
	int i;

	if(ms > (1<<20))
		ms = 1<<20;
	if(mr > (1<<20))
		mr = 1<<20;

	max_servers = ms;
	max_requests = mr;
	max_request_id = MAXREQID / max_requests;

	server_list = new NCServerInfo[max_servers];
	// needn't zero init
	num_servers = 0;

	trans_list = new NCTransaction[max_requests];
	for(i=0; i<max_requests; i++)
	{
		trans_list[i].ListAdd(&free_list);;
	}
	num_requests = 0;

	done_requests = 0;

	init_flag = -1;
	buf = NULL;
}

NCPool::~NCPool()
{
	int i;

	DELETE_ARRAY(server_list);
	if(trans_list)
	{
		for(i=0; i<max_requests; i++)
		{
		}

		DELETE_ARRAY(trans_list);
	}
	DELETE_ARRAY(buf);
}

/* add server to pool */
int NCPool::add_server(NCServer *server, int mReq, int mConn)
{
	if(server->owner_pool_ != NULL)
		return -EINVAL;

	if(num_servers >= max_servers)
		return -E2BIG;

	if(server->is_dgram())
	{
		if(mConn == 0)
			mConn = 1;
		else if(mConn != 1)
			return -EINVAL;
	} else {
		if(mConn == 0)
			mConn = mReq;
		else if(mConn > mReq)
			return -EINVAL;
	}

	server->set_owner(this, num_servers);
	server_list[num_servers].do_init(server, mReq, mConn);
	num_servers++;
	return 0;
}

/* switch basic server to async mode */
void NCServer::set_owner(NCPool *owner, int id)
{
	Close();
	owner_id_ = id;;
	owner_pool_ = owner;
	increase();
}

/* async connect, high level */
int NCServer::async_connect(int &netfd)
{
	int err = -EC_NOT_INITIALIZED;
	netfd = -1;

	if(addr_.socket_family() != 0) {
		netfd = addr_.create_socket();
		if(netfd < 0) {
			err = -errno;
		} else if(addr_.socket_family()==AF_UNIX && is_dgram() && bind_temp_unix_socket() < 0) {
			err = -errno;
			close(netfd);
			netfd = -1;
		} else {
			fcntl(netfd, F_SETFL, O_RDWR|O_NONBLOCK);
			if(addr_.connect_socket(netfd)==0)
				return 0;
			err = -errno;
			if(err!=-EINPROGRESS) 
			{
				close(netfd);
				netfd = -1;
			}
		}
	}
	return err;
}

NCConnection::NCConnection(NCPool *owner, NCServerInfo *si)
	:
	EpollBase(owner),
	timer(this)
{
	server_info = si;
	sreq = NULL;
	state = SS_CLOSED;
	result = NULL;
}

NCConnection::~NCConnection(void)
{
	if(state==SS_CONNECTING)
		/* decrease connecting count */
		server_info->connecting_done();
	/* remove partial result */
	DELETE(result);
	/* remove from all connection queue */
	list_del();
	/* abort all associated requests */
	abort_requests(-EC_REQUEST_ABORTED);
	server_info->more_closed_connection_and_ready();
}

/* connection is idle again */
void NCConnection::switch_to_idle(void)
{
	if(state==SS_CONNECTING)
		/* decrease connecting count */
		server_info->connecting_done();
	ListMove(&server_info->idle_list);
	server_info->connection_idle_and_ready();
	state = SS_CONNECTED;
	disable_output();
	enable_input();
	timer.disable_timer();
	/* ApplyEvnets by caller */
}

/* prepare connecting state, wait for EPOLLOUT */
void NCConnection::switch_to_connecting(void)
{
	state = SS_CONNECTING;
	timer.attach_timer(server_info->timerList);
	ListMove(&server_info->busy_list);
	disable_input();
	enable_output();
	// no apply events, following AttachPoller will do it */
}

/* try async connecting */
int NCConnection::Connect(void)
{
	int err = server_info->info->async_connect(netfd);
	if(err == 0) {
		switch_to_idle();
		/* treat epollsize overflow as fd overflow */
		if(attach_poller() < 0)
			return -EMFILE;
		/* AttachPoller will ApplyEvents automatically */
	} else if(err == -EINPROGRESS) {
		switch_to_connecting();
		/* treat epollsize overflow as fd overflow */
		if(attach_poller() < 0)
			return -EMFILE;
		/* AttachPoller will ApplyEvents automatically */
	}

	return err;
}

/* Link connection & transaction */
void NCConnection::process_request(NCTransaction *r)
{
	/* linkage between connection & transaction */
	sreq = r;
	sreq->attach_connection(this);

	/* adjust server connection statistics */
	list_move_tail(&server_info->busy_list);
	server_info->request_scheduled();

	/* initial timing and flushing */
	timer.attach_timer(server_info->timerList);
	send_request();
}

/* abort all requests associated transaction */
void NCConnection::abort_requests(int err)
{
	if(sreq)
	{
		sreq->Abort(err);
		sreq = NULL;
	}
	while(req_list.ListEmpty()==0)
	{
		NCTransaction *req = req_list.NextOwner();
		req->Abort(err);
	}
}

/* abort sending transaction, linger connection for RECV request */
void NCConnection::abort_send_side(int err)
{
	if(sreq)
	{
		sreq->Abort(err);
		sreq = NULL;
	}
	if(!is_async() || req_list.ListEmpty())
		Close(err);
	else
	{
		// async connection, lingering
		ListMove(&server_info->busy_list);
		state = SS_LINGER;
		timer.attach_timer(server_info->timerList);
	}
}

/* close a connection */
void NCConnection::Close(int err)
{
	abort_requests(err);
	if(is_dgram()==0)
		delete this;
	/* UDP has not connection close */
}

/* a valid result received */
void NCConnection::done_result(void)
{
	if(result)
	{
		/* searching SN */
		ListObject<NCTransaction> *reqPtr = req_list.ListNext();
		while(reqPtr != &req_list)
		{
			NCTransaction *req = reqPtr->ListOwner();
			if(req->MatchSN(result))
			{
				/* SN matched, transaction is dont */
				req->done(result);
				result = NULL;
				break;
			}
			reqPtr = reqPtr->ListNext();
		}
		DELETE(result);
	}
	if(is_async()==0) {
		/*
		 * SYNC server, switch to idle state,
		 * ASYNC server always idle, no switch needed
		 */
		switch_to_idle();
		apply_events();
	} else if(state == SS_LINGER) {
		/* close LINGER connection if all result done */
		if(req_list.ListEmpty())
			delete this;
	}
	/*
	// disabled because async server should trigger by req->done
	else
		server_info->mark_as_ready();
		*/
}

/* hangup by recv zero bytes */
int NCConnection::check_hangup(void)
{
	if(is_async())
		return 0;
	char buf[1];
	int n = recv(netfd, buf, sizeof(buf), MSG_DONTWAIT|MSG_PEEK);
	return n >= 0;
}

/*
 * sending result, may called by other component
 * apply events is required
 */
void NCConnection::send_request(void)
{
	int ret = sreq->Send(netfd);
	timer.disable_timer();
	switch (ret)
	{
		case SendResultMoreData:
			/* more data to send, enable EPOLLOUT and timer */
			enable_output();
			apply_events();
			timer.attach_timer(server_info->timerList);
			break;

		case SendResultDone:
			/* send OK, disable output and enable receiving */
			disable_output();
			enable_input();
			sreq->send_ok(&req_list);
			sreq = NULL;
			server_info->request_sent();
			/* fire up receiving timer */
			timer.attach_timer(server_info->timerList);
			if(is_async())
			{
				list_move_tail(&server_info->idle_list); 
				server_info->connection_idle_and_ready();
			}
#if 0
			else {
				/* fire up receiving logic */
				recv_result();
			}
#endif
			apply_events();
			break;

		default:
			abort_send_side(-ECONNRESET);
			break;
	}
}

int NCConnection::recv_result(void)
{
	int ret;
	if(result==NULL)
	{
		result = new NCResult(server_info->info->table_definition_);
		receiver.attach(netfd);
		receiver.erase();
	}
	if(is_dgram())
	{
		if(server_info->owner->buf==NULL)
			server_info->owner->buf = new char[65536];
		ret = recv(netfd, server_info->owner->buf, 65536, 0);
		if(ret <= 0)
			ret = DecodeFatalError;
		else
			ret = result->do_decode(server_info->owner->buf, ret);
	} else
		ret = result->do_decode(receiver);
	timer.disable_timer();
	switch (ret)
	{
		default:
		case DecodeFatalError:
			Close(-ECONNRESET);
			return -1; // connection bad
			break;

		case DecodeDataError:
			done_result();
			// more result maybe available
			ret = 1;
			break;

		case DecodeIdle:
		case DecodeWaitData:
			// partial or no result available yet
			ret = 0;
			break;
			
		case DecodeDone:
			server_info->info->save_definition(result);
			done_result();
			// more result maybe available
			ret = 1;
			break;
	}
	if(sreq || !req_list.ListEmpty())
		timer.attach_timer(server_info->timerList);
	return ret;
}

void NCConnection::input_procedure(void)
{
	switch(state) {
		case SS_CONNECTED:
			while(1) {
				if(recv_result() <= 0)
					break;
			}
			break;
		case SS_LINGER:
			while(req_list.ListEmpty()==0) {
				if(recv_result() <= 0)
					break;
			}
		default:
			break;
	}
}

void NCConnection::output_procedure(void)
{
	switch(state)
	{
	case SS_CONNECTING:
		switch_to_idle();
		break;
	case SS_CONNECTED:
		send_request();
		break;
	default:
		disable_output();
		break;
	}
}

void NCConnection::hang_procedure(void)
{
	if(state==SS_CONNECTING) {
		server_info->connecting_failed();
	}
	abort_requests(-ECONNRESET);
	delete this;
}

void NCConnection::job_timer_procedure(void)
{
	if(sreq || !req_list.ListEmpty())
	{
		Close(-ETIMEDOUT);
	}
}

NCServerInfo::NCServerInfo(void)
{
	info = NULL;
}

NCServerInfo::~NCServerInfo(void)
{
	while(!idle_list.ListEmpty())
	{
		NCConnection *conn = idle_list.NextOwner();
		delete conn;
	}

	while(!busy_list.ListEmpty())
	{
		NCConnection *conn = busy_list.NextOwner();
		delete conn;
	}

	while(!req_list.ListEmpty())
	{
		NCTransaction *trans = req_list.NextOwner();
		trans->Abort(-EC_REQUEST_ABORTED);
	}

	if(info && info->decrease()==0)
		delete info;
}

void NCServerInfo::do_init(NCServer *server, int maxReq, int maxConn)
{
	info = server;
	req_list.ResetList();
	idle_list.ResetList();
	busy_list.ResetList();

	req_wait = 0;
	req_send = 0;
	req_recv = 0;
	req_remain = maxReq;

	conn_remain = maxConn;
	connTotal = maxConn;
	conn_connecting = 0;
	conn_error = 0;
	owner = server->owner_pool_;
	int to = server->get_timeout();
	if(to <= 0) to = 3600 * 1000; // 1 hour
	timerList = owner->get_timer_list_by_m_seconds( to );

	mode = server->is_dgram() ? 2 : maxReq==maxConn ? 0 : 1;
}

int NCServerInfo::Connect(void)
{
	NCConnection *conn = new NCConnection(owner, this);
	conn_remain--;
	int err = conn->Connect();
	if(err==0) {
		/* pass */;
	} else if(err==-EINPROGRESS) {
		conn_connecting++;
		err = 0; /* NOT a ERROR */
	} else {
		conn_remain++;
		delete conn;
		conn_error++;
	}
	return err;
}

void NCServerInfo::abort_wait_queue(int err)
{
	while(!req_list.ListEmpty())
	{
		NCTransaction *trans = req_list.NextOwner();
		trans->Abort(err);
	}
}

void NCServerInfo::job_timer_procedure()
{
	while(req_remain>0 && !req_list.ListEmpty())
	{
		if(!idle_list.ListEmpty())
		{
			// has something to do
			NCConnection *conn = idle_list.NextOwner();

			conn->process_request(get_request_from_queue());
		} else {
			// NO connection available
			if(conn_remain == 0)
				break;
			// need more connection to process
			if(conn_connecting >= req_wait)
				break;

			int err;
			// connect error, abort processing
			if((err=Connect()) != 0)
			{
				if(conn_remain>=connTotal)
					abort_wait_queue(err);
				break;
			}
		}
	}
	/* no more work, clear bogus ready timer */
	TimerObject::disable_timer();
	/* reset connect error count */
	conn_error = 0;
}

void NCServerInfo::request_done_and_ready(int state) {
	switch(state) {
		case RS_WAIT:
			req_wait--; // Abort a queued not really ready
			break;
		case RS_SEND:
			req_send--;
			req_remain++;
			mark_as_ready();
			break;
		case RS_RECV:
			req_recv--;
			req_remain++;
			mark_as_ready();
			break;
	}
}

NCTransaction::NCTransaction(void)
{
}

NCTransaction::~NCTransaction(void)
{
	DELETE(packet);
	DELETE(result);
}

/* clear transaction state */
void NCTransaction::Clear(void)
{
	/* detach from list, mostly free_list */
	ListObject<NCTransaction>::ResetList();

	/* increase reqId */
	genId++;

	/* reset local state */
	state = RS_IDLE;
	reqTag = 0;;
	server = NULL;
	packet = NULL;
	result = NULL;
}

/* get transaction result */
NCResult * NCTransaction::get_result(void) {
	NCResult *ret = result;
	ret->set_api_tag(reqTag);
	DELETE(packet);
	Clear();
	return ret;
}

/* attach new request to transaction slot */
int NCTransaction::attach_request(NCServerInfo *info, long long tag, NCRequest *req, DTCValue *key) {
	state = RS_WAIT;
	reqTag = tag;
	server = info;
	packet = new Packet;
	int ret = req->encode(key, packet);
	if(ret < 0) {
		/* encode error */
		result = new NCResult(ret, "API::encoding", "client encode packet error");
	} else {
		SN = server->info->get_last_serialnr();
	}
	return ret;
}

extern inline void NCTransaction::attach_connection(NCConnection *c) {
	state = RS_SEND;
	conn = c;
}

extern inline void NCTransaction::send_ok(ListObject<NCTransaction> *queue) {
	state = RS_RECV;
	ListAddTail(queue);
}

extern inline void NCTransaction::recv_ok(ListObject<NCTransaction> *queue) {
	state = RS_DONE;
	ListAddTail(queue);
}

/* abort current transaction, zero means succ */
void NCTransaction::Abort(int err)
{
	if(server == NULL) // empty slot or done
		return;

	//Don't reverse abort connection, just clear linkage
	//NCConnection *c = conn;
	//DELETE(c);
	conn = NULL;

	NCPool *owner = server->owner;
	list_del();

	server->request_done_and_ready(state);
	server = NULL;

	if(err)
	{
		result = new NCResult(err, "API::aborted", "client abort request");
	}
	
	owner->transaction_finished(this);
}

NCResult *NCPool::get_transaction_result(NCTransaction *req)
{
	NCResult *ret = req->get_result();
	done_requests--;
	num_requests--;
	req->ListAdd(&free_list);
	return ret;
}

/* get transaction slot for new request */
NCTransaction * NCPool::get_transaction_slot(void)
{
	NCTransaction *trans = free_list.NextOwner();
	trans->Clear();
	trans->round_gen_id(max_request_id);
	num_requests ++;
	return trans;
}

void NCPool::transaction_finished(NCTransaction *trans)
{
	trans->recv_ok(&done_list);
	done_requests++;
}

int NCPool::add_request(NCRequest *req, long long tag, DTCValue *key)
{
	int ret;
	if(num_requests >= max_requests)
	{
		return -E2BIG;
	}

	if(req->server == NULL)
	{
	    return -EC_NOT_INITIALIZED;
	}

	NCServer *server = req->server;
	if(server->owner_pool_ == NULL)
	{
		ret = add_server(server, 1);
		if(ret < 0)
			return ret;
	}

	if(server->owner_pool_ != this)
	{
		return -EINVAL;
	}

	if(key==NULL && req->haskey) key = &req->key;

	NCTransaction *trans = get_transaction_slot();
	NCServerInfo *info = &server_list[server->owner_id_];

	if(trans->attach_request(info, tag, req, key) < 0)
	{
		// attach error, result already prepared */
		transaction_finished(trans);
	} else {
		info->queue_request(trans);
	}

	return (trans-trans_list) + trans->GenId() * max_requests + 1;
}

void NCPool::execute_one_loop(int timeout)
{
	wait_poller_events(timeout);
	uint64_t now = GET_TIMESTAMP();
	process_poller_events();
	check_expired(now);
	check_ready();
}

int NCPool::do_execute(int timeout)
{
	if(timeout > 3600000) timeout = 3600000;
	initialize_poller_unit();
	execute_one_loop(0);
	if(done_requests > 0)
		return done_requests;
	int64_t till = GET_TIMESTAMP() + timeout * TIMESTAMP_PRECISION / 1000;
	while(done_requests <= 0)
	{
		int64_t exp = till - GET_TIMESTAMP();
		if(exp < 0)
			break;
		int msec;
#if TIMESTAMP_PRECISION > 1000
		msec = exp / (TIMESTAMP_PRECISION/1000);
#else
		msec = exp * 1000/TIMESTAMP_PRECISION;
#endif
		execute_one_loop(expire_micro_seconds(msec, 1));
	}
	return done_requests;
}

int NCPool::execute_all(int timeout)
{
	if(timeout > 3600000) timeout = 3600000;
	initialize_poller_unit();
	execute_one_loop(0);
	if(num_requests <= done_requests)
		return done_requests;
	int64_t till = GET_TIMESTAMP() + timeout * TIMESTAMP_PRECISION / 1000;
	while(num_requests > done_requests)
	{
		int64_t exp = till - GET_TIMESTAMP();
		if(exp < 0)
			break;
		int msec;
#if TIMESTAMP_PRECISION > 1000
		msec = exp / (TIMESTAMP_PRECISION/1000);
#else
		msec = exp * 1000/TIMESTAMP_PRECISION;
#endif
		execute_one_loop(expire_micro_seconds(msec, 1));
	}
	return done_requests;
}

int NCPool::initialize_poller_unit(void)
{
	if(init_flag==-1)
	{
		init_flag = EpollOperation::initialize_poller_unit() >= 0;
	}
	return init_flag;
}

int NCPool::get_epoll_fd(int mp)
{
	if(init_flag == -1 && mp > EpollOperation::get_max_pollers())
		EpollOperation::set_max_pollers(mp);
	initialize_poller_unit();
	return EpollOperation::get_fd();
}

NCTransaction * NCPool::Id2Req(int reqId) const
{
	if(reqId <= 0 || reqId > MAXREQID)
		return NULL;
	reqId--;
	NCTransaction *req = &trans_list[reqId % max_requests];
	if(reqId / max_requests != req->GenId())
		return NULL;
	return req;
}


int NCPool::abort_request(NCTransaction *req)
{
	if(req->conn)
	{
		/* send or recv state */
		if(req->server->mode==0)
		{	// SYNC, always close connecction
			req->conn->Close(-EC_REQUEST_ABORTED);
		} else if(req->server->mode==1)
		{	// ASYNC, linger connection for other result, abort send side
			if(req->State()==RS_SEND)
				req->conn->abort_send_side(-EC_REQUEST_ABORTED);
			else 
				req->Abort(-EC_REQUEST_ABORTED);
		} else { // UDP, abort request only
			req->Abort(-EC_REQUEST_ABORTED);
		}
		return 1;
	}
	else if(req->server)
	{
		/* waiting state, abort request only */
		req->Abort(-EC_REQUEST_ABORTED);
		return 1;
	}
	return 0;
}

int NCPool::cancel_request(int reqId)
{
	NCTransaction *req = Id2Req(reqId);
	if(req==NULL)
		return -EINVAL;

	abort_request(req);
	NCResult *res = get_transaction_result(req);
	DELETE(res);
	return 1;
}

int NCPool::cancel_all_request(int type)
{
	int n = 0;
	if((type & ~0xf) != 0)
		return -EINVAL;
	for(int i=0; i<max_requests; i++)
	{
		NCTransaction *req = &trans_list[i];
		if((type & req->State()))
		{
			abort_request(req);
			NCResult *res = get_transaction_result(req);
			DELETE(res);
			n++;
		}
	}
	return n;
}

int NCPool::abort_request(int reqId)
{
	NCTransaction *req = Id2Req(reqId);
	if(req==NULL)
		return -EINVAL;

	return abort_request(req);
}

int NCPool::abort_all_request(int type)
{
	int n = 0;
	if((type & ~0xf) != 0)
		return -EINVAL;
	type &= ~RS_DONE;
	for(int i=0; i<max_requests; i++)
	{
		NCTransaction *req = &trans_list[i];
		if((type & req->State()))
		{
			abort_request(req);
			n++;
		}
	}
	return n;
}

NCResult *NCPool::get_result(int reqId)
{
	NCTransaction *req;
	if(reqId==0)
	{
		if(done_list.ListEmpty())
			return (NCResult *)-EAGAIN;
		req = done_list.NextOwner();
	} else {
		req = Id2Req(reqId);
		if(req==NULL) return (NCResult *)-EINVAL;

		if(req->State() != RS_DONE)
			return (NCResult *)((long)req->State());
	}
	return get_transaction_result(req);
}

int NCServerInfo::count_request_state(int type) const
{
	int n = 0;
	if((type & RS_WAIT))
		n += req_wait;
	if((type & RS_SEND))
		n += req_send;
	if((type & RS_RECV))
		n += req_recv;
	return n;
}

int NCPool::count_request_state(int type) const
{
	int n = 0;
#if 0
	for(int i=0; i<max_requests; i++)
	{
		if((type & trans_list[i].State()))
			n++;
	}
#else
	for(int i=0; i<max_servers; i++)
	{
		if(server_list[i].info==NULL)
			continue;
		n += server_list[i].count_request_state(type);
	}
	if((type & RS_DONE))
		n += done_requests;
#endif
	return n;
}

int NCPool::request_state(int reqId) const
{
	NCTransaction *req = Id2Req(reqId);
	if(req==NULL)
		return -EINVAL;

	return req->State();
}

