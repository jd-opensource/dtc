#include <inttypes.h>
#include <value.h>
#include <compiler.h>

#include "dtcapi.h"
#include "dtcpool.h"

using namespace DTC;

#define CAST(type, var) type *var = (type *)addr_
#define CAST2(type, var, src) type *var = (type *)src->addr_
#define CAST3(type, var, src) type *var = (type *)src.addr_

// WRAPPER for Server
__EXPORT
ServerPool::ServerPool(int ms, int mr) {
	NCPool *pl = new NCPool(ms, mr);
	addr_ = pl;
}

__EXPORT
ServerPool::~ServerPool(void) {
	CAST(NCPool, pl);
	delete pl;
}

__EXPORT
int ServerPool::add_server(Server *srv, int mreq, int mconn)
{
	CAST(NCPool, pl);
	CAST2(NCServer, s, srv);

	return pl->add_server(s, mreq, mconn);
}

__EXPORT
int ServerPool::add_request(Request *req, long long tag)
{
	CAST(NCPool, pl);
	CAST2(NCRequest, r, req);

	if(r->server == NULL)
	    return -EC_NOT_INITIALIZED;

	return pl->add_request(r, tag, NULL);
}

__EXPORT
int ServerPool::add_request(Request *req, void *tag)
{
	return add_request(req, (long long)(long)tag);
}

__EXPORT
int ServerPool::add_request(Request *req, long long tag, long long k)
{
	CAST(NCPool, pl);
	CAST2(NCRequest, r, req);

	if(r->server == NULL)
	    return -EC_NOT_INITIALIZED;
	if(r->server->keytype_ != DField::Signed)
	    return -EC_BAD_KEY_TYPE;

	DTCValue v(k);
	return pl->add_request(r, tag, &v);
}

__EXPORT
int ServerPool::add_request(Request *req, void *tag, long long k)
{
	return add_request(req, (long long)(long)tag, k);
}

__EXPORT
int ServerPool::add_request(Request *req, long long tag, const char *k)
{
	CAST(NCPool, pl);
	CAST2(NCRequest, r, req);

	if(r->server == NULL)
	    return -EC_NOT_INITIALIZED;
	if(r->server->keytype_ != DField::String)
	    return -EC_BAD_KEY_TYPE;

	DTCValue v(k);
	return pl->add_request(r, tag, &v);
}

__EXPORT
int ServerPool::add_request(Request *req, void *tag, const char *k)
{
	return add_request(req, (long long)(long)tag, k);
}

__EXPORT
int ServerPool::add_request(Request *req, long long tag, const char *k, int l)
{
	CAST(NCPool, pl);
	CAST2(NCRequest, r, req);

	if(r->server == NULL)
	    return -EC_NOT_INITIALIZED;
	if(r->server->keytype_ != DField::String)
	    return -EC_BAD_KEY_TYPE;

	DTCValue v(k, l);
	return pl->add_request(r, tag, &v);
}

__EXPORT
int ServerPool::add_request(Request *req, void *tag, const char *k, int l)
{
	return add_request(req, (long long)(long)tag, k, l);
}

__EXPORT
int ServerPool::do_execute(int msec)
{
	CAST(NCPool, pl);
	return pl->do_execute(msec);
}

__EXPORT
int ServerPool::execute_all(int msec)
{
	CAST(NCPool, pl);
	return pl->execute_all(msec);
}

__EXPORT
int ServerPool::cancel_request(int id)
{
	CAST(NCPool, pl);
	return pl->cancel_request(id);
}

__EXPORT
int ServerPool::cancel_all_request(int type)
{
	CAST(NCPool, pl);
	return pl->cancel_all_request(type);
}

__EXPORT
int ServerPool::abort_request(int id)
{
	CAST(NCPool, pl);
	return pl->abort_request(id);
}

__EXPORT
int ServerPool::abort_all_request(int type)
{
	CAST(NCPool, pl);
	return pl->abort_all_request(type);
}

__EXPORT
Result * ServerPool::get_result(void)
{
	return get_result(0);
}

__EXPORT
Result * ServerPool::get_result(int id)
{
	CAST(NCPool, pl);
	NCResult *a = pl->get_result(id);
	if( (long)a  < 4096 && (long)a > -4095)
		return NULL;
	Result *s = new Result();
	s->addr_ = (void *)a;
	return s;
}

__EXPORT
int ServerPool::get_result(Result &s)
{
	return get_result(s, 0);
}

__EXPORT
int ServerPool::get_result(Result &s, int id)
{
	CAST(NCPool, pl);
	s.Reset();
	NCResult *a = pl->get_result(id);
	long iv = (long)a;
	if( iv  < 4096 && iv > -4095)
	{
		s.addr_ = (void *)(new NCResult(iv, "API::executing", iv>0 ? "Request not completed" : id ? "Invalid request" : "No more result"));
		return iv;
	}
	s.addr_ = (void *)a;
	return 0;
}

__EXPORT
int ServerPool::server_count(void) const
{
	CAST(NCPool, pl);
	return pl->server_count();
}

__EXPORT
int ServerPool::request_count(int type) const
{
	CAST(NCPool, pl);
	if(type == DONE)
		return pl->done_request_count();
	if(type == (WAIT|SEND|RECV|DONE))
		return pl->request_count();
	return pl->count_request_state(type);
}

__EXPORT
int ServerPool::request_state(int reqId) const
{
	CAST(NCPool, pl);
	return pl->request_state(reqId);
}

__EXPORT
int ServerPool::get_epoll_fd(int size)
{
    CAST(NCPool, pl);
    return pl->get_epoll_fd(size);
}

const int ALL_STATE = WAIT|SEND|RECV|DONE;
