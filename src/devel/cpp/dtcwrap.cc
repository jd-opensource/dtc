#include <stdlib.h>
#include <stdint.h>
#include <new>

#include "dtcapi.h"
#include "dtcint.h"
#include <string.h>
#include <protocol.h>
#include "compiler.h"


using namespace DTC;

#define CAST(type, var) type *var = (type *)addr_
#define CAST2(type, var, src) type *var = (type *)src->addr_
#define CAST2Z(type, var, src) type *var = (type *)((src)?(src)->addr_:0)
#define CAST3(type, var, src) type *var = (type *)src.addr_



__EXPORT
int DTC::set_key_value_max(unsigned int count)
{
	if(count >= 8 && count <= 10000) {
		NCKeyValueList::key_value_max_ = count;
	} else  {
		return -EINVAL;
	}
	return 0;
};

/* WRAPPER for Server */
__EXPORT
Server::Server(void) 
{
	NCServer *srv = new NCServer;
	srv->increase();
	addr_ = srv;
}

__EXPORT
Server::Server(const Server &that) 
{
	CAST3(NCServer, old, that);
	NCServer *srv = new NCServer(*old);
	srv->increase();
	addr_ = srv;
}

__EXPORT
Server::~Server(void) 
{
	CAST(NCServer, srv);  
	DEC_DELETE(srv);
}

__EXPORT
int Server::set_address(const char *host, const char *port) 
{
	CAST(NCServer, srv);
	return srv->set_address(host, port);
}

__EXPORT
int Server::set_table_name(const char *name) 
{
	CAST(NCServer, srv);
	return srv->set_table_name(name);
}

__EXPORT
void Server::set_compress_level(int level) 
{
	CAST(NCServer, srv);
	srv->set_compress_level(level);
}

__EXPORT
const char * Server::get_address(void) const
{
	CAST(NCServer, srv);
	return srv ? srv->get_address() : NULL;
}

__EXPORT
const char * Server::get_table_name(void) const 
{
	CAST(NCServer, srv);
	return srv ? srv->get_table_name() : NULL;
}

__EXPORT
const char * Server::get_server_address(void) const 
{
	CAST(NCServer, srv);
	return srv ? srv->get_server_address() : NULL;
}

__EXPORT
const char * Server::get_server_table_name(void) const 
{
	CAST(NCServer, srv);
	return srv ? srv->get_server_table_name() : NULL;
}
__EXPORT
void Server::clone_table_define(const Server& source)
{
	CAST(NCServer, srv);
	CAST3(NCServer, ssrv, source);
	srv->clone_table_define(*ssrv);
}

__EXPORT
int Server::int_key(void) 
{
	CAST(NCServer, srv);
	return srv->int_key();
}

__EXPORT
int Server::string_key(void) 
{
	CAST(NCServer, srv);
	return srv->string_key();
}

__EXPORT
int Server::binary_key(void) 
{
	CAST(NCServer, srv);
	return srv->string_key();
}

__EXPORT
int Server::add_key(const char* name, int type) 
{
	CAST(NCServer, srv);
	return srv->add_key(name, (uint8_t)type);
}

__EXPORT
int Server::get_field_type(const char* name) 
{
	CAST(NCServer, srv);
	return srv->get_field_type(name);
}

__EXPORT
const char * Server::get_error_message(void) const 
{
	CAST(NCServer, srv);
	return srv->get_error_message();
}

/* fixed, sec ---> msec */
__EXPORT
void Server::set_timeout(int n) 
{
	CAST(NCServer, srv);
	return srv->set_timeout(n);
}

__EXPORT
void Server::SetTimeout(int n) 
{
	CAST(NCServer, srv);
	return srv->set_timeout(n*1000);
}

__EXPORT
int Server::Connect(void) 
{
	CAST(NCServer, srv);
	return srv->Connect();
}

__EXPORT
void Server::Close(void) 
{
	CAST(NCServer, srv);
	return srv->Close();
}

__EXPORT
int Server::ping(void) 
{
	CAST(NCServer, srv);
	return srv->ping();
}

__EXPORT
void Server::auto_ping(void) 
{
	CAST(NCServer, srv);
	return srv->auto_ping();
}

__EXPORT
void Server::set_fd(int fd) 
{
	CAST(NCServer, srv);
	srv->set_fd(fd);
}

__EXPORT
void Server::set_auto_Update_table(bool autoUpdate)
{
	CAST(NCServer, srv);
	srv->set_auto_Update_table(autoUpdate);
}

__EXPORT
void Server::set_auto_reconnect(int auto_reconnect)
{
	CAST(NCServer, srv);
	srv->set_auto_reconnect(auto_reconnect);
}

__EXPORT
void Server::set_accesskey(const char *token)
{
	CAST(NCServer, srv);
	srv->set_accesskey(token);
}

/* QOS ADD BY NEOLV */
__EXPORT
void Server::increase_error_count_()
{
	CAST(NCServer, srv);
	srv->increase_error_count_();
}

__EXPORT
uint64_t Server::get_error_count()
{
	CAST(NCServer, srv);
	return srv->get_error_count();
}
__EXPORT
void Server::clear_error_count()
{
	CAST(NCServer, srv);
	srv->clear_error_count();
}

__EXPORT
void Server::increase_remove_count()
{
	CAST(NCServer, srv);
	srv->increase_remove_count();
}

__EXPORT
int Server::get_remove_count()
{
	CAST(NCServer, srv);
	return srv->get_remove_count();
}

__EXPORT
void Server::clear_remove_count()
{
	CAST(NCServer, srv);
	srv->clear_remove_count();
}
__EXPORT
void Server::increase_total_request()
{
	CAST(NCServer, srv);
	srv->increase_total_request();
}
__EXPORT
uint64_t Server::get_total_request()
{
	CAST(NCServer, srv);
	return srv->get_total_request();
}
__EXPORT
void Server::clear_total_request()
{
	CAST(NCServer, srv);
	srv->clear_total_request();
}

__EXPORT
void Server::add_total_elaps(uint64_t iElaps)
{
	CAST(NCServer, srv);
	srv->add_total_elaps(iElaps);
}
__EXPORT
uint64_t Server::get_total_elaps()
{
	CAST(NCServer, srv);
	return srv->get_total_elaps();
}
__EXPORT
void Server::clear_total_elaps()
{
	CAST(NCServer, srv);
	srv->clear_total_elaps();
}

/* WRAPER for Request */

__EXPORT
Request::Request(Server *srv0, int op) {
	CAST2Z(NCServer, srv, srv0);
	NCRequest *req = new NCRequest(srv, op);
	addr_ = (void *)req;
}

Request::Request()
{
	NCRequest *req = new NCRequest(NULL, 0);
	addr_ = (void *)req;
}

__EXPORT
Request::~Request(void) 
{
	CAST(NCRequest, req);
	delete req;
}

__EXPORT
int Request::attach_server(Server *srv0) 
{
	CAST2Z(NCServer, srv, srv0);
	CAST(NCRequest, r);
	return r->attach_server(srv);
}

__EXPORT
void Request::Reset(void) 
{
	CAST(NCRequest, old);
	if(old) {
		int op = old->cmd;
		NCServer *srv = old->server;
		srv->increase();
		old->~NCRequest();
		memset(old, 0, sizeof(NCRequest));
		new(addr_) NCRequest(srv, op);
		srv->decrease();
	}
}

__EXPORT
void Request::Reset(int op)
{
	CAST(NCRequest, old);
	if (old) {
		NCServer *srv = old->server;
		srv->increase();
		old->~NCRequest();
		memset(old, 0, sizeof(NCRequest));
		new(addr_) NCRequest(srv, op);
		srv->decrease();
	}
}

__EXPORT
void Request::set_admin_code(int code) 
{
	CAST(NCRequest, r);
	r->set_admin_code(code);
}

__EXPORT
void Request::set_hotbackup_id(long long v) 
{
	CAST(NCRequest, r);
	r->set_hotbackup_id(v);
}

__EXPORT
void Request::set_master_hotbackup_timestamp(long long t) 
{
	CAST(NCRequest, r);
	r->set_master_hotbackup_timestamp(t);
}

__EXPORT
void Request::set_slave_hotbackup_timestamp(long long t) 
{
	CAST(NCRequest, r);
	r->set_slave_hotbackup_timestamp(t);
}

__EXPORT
void Request::no_cache(void) 
{
	CAST(NCRequest, r); return r->enable_no_cache();
}

__EXPORT
void Request::no_next_server(void) 
{
	CAST(NCRequest, r); return r->enable_no_next_server();
}

__EXPORT
int Request::need(const char *name) 
{
	CAST(NCRequest, r); return r->need(name, 0);
}

__EXPORT
int Request::need(const char *name, int vid) 
{
	CAST(NCRequest, r); return r->need(name, vid);
}

__EXPORT
int Request::get_field_type(const char *name) 
{
	CAST(NCRequest, r); return r->get_field_type(name);
}

__EXPORT
void Request::limit(unsigned int st, unsigned int cnt) 
{
	CAST(NCRequest, r); return r->limit(st, cnt);
}

__EXPORT
void Request::unset_key(void) 
{
	CAST(NCRequest, r);
	r->unset_key();
	r->unset_key_value();
}

__EXPORT
int Request::set_key(long long key) 
{
	CAST(NCRequest, r); return r->set_key((int64_t)key);
}

__EXPORT
int Request::set_key(const char *key) 
{
	CAST(NCRequest, r); return r->set_key(key, strlen(key));
}

__EXPORT
int Request::set_key(const char *key, int len) 
{
	CAST(NCRequest, r); return r->set_key(key, len);
}

__EXPORT
int Request::add_key_value(const char* name, long long v) 
{
	CAST(NCRequest, r);
	DTCValue key(v);
	return r->add_key_value(name, key, kKeyTypeInt);
}

__EXPORT
int Request::add_key_value(const char* name, const char *v) 
{
	CAST(NCRequest, r);
	DTCValue key(v);
	return r->add_key_value(name, key, kKeyTypeString);
}

__EXPORT
int Request::add_key_value(const char* name, const char *v, int len) 
{
	CAST(NCRequest, r);
	DTCValue key(v, len);
	return r->add_key_value(name, key, kKeyTypeString);
}

__EXPORT
int Request::set_cache_id(long long id)
{
    CAST(NCRequest, r); return r->set_cache_id(id);
}

__EXPORT
Result *Request::do_execute(void) 
{
	Result *s = new Result();
	CAST(NCRequest, r);
	s->addr_ = r->do_execute();
	return s;
}

__EXPORT
Result *Request::do_execute(long long k) 
{
	Result *s = new Result();
	CAST(NCRequest, r);
	s->addr_ = r->do_execute((int64_t)k);
	return s;
}

__EXPORT
Result *Request::do_execute(const char *k) 
{
	Result *s = new Result();
	CAST(NCRequest, r);
	s->addr_ = r->do_execute(k, k?strlen(k):0);
	return s;
}

__EXPORT
Result *Request::do_execute(const char *k, int l) 
{
	Result *s = new Result();
	CAST(NCRequest, r);
	s->addr_ = r->do_execute(k, l);
	return s;
}

__EXPORT
int Request::do_execute(Result &s) 
{
	CAST(NCRequest, r);
	s.Reset();
	s.addr_ = r->do_execute();
	return s.get_result_code();
}

__EXPORT
int Request::do_execute(Result &s, long long k) 
{
	CAST(NCRequest, r);
	s.Reset();
	s.addr_ = r->do_execute((int64_t)k);
	return s.get_result_code();
}

__EXPORT
int Request::do_execute(Result &s, const char *k) 
{
	CAST(NCRequest, r);
	s.Reset();
	s.addr_ = r->do_execute(k, k?strlen(k):0);
	return s.get_result_code();
}

__EXPORT
int Request::do_execute(Result &s, const char *k, int l) 
{
	CAST(NCRequest, r);
	s.Reset();
	s.addr_ = r->do_execute(k, l);
	return s.get_result_code();
}

/* 增加错误信息，方便以后问题定位. add by foryzhou */
__EXPORT
const char * Request::get_error_message(void) const 
{
	CAST(NCRequest, r);
	if(r==NULL) return NULL;
	return r->get_error_message();
}

#define _DECLACT_(f0,t0,f1,f2,t1) \
    __EXPORT \
    int Request::f0(const char *n, t0 v) { \
	CAST(NCRequest, r); \
	return r->f1(n, DField::f2, DField::t1, DTCValue::Make(v)); \
    }
#define _DECLACT2_(f0,t0,f1,f2,t1) \
    __EXPORT \
    int Request::f0(const char *n, t0 v, int l) { \
	CAST(NCRequest, r); \
	return r->f1(n, DField::f2, DField::t1, DTCValue::Make(v,l)); \
    }



_DECLACT_(EQ, long long, add_condition, EQ, Signed);
_DECLACT_(NE, long long, add_condition, NE, Signed);
_DECLACT_(GT, long long, add_condition, GT, Signed);
_DECLACT_(GE, long long, add_condition, GE, Signed);
_DECLACT_(LT, long long, add_condition, LT, Signed);
_DECLACT_(LE, long long, add_condition, LE, Signed);
_DECLACT_(EQ, const char *, add_condition, EQ, String);
_DECLACT_(NE, const char *, add_condition, NE, String);
_DECLACT2_(EQ, const char *, add_condition, EQ, String);
_DECLACT2_(NE, const char *, add_condition, NE, String);
_DECLACT_(Set, long long, add_operation, Set, Signed);
_DECLACT_(OR, long long, add_operation, OR, Signed);
_DECLACT_(Add, long long, add_operation, Add, Signed);

__EXPORT
int Request::Sub(const char *n, long long v) 
{
    CAST(NCRequest, r);
    return r->add_operation(n, DField::Add, DField::Signed, DTCValue::Make(-(int64_t)v));
}

_DECLACT_(Set, double, add_operation, Set, Float);
_DECLACT_(Add, double, add_operation, Add, Float);

__EXPORT
int Request::Sub(const char *n, double v) 
{
    CAST(NCRequest, r);
    return r->add_operation(n, DField::Add, DField::Signed, DTCValue::Make(-v));
}

_DECLACT_(Set, const char *, add_operation, Set, String);
_DECLACT2_(Set, const char *, add_operation, Set, String);
#undef _DECLACT_
#undef _DECLACT2_

__EXPORT
int Request::compress_set(const char *n,const char * v,int len)
{
    CAST(NCRequest, r);
    return r->compress_set(n,v,len);
}

__EXPORT
int Request::compress_set_force(const char *n,const char * v,int len)
{
    CAST(NCRequest, r);
    return r->compress_set_force(n,v,len);
}

__EXPORT
int Request::set_multi_bits(const char *n, int o, int s, unsigned int v) 
{
    if(s <=0 || o >= (1 << 24) || o < 0)
		return 0;
    if(s >= 32)
		s = 32;

    CAST(NCRequest, r);
    /**
     * 1 byte 3 byte 4 byte
	 * s     o      v
     */
    uint64_t t = ((uint64_t)s << 56)+ ((uint64_t)o << 32) + v;
    return r->add_operation(n, DField::SetBits, DField::Unsigned, DTCValue::Make(t));
}

__EXPORT
int Request::set_expire_time(const char* key, int time)
{
	CAST(NCRequest, r);
	return r->set_expire_time(key, time);
}

__EXPORT
int Request::get_expire_time(const char* key)
{
	CAST(NCRequest, r);
	return r->get_expire_time(key);
}


/* WRAPER for Result */
__EXPORT
Result::Result(void) 
{
	addr_ = NULL;
}

__EXPORT
Result::~Result(void) 
{
	Reset();
}

__EXPORT
void Result::set_error(int err, const char *via, const char *msg) 
{
	Reset();
	addr_ = new NCResult(err, via, msg);
}

__EXPORT
void Result::Reset(void) 
{
	if(addr_ != NULL) {
		CAST(NCResult, job);
		CAST(NCResultInternal, itask);
		if(job->Role()==TaskRoleClient)
			delete job;
		else
			delete itask;
	}
	addr_ = NULL;
}

__EXPORT
int Result::fetch_row(void) 
{
	CAST(NCResult, r);
	if(r==NULL) return -EC_NO_MORE_DATA;
	if(r->result_code()<0) return r->result_code();
	if(r->result==NULL) return -EC_NO_MORE_DATA;
	return r->result->decode_row();
}

__EXPORT
int Result::rewind(void) 
{
	CAST(NCResult, r);
	if(r==NULL) return -EC_NO_MORE_DATA;
	if(r->result_code()<0) return r->result_code();
	if(r->result==NULL)  return -EC_NO_MORE_DATA;
	return r->result->rewind();
}

__EXPORT
int Result::get_result_code(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return 0;
	return r->result_code();
}

__EXPORT
const char * Result::get_error_message(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return NULL;
	int code = r->result_code();
	const char *msg = r->resultInfo.error_message();
	if(msg==NULL && code < 0) {
		char a[1];
		msg = strerror_r(-code, a, 1);
		if(msg==a) msg = NULL;
	}
	return msg;
}

__EXPORT
const char * Result::get_error_from(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return NULL;
	return r->resultInfo.error_from();
}

__EXPORT
long long Result::get_hotbackup_id() const 
{
    CAST(NCResult, r);
    return r->versionInfo.hot_backup_id();
}

__EXPORT
long long Result::get_master_hotbackup_timestamp() const 
{
    CAST(NCResult, r);
    return r->versionInfo.master_hb_timestamp();
}

__EXPORT
long long Result::slave_hotbackup_timestamp() const 
{
    CAST(NCResult, r);
    return r->versionInfo.slave_hb_timestamp();
}

__EXPORT
char* Result::ServerInfo() const 
{
    CAST(NCResult, r);
    return r->resultInfo.server_info();
}

__EXPORT
long long Result::get_binlog_id() const 
{
	DTCServerInfo *p = (DTCServerInfo *)ServerInfo();
	return p ? p->binlog_id : 0;
}

__EXPORT
long long Result::get_binlog_offset() const 
{
	DTCServerInfo *p = (DTCServerInfo *)ServerInfo();
	return p ? p->binlog_off : 0;
}

__EXPORT
long long Result::get_mem_size() const
{
	DTCServerInfo *p = (DTCServerInfo *)ServerInfo();
	return p ? p->memsize : 0;
}
__EXPORT
long long Result::get_datd_size() const
{
	DTCServerInfo *p = (DTCServerInfo *)ServerInfo();
	return p ? p->datasize : 0;
}

__EXPORT
int Result::get_num_row_size(void) const 
{
	CAST(NCResult, r);
	if(r==NULL || r->result==NULL) return 0;
	return r->result->total_rows();
}

__EXPORT
int Result::get_total_rows_size(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return 0;
	return r->resultInfo.total_rows();
}

__EXPORT
long long Result::get_insert_id(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return 0;
	return r->resultInfo.insert_id();
}

__EXPORT
int Result::get_num_fields_size(void) const 
{
	CAST(NCResult, r);
	if(r==NULL || r->result==NULL) return 0;
	return r->result->num_fields();
}

__EXPORT
const char* Result::get_field_name(int n) const 
{
	CAST(NCResult, r);
	if(r==NULL || r->result==NULL) return NULL;
	if(n<0 || n>=r->result->num_fields()) return NULL;
	return r->field_name(r->result->field_id(n));
}

__EXPORT
int Result::get_field_present(const char* name) const 
{
	CAST(NCResult, r);
	if(r==NULL || r->result==NULL) return 0;
	int id = r->field_id(name);
	if(id < 0) return 0;
	return r->result->field_present(id);
}

__EXPORT
int Result::get_field_type(int n) const 
{
	CAST(NCResult, r);
	if(r==NULL || r->result==NULL) return kFieldTypeNone;
	if(n<0 || n>=r->result->num_fields()) return kFieldTypeNone;
	return r->field_type(r->result->field_id(n));
}

__EXPORT
int Result::get_affected_rows_size(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return 0;
	return r->resultInfo.affected_rows();
}

__EXPORT
long long Result::int_key(void) const 
{
	CAST(NCResult, r);
	const DTCValue *v = NULL;
	if(r && r->flag_multi_key_result() && r->result) {
		v = r->result->field_value(0);
	}
	else if(r && r->result_key()) {
		v = r->result_key();
	}
	if(v != NULL) {
		switch(r->field_type(0)) {
		case DField::Signed:
		case DField::Unsigned:
		    return v->s64;
	    }
	}
	return 0;
}

__EXPORT
const char *Result::string_key(void) const 
{
	CAST(NCResult, r);
	const DTCValue *v = NULL;
	if(r && r->flag_multi_key_result() && r->result) {
		v = r->result->field_value(0);
	} else if(r && r->result_key()) {
		v = r->result_key();
	}
	if(v != NULL) {
		  switch(r->field_type(0)) {
			case DField::Binary:
			case DField::String:
				return v->bin.ptr;
		}
	}
	return NULL;
}

__EXPORT
const char *Result::string_key(int *lp) const 
{
	CAST(NCResult, r);
	const DTCValue *v = NULL;
	if(r && r->flag_multi_key_result() && r->result) {
		v = r->result->field_value(0);
	} else if(r && r->result_key()) {
		v = r->result_key();
	}
	if(v != NULL) {
		switch(r->field_type(0)) {
			case DField::Binary:
		    case DField::String:
			     if(lp) *lp = v->bin.len;
		        return v->bin.ptr;
	    }
	}	
	return NULL;
}

const char *Result::string_key(int &l) const 
{
	CAST(NCResult, r);
	const DTCValue *v = NULL;
	if(r && r->flag_multi_key_result() && r->result) {
		v = r->result->field_value(0);
	} else if(r && r->result_key()) {
		v = r->result_key();
	}
	if(v != NULL) {
		switch(r->field_type(0)) {
		case DField::Binary:
		case DField::String:
			 l = v->bin.len;
		    return v->bin.ptr;
	    }
	}		
	return NULL;
}

__EXPORT
const char *Result::binary_key(void) const 
{
	return string_key();
}

__EXPORT
const char *Result::binary_key(int *lp) const 
{
	return string_key(lp);
}

__EXPORT
const char *Result::binary_key(int &l) const 
{
	return string_key(l);
}

static inline int64_t GetIntValue(NCResult *r, int id) 
{
	if(id >= 0) {
		const DTCValue *v;
		if(id==0 && !(r->result->field_present(0)))
			v = r->result_key();
		else
			v = r->result->field_value(id);
		if(v) {
			switch(r->field_type(id)) {
				case DField::Signed:
				case DField::Unsigned:
					return v->s64;
				case DField::Float:
					return llround(v->flt);
					//return (int64_t)(v->flt);
				case DField::String:
					if(v->str.ptr)
						return atoll(v->str.ptr);
			}
		}
	}
	return 0;
}

static inline double GetFloatValue(NCResult *r, int id) 
{
	if(id >= 0) {
		const DTCValue *v;
		if(id==0 && !(r->result->field_present(0)))
			v = r->result_key();
		else
			v = r->result->field_value(id);

		if(v) {
			switch(r->field_type(id)) {
				case DField::Signed:
					return (double)v->s64;
				case DField::Unsigned:
					return (double)v->u64;
				case DField::Float:
					return v->flt;
				case DField::String:
					if(v->str.ptr)
						return atof(v->str.ptr);
			}
		}
	}
	return NAN;
}

static inline const char *GetStringValue(NCResult *r, int id, int *lenp) 
{
	if(id >= 0) {
		const DTCValue *v;
		if(id==0 && !(r->result->field_present(0)))
			v = r->result_key();
		else
			v = r->result->field_value(id);

		if(v) {
			switch(r->field_type(id)) {
				case DField::String:
					if(lenp) *lenp = v->bin.len;
					return v->str.ptr;
			}
		}
	}
	return NULL;
}

static inline const char *GetBinaryValue(NCResult *r, int id, int *lenp) 
{
	if(id >= 0) {
		const DTCValue *v;
		if(id==0 && !(r->result->field_present(0)))
			v = r->result_key();
		else
			v = r->result->field_value(id);

		if(v) {
			switch(r->field_type(id)) {
				case DField::String:
				case DField::Binary:
					if(lenp) *lenp = v->bin.len;
					return v->str.ptr;
			}
		}
	}
	return NULL;
}

__EXPORT
long long Result::int_value(const char *name) const 
{
	CAST(NCResult, r);
	if(r && r->result)
	    return GetIntValue(r, r->field_id(name));
	return 0;
}

__EXPORT
long long Result::int_value(int id) const 
{
	CAST(NCResult, r);
	if(r && r->result)
	    return GetIntValue(r, r->field_id_virtual(id));
	return 0;
}

__EXPORT
double Result::float_value(const char *name) const 
{
	CAST(NCResult, r);
	if(r && r->result)
	    return GetFloatValue(r, r->field_id(name));
	return NAN;
}

__EXPORT
double Result::float_value(int id) const 
{
	CAST(NCResult, r);
	if(r && r->result)
	    return GetFloatValue(r, r->field_id_virtual(id));
	return NAN;
}

__EXPORT
const char *Result::string_value(const char *name, int *lenp) const 
{
	if(lenp) *lenp = 0;
	CAST(NCResult, r);
	if(r && r->result)
	    return GetStringValue(r, r->field_id(name), lenp);
	return NULL;
}

__EXPORT
const char *Result::string_value(int id, int *lenp) const 
{
	if(lenp) *lenp = 0;
	CAST(NCResult, r);
	if(r && r->result)
	    return GetStringValue(r, r->field_id_virtual(id), lenp);
	return NULL;
}

__EXPORT
const char *Result::string_value(const char *name, int &len) const 
{
	return string_value(name, &len);
}

__EXPORT
const char *Result::string_value(int id, int &len) const 
{
	return string_value(id, &len);
}

__EXPORT
const char *Result::string_value(const char *name) const 
{
	return string_value(name, NULL);
}

__EXPORT
const char *Result::string_value(int id) const 
{
	return string_value(id, NULL);
}

__EXPORT
const char *Result::binary_value(const char *name, int *lenp) const {
	if(lenp) *lenp = 0;
	CAST(NCResult, r);
	if(r && r->result)
	    return GetBinaryValue(r, r->field_id(name), lenp);
	return NULL;
}

/* 所以UnCompressBinaryValue接口的调用均会复制一份buffer，用户得到数据后需要手动释放 */
__EXPORT
int Result::uncompress_binary_value(const char *name,char **buf,int *lenp)
{
    if(buf==NULL||lenp==NULL)
        return -EC_UNCOMPRESS_ERROR;

    int iret = 0;
	*lenp = 0;
    *buf = NULL;
    const char * buf_dup = NULL;
    uint64_t compressflag = 0;

	CAST(NCResult, r);
	if(r && r->result) {
		iret = r->init_compress();
		if (iret) return iret;
		if (name == NULL) {
			snprintf(r->gzip->errmsg_, sizeof(r->gzip->errmsg_), "field name is null");
			return -EC_UNCOMPRESS_ERROR;
		}
		int fieldId = r->field_id(name);
		/* fieldid must less than 64,because compressflag is 8 bytes uint */
		if (fieldId >63) {
			snprintf(r->gzip->errmsg_, sizeof(r->gzip->errmsg_), "fieldid must less than 64,because compressflag is 8 bytes uint");
			return -EC_UNCOMPRESS_ERROR;
		}
		buf_dup = GetBinaryValue(r, fieldId, lenp);
		if (buf_dup==NULL) {
			snprintf(r->gzip->errmsg_, sizeof(r->gzip->errmsg_), "binary_value of this field is NULL,please check fieldname and fieldtype is binary or not");
			return -EC_UNCOMPRESS_ERROR;
		}
        /* 没启用压缩 */
		if (r->compress_id() < 0) {
			*buf = (char *)MALLOC(*lenp);
			if (*buf==NULL)
				return -ENOMEM;
			memcpy(*buf,buf_dup,*lenp);
			return 0;
		}
		compressflag = GetIntValue(r,r->compress_id());
		/* 启用了压缩，但是该字段没有被压缩 */
		if (!(compressflag&(0x1<<fieldId)))
		{
			*buf = (char *)MALLOC(*lenp);
			if (*buf==NULL)
				return -ENOMEM;
			memcpy(*buf,buf_dup,*lenp);
			return 0;
        }

        /* 需要解压 */
        iret = r->gzip->UnCompress(buf,lenp,buf_dup,*lenp);
        if (iret == -111111)
            return -EC_UNCOMPRESS_ERROR;
        return iret;
    }
    return  -EC_UNCOMPRESS_ERROR;
}

/** 
 * 所以UnCompressBinaryValue接口的调用均会复制一份buffer，用户得到数据后需要手动释放
 * 忽略compressflag
 */
__EXPORT
int Result::uncompress_binary_value_force(const char *name,char **buf,int *lenp)
{
    if(buf==NULL||lenp==NULL)
        return -EC_UNCOMPRESS_ERROR;

    int iret = 0;
	*lenp = 0;
    *buf = NULL;
    const char * buf_dup = NULL;

	CAST(NCResult, r);
	if(r && r->result) {
		if (name == NULL) {
			snprintf(r->gzip->errmsg_, sizeof(r->gzip->errmsg_), "field name is null");
			return -EC_UNCOMPRESS_ERROR;
		}
		iret = r->init_compress();
		if (iret) return iret;
		int fieldId = r->field_id(name);
		buf_dup = GetBinaryValue(r, fieldId, lenp);
		if (buf_dup==NULL) {
			snprintf(r->gzip->errmsg_, sizeof(r->gzip->errmsg_), "binary_value of this field is NULL,please check fieldname and fieldtype is binary or not");
			return -EC_UNCOMPRESS_ERROR;
		}
        /* 需要解压 */
        iret = r->gzip->UnCompress(buf,lenp,buf_dup,*lenp);
        if (iret == -111111)
            return -EC_UNCOMPRESS_ERROR;
        return iret;
    }
    return  -EC_UNCOMPRESS_ERROR;
}

__EXPORT
const char *Result::uncompress_error_message() const
{
	CAST(NCResult, r);
	if(r && r->gzip)
        return  r->gzip->error_message();
    return NULL;
}

__EXPORT
const char *Result::binary_value(int id, int *lenp) const 
{
	if(lenp) *lenp = 0;
	CAST(NCResult, r);
	if(r && r->result)
	    return GetBinaryValue(r, r->field_id_virtual(id), lenp);
	return NULL;
}

__EXPORT
const char *Result::binary_value(const char *name, int &len) const 
{
	return binary_value(name, &len);
}

__EXPORT
const char *Result::binary_value(int id, int &len) const 
{
	return binary_value(id, &len);
}

__EXPORT
const char *Result::binary_value(const char *name) const 
{
	return binary_value(name, NULL);
}

__EXPORT
const char *Result::binary_value(int id) const 
{
	return binary_value(id, NULL);
}

__EXPORT
long long Result::get_tag(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return 0;
	return r->get_api_tag();
};

__EXPORT
void *Result::get_tag_ptr(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return NULL;
	return (void *)(long)r->get_api_tag();
};

__EXPORT
long long Result::magic(void) const 
{
	CAST(NCResult, r);
	if(r==NULL) return 0;
	return r->versionInfo.serial_nr();
};

__EXPORT
long long Result::get_server_timestamp(void) const
{
	CAST(NCResult, r);

	if (r == NULL) {
		return 0;
	}

	return r->resultInfo.Timestamp();
};

__EXPORT
int Server::decode_packet(Result &s, const char *buf, int len)
{
	CAST(NCServer, srv);
	s.Reset();
	s.addr_ = srv->decode_buffer(buf, len);
	return s.get_result_code();
}

__EXPORT
int Server::check_packet_size(const char *buf, int len)
{
	return NCServer::check_packet_size(buf, len);
}

__EXPORT
int Request::encode_packet(char *&ptr, int &len, long long &m) {
	CAST(NCRequest, r);
	int64_t mm;
	int ret = r->encode_buffer(ptr, len, mm);
	m = mm;
	return ret;
}

__EXPORT
int Request::encode_packet(char *&ptr, int &len, long long &m, long long k) 
{
	CAST(NCRequest, r);
	int64_t mm;
	int ret = r->encode_buffer(ptr, len, mm, (int64_t)k);
	m = mm;
	return ret;
}

__EXPORT
int Request::encode_packet(char *&ptr, int &len, long long &m, const char *k) 
{
	CAST(NCRequest, r);
	int64_t mm;
	int ret = r->encode_buffer(ptr, len, mm, k, k?strlen(k):0);
	m = mm;
	return ret;
}

__EXPORT
int Request::encode_packet(char *&ptr, int &len, long long &m, const char *k, int l) 
{
	CAST(NCRequest, r);
	int64_t mm;
	int ret = r->encode_buffer(ptr, len, mm, k, l);
	m = mm;
	return ret;
}

