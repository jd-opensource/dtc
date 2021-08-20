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
#include <stdlib.h>
#include <linux/sockios.h>
#include <pthread.h>
#include <fcntl.h>
#include <netinet/tcp.h>

#include "socket/unix_socket.h"
#include "dtcint.h"
#include <algorithm/timestamp.h>
#include <container.h>

using namespace std;

const uint32_t ACCESS_KEY_LEN = 40;
#define CLIENT_CURVE 5
#define AGENT_CURVE 8

const char* dtc_api_ver="dtc-api-ver:4.3.4"; 
const char* dtc_api_complite="dtc-api-complite-date:" __DATE__ " " __TIME__;

int NCServer::network_mode_ = 0;
char * NCServer::server_address_ = NULL;
char * NCServer::server_tablename_ = NULL;
extern "C" void set_network_mode(void)
{
    NCServer::network_mode_ = 1;
}

extern "C" void set_server_address(const char * address)
{
    NCServer::server_address_ = strdup(address);
}

extern "C" void set_server_tablename(const char * tablename)
{
    NCServer::server_tablename_ = strdup(tablename);
}

NCServer::NCServer(void)
{
	keytype_ = DField::None;

	tablename_ = NULL;
	appname_ = NULL;

	auto_update_table_ = 1;
	uto_reconnect_ = 1;
	table_definition_ = NULL;
	admin_tdef = NULL;
	
	unsigned int a = time(NULL);
	a = rand_r(&a) + (long)this;
	last_serialnr_ = rand_r(&a);

	completed_ = 0;
	badkey_ = 0;
	badname_ = 0;
	autoping_ = 0;
	error_str_ = NULL;

	timeout_ = 5000;
	realtmo_ = 0;

	service_ = NULL;
	executor_ = NULL;
	netfd = -1;
	last_action_ = 0;
	ping_request_ = NULL;

	owner_pool_ = NULL;
	owner_id_ = 0;
	data_connector_ = DataConnector::getInstance();

	error_count_ = 0;
	total_count_ = 0;
	total_elaps_ = 0;
	remove_count_ = 0;
}


// copy most members, zero connection state
// copy address if use generic address
NCServer::NCServer(const NCServer &that) :
	addr_(that.addr_),
	keyinfo_(that.keyinfo_)
{
#define _MCOPY(x) this->x = that.x
#define _MZERO(x) this->x = 0
#define _MNEG(x) this->x = -1
#define _MDUP(x)	this->x = that.x ? STRDUP(that.x) : NULL

	_MDUP(tablename_);
	_MDUP(appname_);
	_MCOPY(keytype_);
	_MCOPY(auto_update_table_);
	_MCOPY(uto_reconnect_);

	_MCOPY(completed_);
	_MCOPY(badkey_);
	_MCOPY(badname_);
	_MCOPY(autoping_);
	// errstr always compile time const string
	_MCOPY(error_str_);

	_MCOPY(table_definition_);
	_MCOPY(admin_tdef);
	if(table_definition_) table_definition_->increase();
	if(admin_tdef) admin_tdef->increase();

	last_action_ = time(NULL);
	_MCOPY(timeout_);
	_MZERO(realtmo_); // this is sockopt set to real netfd
	_MCOPY(service_);
	_MCOPY(executor_);
	_MNEG(netfd);	// real socket fd, cleared
	_MZERO(ping_request_); // cached ping request object

	unsigned int a = last_action_;
	a = rand_r(&a) + (long)this;
	last_serialnr_ = rand_r(&a);


	_MZERO(owner_pool_); // serverpool linkage
	_MZERO(owner_id_); // serverpool linkage

#undef _MCOPY
#undef _MZERO
#undef _MNEG
#undef _MDUP
}

NCServer::~NCServer(void) 
{
	DELETE(ping_request_);
	Close();
	FREE_IF(tablename_);
	FREE_IF(appname_);
	DEC_DELETE(table_definition_);
	DEC_DELETE(admin_tdef);
	DataConnector::getInstance()->send_data();		// 防止10s 间隔时间未到，程序先结束了少报一次
	DataConnector::getInstance()->send_top_percentile_data();
}

DataConnector*  NCServer::data_connector_ = NULL;//DataConnector::getInstance();

void NCServer::clone_table_define(const NCServer& source)
{
	DEC_DELETE(table_definition_);
	table_definition_ = source.table_definition_;
	table_definition_->increase();
	if(tablename_ != NULL)
		FREE(tablename_);
	tablename_ = STRDUP(source.tablename_);
	keytype_ = source.keytype_;
	completed_ = 1;
}

int NCServer::set_address(const char *h, const char *p) {
	if(h==NULL) {
		error_str_ = "No Host Specified";
		return -EC_BAD_HOST_STRING;
	}

	char *oldhost = NULL;
	if(addr_.Name() != NULL) {
		// dup string put on stack
		oldhost = strdupa(addr_.Name());
	}

	const char *err = addr_.set_address(h, p);
	if(err) {
		this->error_str_ = err;
		return -EC_BAD_HOST_STRING;
	}

	// un-changed
	if(oldhost!=NULL && !strcmp(addr_.Name(), oldhost)) 
		return 0;

	Close();

    //set network model
	executor_ = NULL;
    if(!network_mode_&&service_ && service_->match_listening_ports(addr_.Name(), NULL)) {
        executor_ = service_->query_task_executor();
        DTCTableDefinition *t1 = service_->query_table_definition();
        if(t1 != this->table_definition_) {
            DEC_DELETE(this->table_definition_);
            this->table_definition_ = t1;
        }
        DTCTableDefinition *t2 = service_->query_admin_table_definition();
        if(t2 != this->table_definition_) {
            DEC_DELETE(this->admin_tdef);
            this->admin_tdef = t2;
        }
    }

	return 0;
}

int NCServer::int_key(void) {
	if(keytype_ != DField::None && keytype_ != DField::Signed)
	    return -EC_BAD_KEY_TYPE;
	keytype_ = DField::Signed;
	if(tablename_ != NULL) completed_ = 1;
	return 0;
}

int NCServer::string_key(void) {
	if(keytype_ != DField::None && keytype_ != DField::String)
	    return -EC_BAD_KEY_TYPE;
	keytype_ = DField::String;
	if(tablename_ != NULL) completed_ = 1;
	return 0;
}

int NCServer::add_key(const char* name, uint8_t type)
{
	int ret = keyinfo_.add_key(name, type);
	// TODO verify tabledef
	if(ret==0) {
		if(tablename_ != NULL) completed_ = 1;
		if(keytype_ == DField::None)
			keytype_ = type;
	}
	return 0;
}

int NCServer::set_table_name(const char *name) {
	if(name==NULL) return -EC_BAD_TABLE_NAME;

	if(tablename_)
		return mystrcmp(name, tablename_, 256)==0 ? 0 : -EC_BAD_TABLE_NAME;

	tablename_ = STRDUP(name);

    if(&NCServer::check_internal_service != 0 && !network_mode_) {
        check_internal_service();
    }

	if(keytype_ != DField::None) completed_ = 1;

	return 0;
}

int NCServer::get_field_type(const char *name) {
	if(table_definition_ == NULL)
		ping();

	if(table_definition_ != NULL){
		int id = table_definition_->field_id(name);
		if(id >= 0)
			return table_definition_->field_type(id);
	}
	
	return DField::None;
}

//fixed, sec --> msec
void NCServer::set_timeout(int n) {
	timeout_ = n<=0 ? 5000 : n;
	update_timeout();
}

void NCServer::update_timeout(void) {
	if(netfd >= 0 && realtmo_ != timeout_) {
		struct timeval tv = { timeout_/1000, (timeout_%1000)*1000 };
		setsockopt(netfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
		if(timeout_) {
			if(tv.tv_usec > 1000000)
			{
				tv.tv_usec += 100000; // add more 100ms
			}else {
				tv.tv_usec += 100; // add more 100us
			}
			if(tv.tv_usec >= 1000000)
			{
				tv.tv_usec -= 1000000;
				tv.tv_sec  +=1;
			}
		}
		setsockopt(netfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		realtmo_ = timeout_;
	}
}

void NCServer::update_timeout_anyway(void){
	if(netfd >= 0) {
		struct timeval tv = { timeout_/1000, (timeout_%1000)*1000 };
		setsockopt(netfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
		if(timeout_) {
			if(tv.tv_usec > 1000000)
			{
				tv.tv_usec += 100000; // add more 100ms
			}else {
				tv.tv_usec += 100; // add more 100us
			}
			if(tv.tv_usec >= 1000000)
			{
				tv.tv_usec -= 1000000;
				tv.tv_sec  +=1;
			}
		}
		setsockopt(netfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		realtmo_ = timeout_;
	}
}

void NCServer::Close(void){
	if(netfd>=0) {
		close(netfd);
		netfd = -1;
		realtmo_=0;
	}
}

int NCServer::bind_temp_unix_socket(void) {
	struct sockaddr_un tempaddr;
	tempaddr.sun_family = AF_UNIX;
	snprintf(tempaddr.sun_path, sizeof(tempaddr.sun_path),
			"@dtcapi-%d-%d-%d", getpid(), netfd, (int)time(NULL));
	socklen_t len = SUN_LEN(&tempaddr);
	tempaddr.sun_path[0] = 0;
	return bind(netfd, (const sockaddr *)&tempaddr, len);
}

int NCServer::Connect(void) {
	if(owner_pool_ != NULL)
		return -EC_PARALLEL_MODE;
	if(executor_ != NULL)
		return 0;
	if(netfd >= 0)
		return 0;

	int err = -EC_NOT_INITIALIZED;

	if(addr_.socket_family() != 0) {
		netfd = addr_.create_socket();
		if(netfd < 0) {
			log4cplus_error("create socket error: %d, %m", errno);
			err = -errno;
		} else if(addr_.socket_family()==AF_UNIX && is_dgram() && bind_temp_unix_socket() < 0) {
			log4cplus_error("bind unix socket error: %d, %m", errno);
			err = -errno;
			Close();
		} else {
			int iRes = -1;
			//先将netfd设置为非阻塞模式，然后进行connect，返回0，则建立连接成功；返回其它失败
			fcntl(netfd, F_SETFL, fcntl(netfd, F_GETFL, 0) | O_NONBLOCK);
			if(addr_.connect_socket(netfd)==0) {
				iRes = 0;
			}
			else{
				int iTimeout = timeout_<=0 ? 5000 : timeout_;
				//struct pollfd *pollfds = (struct pollfd *) calloc(1, sizeof(struct pollfd));
				struct pollfd pollfds[1];
						
				pollfds[0].fd = netfd;
				pollfds[0].events = POLLIN | POLLOUT | POLLERR;
				int ready_num = poll(pollfds, 1, iTimeout);
				if (ready_num < 0) {
					log4cplus_error("network error in connect, errno [%d], errmsg [%s]", errno, strerror(errno));
				}
				else if (ready_num == 0) {
					log4cplus_error("connect time out, errno [%d], errmsg [%s]", errno, strerror(errno));
				}
				else {
					if (pollfds[0].revents & POLLERR) {
						log4cplus_error("network error in connect, errno [%d], errmsg [%s]", errno, strerror(errno));
					}
					else if (pollfds[0].revents & POLLOUT || pollfds[0].revents & POLLIN) {
						iRes = 0;
					}

				}
			}
			//将netfd恢复为阻塞模式
			fcntl(netfd, F_SETFL, fcntl(netfd, F_GETFL, 0) & ~O_NONBLOCK);
			int keepalive = 1; //开启keepalive属性
			int keepidle = 60; //如该连接在60秒内没有任何数据往来,则进行探测
			int keepinterval = 5; //探测时发包的时间间隔为5 秒
			int keepcount = 3; //探测尝试的次数。如果第1次探测包就收到响应了,则后2次的不再发。
			setsockopt(netfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&keepalive , sizeof(keepalive ));
			setsockopt(netfd, SOL_TCP, TCP_KEEPIDLE, (void*)&keepidle , sizeof(keepidle ));
			setsockopt(netfd, SOL_TCP, TCP_KEEPINTVL, (void *)&keepinterval , sizeof(keepinterval ));
			setsockopt(netfd, SOL_TCP, TCP_KEEPCNT, (void *)&keepcount , sizeof(keepcount ));
			update_timeout_anyway();
			
			if(0 == iRes)
				return 0;
			log4cplus_error("connect dtc server error: %d,%m", errno);
			err = -errno;
			if(err==-EINPROGRESS) err = -ETIMEDOUT;
			Close();
		}
	}
	return err;
}

int NCServer::reconnect(void) {
	if(owner_pool_ != NULL)
		return -EC_PARALLEL_MODE;
	if(executor_ != NULL)
		return 0;
	if(netfd < 0)
		return -ENOTCONN;

	if(addr_.connect_socket(netfd)==0) {
		return 0;
	}
	log4cplus_error("connect dtc server error: %d,%m", errno);
	int err = -errno;
	if(err==-EINPROGRESS) err = -ETIMEDOUT;
	Close();
	return err;
}

int NCServer::send_packet_stream(Packet &pk) 
{
	int err;
#ifndef SIOCOUTQ
	if(1) {
		char tmp[1];
		err = recv(netfd, tmp, sizeof(tmp), MSG_PEEK|MSG_DONTWAIT);
		if(err==0 || (err<0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
			if(!uto_reconnect_) {
				log4cplus_error("dtc server close connection: %d,%m", err);
				return err;
			} else {
				log4cplus_debug("%s", "dtc server close connection, re-connect now.");
				Close();
				err = Connect();
				if(err) return err;
			}
		}
	}
#endif

	while((err = pk.Send(netfd)) == SendResultMoreData)
	{
	    if(errno==EINPROGRESS)
	    {
			log4cplus_error("socket[%d] send data timeout: %d,%m", netfd, errno);
	    	// timeout
			if(1) 
				Close();
			return -ETIMEDOUT;
	    }
	}
	if(err != SendResultDone)
	{
		log4cplus_error("socket[%d] send data error: %d,%m", netfd, err);
	    err = -errno;
	    if(1) 
		    Close();
	    return err;
	}
	return 0;
}

// always return network error code
// EAGAIN must re-sent
int NCServer::decode_result_stream(NCResult &tk) 
{
	if(netfd < 0) 
	{
		tk.set_error(-ENOTCONN, "API::recving", "connection is closed");
		return -ENOTCONN;
	}

	int err;
	SimpleReceiver receiver(netfd);

	uint64_t beginTime = 0;
	
	while(1)
	{
		errno = 0;
		err = tk.do_decode(receiver);

		if(err == DecodeFatalError) 
		{
			log4cplus_error("socket[%d] decode data error: %d, %m", netfd, errno);
			
			err = -errno;
			if(err==0)
				err = -ECONNRESET;
#ifdef SIOCOUTQ
			int value = 0;
			ioctl(netfd, SIOCOUTQ, &value);
			// unsent bytes
			if(value > 0) {
				err = -EAGAIN;
				tk.set_error(err, "API::sending", "client send packet error");
				Close();
				return err;
			}
#endif
			Close();
			// EAGAIN never return FatalError, should be DecodeWaitData
			tk.set_error(err, "API::recving", "client recv packet error");
			return err;
		}

		if(err == DecodeDataError)
		{
			log4cplus_error("socket[%d] decode data error: %d, %m", netfd, errno);
			
			// packet is valid
			return 0;
		}

		if(err == DecodeDone)
		{
			break;
		}

		if(errno == EINPROGRESS || errno == EAGAIN)
		{
			log4cplus_error("socket[%d] decode data error: %d, %m", netfd, errno);
			log4cplus_debug("use time %ldms", (long)((GET_TIMESTAMP()-beginTime)/1000));
			
			Close();
			tk.set_error(-ETIMEDOUT, "API::recving", "client recv packet timedout");
			return -ETIMEDOUT;
		}
	}

	save_definition(&tk);
	if(autoping_) {
		time(&last_action_);
		err = tk.versionInfo.keep_alive_timeout();
		if(err<15) err = 15;
		last_action_ += err - 1;
	}
	return 0;
}

int NCServer::send_packet_dgram(SocketAddress *peer, Packet &pk)
{
	int err = 0;

	while ((err = pk.send_to(netfd, peer)) == SendResultMoreData)
	{
		if (errno == EINPROGRESS)
		{
			log4cplus_error("socket[%d] send data timeout: %d,%m", netfd, errno);
			// timeout
			return -ETIMEDOUT;
		}
	}

	if (err != SendResultDone)
	{
		log4cplus_error("socket[%d] send data error: %d,%m", netfd, err);
		err = -errno;
		return err;
	}
	return 0;
}

static inline char *RecvFromFixedSize(int fd, int len, int &err, struct sockaddr *peeraddr, socklen_t *addrlen)
{
	int blen = len <= 0 ? 1 : len;
	char *buf = (char *)MALLOC(blen);
	blen = recvfrom(fd, buf, blen, 0, peeraddr, addrlen);
	if(blen < 0) {
		err = errno;
		free(buf);
		buf = NULL;
	} else if(blen != len) {
		err = EIO;
		free(buf);
		buf = NULL;
	} else {
		err = 0;
	}

	return buf;
}

static char *RecvPacketPeek(int fd, int &len, int &err)
{
	// MSG_PEEK + MSG_TRUNC get next packet size
	char dummy[1];
	len = recv(fd, dummy, 1, MSG_PEEK|MSG_TRUNC);
	if(len < 0) {
		err = errno;
		return NULL;
	}

	return RecvFromFixedSize(fd, len, err, NULL, NULL);
}

static char *RecvPacketPeekPeer(int fd, SocketAddress *peer, int &len, int &err)
{
	struct sockaddr peeraddr;
	socklen_t sock_len;

	for (int n=0; n<10; n++) {
		char dummy[1];

		sock_len = sizeof(peeraddr);
		len = recvfrom(fd, dummy, 1, MSG_PEEK|MSG_TRUNC, &peeraddr, &sock_len);
		if(len < 0) {
			err = errno;
			return NULL;
		}

		if(peer->Equal(&peeraddr, sock_len, SOCK_DGRAM))
			break;
	}

	return RecvFromFixedSize(fd, len, err, &peeraddr, &sock_len);
}

static char *RecvPacketPoll(int fd, int msec, int &len, int &err)
{
	struct pollfd pfd;
	pfd.fd = fd;
	pfd.events = POLLIN;
	int n;
	
	while( (n = poll(&pfd, 1, msec)) < 0 && errno == EINTR)
		/* LOOP IF EINTR */;

	if(n == 0) {
		err = EAGAIN;
		return NULL;
	}
	if(n < 0) {
		err = errno;
		return NULL;
	}

	len = 0;
	ioctl(fd, SIOCINQ, &len);

	return RecvFromFixedSize(fd, len, err, NULL, NULL);
}

// always return network error code
// EAGAIN must re-sent
int NCServer::decode_result_dgram(SocketAddress *peer, NCResult &tk) 
{
	if(netfd < 0) 
	{
		tk.set_error(-ENOTCONN, "API::recving", "connection is closed");
		return -ENOTCONN;
	}

	int saved_errno; // saved errno
	int len; // recv len
	char *buf; // packet buffer
	uint64_t beginTime = 0;

	log4cplus_debug("wait response time stamp = %lld", (long long)(beginTime = GET_TIMESTAMP()));
	
	if(addr_.socket_family() == AF_UNIX) {
		if(peer == NULL) {
			buf = RecvPacketPoll(netfd, timeout_,  len, saved_errno);
		} else {
			if(peer->connect_socket(netfd) < 0) {
				saved_errno = errno;
				log4cplus_error("connect dtc server error: %d,%m", saved_errno);
				if(saved_errno == EAGAIN) {
					// unix socket return EAGAIN if listen queue overflow
					saved_errno = EC_SERVER_BUSY;
				}
				tk.set_error(-saved_errno, "API::connecting", "client api connect server error");
				return -saved_errno;
			}

			buf = RecvPacketPoll(netfd, timeout_, len, saved_errno);

			// Un-connect
			struct sockaddr addr;
			addr.sa_family = AF_UNSPEC;
			connect(netfd, &addr, sizeof(addr));
		}
	} else {
		if(peer == NULL) {
			buf = RecvPacketPeek(netfd, len, saved_errno);
		} else {
			buf = RecvPacketPeekPeer(netfd, peer, len, saved_errno);
		}
	}
	log4cplus_debug("receive result use time: %ldms", (long)((GET_TIMESTAMP()-beginTime)/1000));

	if(buf == NULL)
	{
		errno = saved_errno;
		log4cplus_error("socket[%d] recv udp data error: %d,%m", netfd, saved_errno);

		if(saved_errno == EAGAIN) {
			// change errcode to TIMEDOUT;
			saved_errno = ETIMEDOUT;
			tk.set_error(-ETIMEDOUT, "API::recving", "client recv packet timedout");
		} else {
			if(saved_errno==0)
				saved_errno = ECONNRESET;
			tk.set_error(-saved_errno, "API::recving", "connection reset by peer");
		}
		return -saved_errno;
	}

	int err = tk.do_decode(buf, len, 1 /*eat buf*/);

	switch(err) {
	case DecodeFatalError:
		log4cplus_error("socket[%d] decode udp data error: invalid packet content", netfd);
		tk.set_error(-ECONNRESET, "API::recving", "invalid packet content");
		FREE_IF(buf); // buf not eaten
		return -ECONNRESET;

	case DecodeWaitData:
	default:
		// UNREACHABLE, decode_packet never return these code
		log4cplus_error("socket[%d] decode udp data error: %d, %m", netfd, errno);
		tk.set_error(-ETIMEDOUT, "API::recving", "client recv packet timedout");
		return -ETIMEDOUT;

	case DecodeDataError:
		// packet maybe valid
		break;

	case DecodeDone:
		save_definition(&tk);
		break;

	}

	return 0;
}

//Get udpport form udppool
NCUdpPort *NCServer::get_global_port()
{
    NCUdpPort *udpport = NCUdpPort::get_family_port(addr_.socket_family());

    if (udpport)
    {
	    netfd = udpport->fd;
	    last_serialnr_ = udpport->sn;
	    realtmo_ = udpport->timeout; 
	    update_timeout();
    }

    return udpport;
}

//Put the udpport to the udppool
void NCServer::put_global_port(NCUdpPort *udpport)
{
	if (udpport)
	{
		if (netfd > 0)
		{
			udpport->sn = last_serialnr_;
			udpport->timeout = realtmo_;
			netfd = -1;
			udpport->put_list_node();
		}
		else
		{
			//if fd error, delete this udpport object
			udpport->delete_port();
		}
	}
}

void NCServer::save_definition(NCResult *tk)
{
	if(strcmp("*", tablename_)!=0 && tk->result_code()==-EC_TABLE_MISMATCH) {
		badname_ = 1;
		error_str_ = "Table Name Mismatch";
		return;
	}
	if(strcmp("*", tablename_)!=0 && tk->result_code()==-EC_BAD_KEY_TYPE) {
		badkey_ = 1;
		error_str_ = "Key Type Mismatch";
		return;
	}
	
	DTCTableDefinition *t = tk->remote_table_definition();
	if(t == NULL) return;

	if(t->is_admin_table()){
		if(admin_tdef)
		{
			if(admin_tdef->is_same_table(t)) return;
			if(!auto_update_table_){
				badname_ = 1;
				error_str_ = "Table Mismatch";
				tk->set_error(-EC_TABLE_MISMATCH, "API::executed", "AdminTable Mismatch");
				return;
			}
			DEC_DELETE(admin_tdef);
		}
		admin_tdef = t;
		admin_tdef->increase();
	}
	else{
		if(table_definition_)
		{
			if(table_definition_->is_same_table(t)) return;
			if(!auto_update_table_){
				badname_ = 1;
				error_str_ = "Table Mismatch";
				tk->set_error(-EC_TABLE_MISMATCH, "API::executed", "Table Mismatch");
				return;
			}
			DEC_DELETE(table_definition_);
		}
		table_definition_ = t;
		table_definition_->increase();
		
		FREE(tablename_);
		tablename_ = STRDUP(table_definition_->table_name());
		keytype_ = table_definition_->key_type();

	    //bugfix， by ada
	    if(keytype_ == DField::Unsigned)
	        keytype_ = DField::Signed;

	    if(keytype_ == DField::Binary)
	        keytype_ = DField::String;
	}
}

void NCServer::try_ping(void) {
	if(autoping_ && netfd >= 0) {
		time_t now;
		time(&now);
		if(now >= last_action_)
			ping();
	}
}

int NCServer::ping(void) {
	if(owner_pool_ != NULL)
		return -EC_PARALLEL_MODE;
	if(executor_ != NULL)
		return 0;
		NCRequest r(this, DRequest::TYPE_PASS);
	NCResult *t = r.execute_network();
	int ret = t->result_code();
	delete t;
	return ret;
}

NCResult *NCServer::decode_buffer(const char *buf, int len)
{
	NCResult *res = new NCResult(table_definition_);

	switch(len <= (int)sizeof(PacketHeader) ? DecodeFatalError : res->do_decode(buf, len))
	{
		default:
			res->set_error(-EINVAL, "API::decoding", "invalid packet content");
			break;
		case DecodeDataError:
		case DecodeDone:
			break;
	}
	return res;
}

int NCServer::check_packet_size(const char *buf, int len)
{
	return NCResult::check_packet_size(buf, len);
}

/*date:2014/06/04, author:xuxinxin*/
int NCServer::set_accesskey(const char *token)
{
	int ret = 0;
	std::string str;
	if(token == NULL)
		return -EC_BAD_ACCESS_KEY;
	else
		str = token;
	if(str.length() != ACCESS_KEY_LEN)
	{
		log4cplus_error("Invalid accessKey!");
		access_token_ = "";
		return -EC_BAD_ACCESS_KEY;
	}
	else
		access_token_ = str;

	/* 截取  AccessKey中前32位以后的bid， 以便上报至数据采集 dtchttpd */
	ret = data_connector_->set_bussiness_id(access_token_);
	ret = data_connector_->set_top_percentile_config(access_token_, this->get_address());
	if(ret ==0)
		return -EC_BAD_ACCESS_KEY;
	return 0;
}

DataConnector* DataConnector::pDataConnector = NULL;

pthread_mutex_t wakeLock;
static void *do_process(void *p)
{
	sigset_t sset;
	sigfillset(&sset);
	sigdelset(&sset, SIGSEGV);
	sigdelset(&sset, SIGBUS);
	sigdelset(&sset, SIGABRT);
	sigdelset(&sset, SIGILL);
	sigdelset(&sset, SIGCHLD);
	sigdelset(&sset, SIGFPE);
	pthread_sigmask(SIG_BLOCK, &sset, &sset);

//	int ret = 0;
	time_t next = 0;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	int offset = random() % 10;
	next = (tv.tv_sec / 10) * 10 + 10 + offset;
	struct timespec ts;
	ts.tv_sec = next;
	ts.tv_nsec = 0;

	while(pthread_mutex_timedlock(&wakeLock, &ts)!=0)
	{
		gettimeofday(&tv, NULL);
		if(tv.tv_sec >= next)
		{
//			ret = DataConnector::getInstance()->send_data();
//			ret = DataConnector::getInstance()->send_top_percentile_data();
			gettimeofday(&tv, NULL);
			next = (tv.tv_sec / 10) * 10 + 10 + offset;
		}
		ts.tv_sec = next;
		ts.tv_nsec = 0;
	}
	pthread_mutex_unlock(&wakeLock);
	return NULL;
}

/* date:2014/06/09, author:xuxinxin */
DataConnector::DataConnector()
{
}

DataConnector::~DataConnector()
{
}

int DataConnector::send_data()
{
	return 0;
}

int DataConnector::set_report_info(const std::string str, const uint32_t curve, const uint64_t t)
{
	uint32_t bid = 0;
	std::string stemp = str.substr(0,8);
	sscanf(stemp.c_str(), "%u", &bid);
	struct bidCurve bc;
	bc.bid = bid;
	bc.curve = curve;

	if(mapBi.find(bc) != mapBi.end())
	{
		do
		{
			ScopedLock lock(lock_);
			mapBi[bc].TotalTime += t;
			mapBi[bc].TotalRequests += 1;
		}while(0);
	}
	return 0;
}

void DataConnector::get_report_info(std::map<bidCurve, businessStatistics> &map)
{
	ScopedLock lock(lock_);
	for(std::map<bidCurve, businessStatistics>::iterator it = mapBi.begin(); it != mapBi.end(); ++it)
	{
		struct businessStatistics bs;
		map[it->first] = it->second;
		/*  mapBi 取出来之后，不管后面是否上报成功，该bid对应的数据都丢弃，重置为0  */
		mapBi[it->first] = bs;
	}
}

int DataConnector::set_bussiness_id(std::string str)
{
	ScopedLock lock(lock_);
	if(mapBi.size() == 0)
	{
		pthread_mutex_init(&wakeLock, NULL);
		if(pthread_mutex_trylock(&wakeLock) == 0)
		{
			int ret = pthread_create(&thread_id_, 0, do_process, (void *)NULL);
			if(ret != 0)
			{
				errno = ret;
				return -1;
			}
		}
	}
	std::string stemp = str.substr(0, 8);
	uint32_t bid = 0;
	sscanf(stemp.c_str(), "%u", &bid);

	struct businessStatistics bs;
	struct bidCurve bclient, bagent;
	bclient.bid = bid;
	bclient.curve = CLIENT_CURVE;

	bagent.bid = bid;
	bagent.curve = AGENT_CURVE;
	mapBi.insert(std::make_pair(bclient, bs));
	mapBi.insert(std::make_pair(bagent, bs));
	return 0;
}

int DataConnector::send_top_percentile_data()
{
	return 0;
}

int DataConnector::set_top_percentile_data(const std::string strAccessKey, const std::string strAgentAddr, const uint64_t elapse, const int status)
{
	if(strAccessKey.empty())
		return -1;
	if(strAgentAddr.empty())
		return -1;
	if(!isdigit(strAgentAddr[0]))
		return -1;

	uint32_t uiBid = 0;
	std::string stemp = strAccessKey.substr(0,8);
	sscanf(stemp.c_str(), "%u", &uiBid);
	
	uint64_t uiIP;
	uint64_t uiPort;
	if(parse_address(strAgentAddr, &uiIP, &uiPort) < 0)
		return -1;

	uint16_t iRealSite = CTopPercentileSection::get_tp_section(elapse);
	if(iRealSite < 0 || iRealSite >= sizeof(kpi_sample_count) / sizeof(kpi_sample_count[0]))
		return -1;

	uint64_t uiKey = (uiIP << 32) | (uiPort << 16) | (uint64_t)uiBid;
	std::map<uint64_t, DataConnector::top_percentile_statistics>::iterator it;
	ScopedLock lock(m_tp_lock);
	if((it = map_tp_stat_.find(uiKey)) == map_tp_stat_.end())
	{
		DataConnector::top_percentile_statistics tpStat;
		tpStat.uiBid = uiBid;
		tpStat.uiAgentIP = uiIP;
		tpStat.uiAgentPort = uiPort;
		std::pair<std::map<uint64_t, DataConnector::top_percentile_statistics>::iterator, bool> pairRet;
		pairRet = map_tp_stat_.insert(std::make_pair(uiKey, tpStat));
		if(!pairRet.second)
			return -1;
		it = pairRet.first;
	}

	if(it->second.uiMaxTime < elapse)
		it->second.uiMaxTime = elapse;
	if(0 == it->second.uiMinTime)
		it->second.uiMinTime = elapse;
	else if(it->second.uiMinTime > elapse)
		it->second.uiMinTime = elapse;
	if(0 != status)
		it->second.uiFailCount ++;
	it->second.uiTotalRequests ++;
	it->second.uiTotalTime += elapse;
	it->second.statArr[iRealSite] ++;
	
	return 0;
}

void DataConnector::get_top_percentile_data(std::map<uint64_t, top_percentile_statistics> &mapStat)
{
	ScopedLock lock(m_tp_lock);
	std::swap(map_tp_stat_, mapStat);
}

/*
 *@strAccessKey:00001460ccea307caad065107c936c88bc59b1a4
 *@strAgentAddr:192.168.145.135:20024/tcp
 */
int DataConnector::set_top_percentile_config(const std::string strAccessKey, const std::string strAgentAddr)
{
	if(strAccessKey.empty())
		return -1;
	if(strAgentAddr.empty())
		return -1;
	if(!isdigit(strAgentAddr[0]))
		return -1;
	uint64_t uiIP = 0;
	uint64_t uiPort = 0;
	if(parse_address(strAgentAddr, &uiIP, &uiPort) < 0)
		return -1;
	uint32_t uiBid = 0;
	std::string strTemp = strAccessKey.substr(0,8);
	sscanf(strTemp.c_str(), "%u", &uiBid);
	struct top_percentile_statistics tpStat;
	tpStat.uiBid = uiBid;
	tpStat.uiAgentIP = (uint32_t)uiIP;
	tpStat.uiAgentPort = (uint16_t)uiPort;
	map_tp_stat_.insert(std::make_pair((uiIP << 32) | (uiPort << 16) | uiBid, tpStat));
	return 0;
}

/*
 *@strAddr:192.168.145.135:20024/tcp
 */
int DataConnector::parse_address(const std::string strAddr, uint64_t *uipIP /* = NULL */, uint64_t *uipPort /* = NULL */)
{
	if(strAddr.empty())
		return -1;
	if(!isdigit(strAddr[0]))
		return -1;
	const char *szAgentAddr = strAddr.c_str();
	const char *szPortProtocol = strchr(strAddr.c_str(), ':');
	if(NULL == szPortProtocol)
		return -1;
	//":"后面必然需要有端口，若是长度小于2，则数据有问题
	if(strlen((char *)szPortProtocol) < 2)
		return -1;
	if(!isdigit(szPortProtocol[1]))
		return -1;
	struct in_addr addr;
	uint32_t uiLen = szPortProtocol - szAgentAddr;
	char szIP[uiLen + 1];
	memcpy(szIP, szAgentAddr, uiLen);
	szIP[uiLen] = 0;
	if(1 != inet_pton(AF_INET, szIP, &addr))
		return -1;
	*uipIP = 0;
	*uipPort = 0;
	*uipIP = (uint64_t)ntohl(addr.s_addr);
	*uipPort = (uint64_t)atoi(szPortProtocol + 1);
	if(0 == *uipIP || 0 == *uipPort)
		return -1;
	return 0;
}

/*
 *以下数组中的值对应的为kpi_sample_count数组的下标，即跟随在其后注释中的数值所在的位置
 */
int16_t CTopPercentileSection::get_tp_section(uint64_t elapse)
{
	if(elapse < THOUSAND)
		return get_little_thousand(elapse);
	else if(THOUSAND <= elapse && elapse < TENTHOUSAND)
		return get_between_thousand_and_tenthousand(elapse);
	else if(TENTHOUSAND <= elapse && elapse < HUNDREDTHOUSAND)
		return get_between_tenthousand_and_hundredthousand(elapse);
	else if(HUNDREDTHOUSAND <= elapse && elapse < MILLION)
		return get_between_hundredthousand_and_million(elapse);
	else
		return get_exceed_million(elapse);

	return -1;
}

int16_t CTopPercentileSection::get_little_thousand(uint64_t elapse)
{
	if(elapse < THOUSAND)
	{
		static int16_t iSiteArr[] =
		{
			0,/*200us下标*/
			0,/*200us下标*/
			1,/*500us下标*/
			1,/*500us下标*/
			1,/*500us下标*/
			2,/*1000us下标*/
			2,/*1000us下标*/
			2,/*1000us下标*/
			2,/*1000us下标*/
			2/*1000us下标*/
		};
		int16_t iSite = elapse / HUNDRED;
		if(iSite < 0 || iSite >= int16_t(sizeof(iSiteArr) / sizeof(iSiteArr[0])))
			return -1;
		return iSiteArr[iSite];
	}
	return -1;
}

int16_t CTopPercentileSection::get_between_thousand_and_tenthousand(uint64_t elapse)
{
	if(THOUSAND <= elapse && elapse < TENTHOUSAND)
	{
		static int16_t iSiteArr[] =
		{
			3,/*2000us下标*/
			4,/*3000us下标*/
			5,/*4000us下标*/
			6,/*5000us下标*/
			7,/*6000us下标*/
			8,/*7000us下标*/
			9,/*8000us下标*/
			10,/*9000us下标*/
			11/*10000us下标*/
		};
		int16_t iSite = elapse / THOUSAND - 1;
		if(iSite < 0 || iSite >= int16_t(sizeof(iSiteArr) / sizeof(iSiteArr[0])))
			return -1;
		return iSiteArr[iSite];
	}
	return -1;
}

int16_t CTopPercentileSection::get_between_tenthousand_and_hundredthousand(uint64_t elapse)
{
	if(TENTHOUSAND <= elapse && elapse < HUNDREDTHOUSAND)
	{
		static int16_t iSiteArr[] =
		{
			12,/*20000us下标*/
			13,/*30000us下标*/
			14,/*40000us下标*/
			15,/*50000us下标*/
			16,/*60000us下标*/
			17,/*70000us下标*/
			18,/*80000us下标*/
			19,/*90000us下标*/
			20/*100000us下标*/
		};
		int16_t iSite = elapse / TENTHOUSAND - 1;
		if(iSite < 0 || iSite >= int16_t(sizeof(iSiteArr) / sizeof(iSiteArr[0])))
			return -1;
		return iSiteArr[iSite];
	}
	return -1;
}

int16_t CTopPercentileSection::get_between_hundredthousand_and_million(uint64_t elapse)
{
	if(HUNDREDTHOUSAND <= elapse && elapse < MILLION)
	{
		static int16_t iSiteArr[] =
		{
			21,/*200000us下标*/
			22,/*300000us下标*/
			23,/*400000us下标*/
			24,/*500000us下标*/
			25,/*600000us下标*/
			26,/*700000us下标*/
			27,/*800000us下标*/
			28,/*900000us下标*/
			29/*1000000us下标*/
		};
		int16_t iSite = elapse / HUNDREDTHOUSAND - 1;
		if(iSite < 0 || iSite >= int16_t(sizeof(iSiteArr) / sizeof(iSiteArr[0])))
			return -1;
		return iSiteArr[iSite];
	}
	return -1;
}

int16_t CTopPercentileSection::get_exceed_million(uint64_t elapse)
{
	if(elapse >= MILLION)
	{
		static int16_t iSiteArr[] =
		{
			30,/*2000000us下标*/
			31,/*5000000us下标*/
			31,/*5000000us下标*/
			31,/*5000000us下标*/
			32,/*10000000us下标*/
			32,/*10000000us下标*/
			32,/*10000000us下标*/
			32,/*10000000us下标*/
			32,/*10000000us下标*/
			32/*10000000us下标*/
		};
		int16_t iSite = elapse / MILLION - 1;
		int16_t iArrLen = int16_t(sizeof(iSiteArr) / sizeof(iSiteArr[0]));
		iSite = (iSite >= iArrLen) ? (iArrLen - 1) : iSite;
		if(iSite < 0)
			return -1;
		return iSiteArr[iSite];
	}
	return -1;
}
