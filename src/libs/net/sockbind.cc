#include <netinet/tcp.h>
#include "sockaddr.h"
#include "log.h"

int SockBind (const CSocketAddress *addr, int backlog, int rbufsz, int wbufsz, int reuse, int nodelay, int defer_accept)
{
	int netfd;

	if((netfd = addr->CreateSocket ()) == -1)
	{
		log_error("%s: make socket error, %m", addr->Name());
		return -1;
	}

	int optval = 1;
	if(reuse) setsockopt (netfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof (optval));
	if(nodelay) setsockopt (netfd, SOL_TCP, TCP_NODELAY, &optval, sizeof (optval));

	/* 避免没有请求的空连接唤醒epoll浪费cpu资源 */
	if(defer_accept) {
		optval = 60;
		setsockopt (netfd, SOL_TCP, TCP_DEFER_ACCEPT, &optval, sizeof (optval));
	}

	if(addr->BindSocket(netfd) == -1)
	{
		log_error("%s: bind failed, %m", addr->Name());
		close (netfd);
		return -1;
	}

	if(addr->SocketType()==SOCK_STREAM && listen(netfd, backlog) == -1)
	{
		log_error("%s: listen failed, %m", addr->Name());
		close (netfd);
		return -1;
	}

	if(rbufsz) setsockopt(netfd, SOL_SOCKET, SO_RCVBUF, &rbufsz, sizeof(rbufsz));
	if(wbufsz) setsockopt(netfd, SOL_SOCKET, SO_SNDBUF, &wbufsz, sizeof(wbufsz));

	log_info("%s on %s", addr->SocketType()==SOCK_STREAM ? "listen" : "bind", addr->Name());
	return netfd;
}
