#ifndef __CH_SOCKADDR_________
#define __CH_SOCKADDR_________

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>

class CSocketAddress
{
	public: // members
		char sockname[128];
		int socktype;
		socklen_t alen;
		union {
		struct sockaddr_in in4[1];
		struct sockaddr_in6 in6[1];
		struct sockaddr_un un[1];
		struct sockaddr addr[1];
		};

	public: // methods
		CSocketAddress(void) : socktype(0), alen(0) { this->addr->sa_family = 0; }
		~CSocketAddress(void) { }

		// initialize to name
		// success: return NULL
		// failure: return error message
		const char *SetAddress(const char *name, int port=0, int type=SOCK_STREAM);
		const char *SetAddress(const char *name, const char *portstr);
		const char *SetAddress(const struct sockaddr *, int socklen, int type);
		const char *SetAddress(const struct sockaddr_un *, int socklen, int type);
		const char *SetAddress(const struct sockaddr_in *, int socklen, int type);
		const char *SetAddress(const struct sockaddr_in6 *, int socklen, int type);
		int SocketFamily(void) const { return this->addr->sa_family; }
		int SocketType(void) const { return this->socktype; }
		const char *SocketTypeName(void) const {
			return	this->socktype==SOCK_DGRAM ? "DGRAM" :
				this->socktype==SOCK_STREAM ? "STREAM" :
				this->socktype==SOCK_RAW ? "RAW" :
				this->socktype==SOCK_SEQPACKET ? "SEQPACKET" :
				"<BAD>";
		}
		int CreateSocket(void) const { return socket(this->addr->sa_family, this->socktype, 0); }
		int ConnectSocket(int fd) const { return connect(fd, this->addr, this->alen); }
		int BindSocket(int fd) const { return bind(fd, this->addr, this->alen); }
		int SendTo(int fd, const char *buf, int len, int flags=0) const {
			return sendto(fd, buf, len, flags, this->addr, this->alen);
		}
		int RecvFrom(int fd, char *buf, int len, int flags=0) {
			this->alen = sizeof(struct sockaddr);
			int ret = recvfrom(fd, buf, len, flags, this->addr, &this->alen);
			BuildName();
			return ret;
		}
		int ConnectSocket(void) const {
			int fd = CreateSocket();
			if(fd >= 0) {
				if(ConnectSocket(fd) < 0)
					close(fd);
				fd = -1;
			}
			return fd;
		}
		int BindSocket(void) const {
			int fd = CreateSocket();
			if(fd >= 0) {
				if(BindSocket(fd) < 0)
					close(fd);
				fd = -1;
			}
			return fd;
		}

		int Equal(const CSocketAddress *) const;
		int Equal(const char *) const;
		int Equal(const char *, int, int=SOCK_STREAM) const;
		int Equal(const char *, const char *) const;
		int Equal(const struct sockaddr *, int, int=SOCK_STREAM) const;
		int Equal(const struct sockaddr_un *, int, int=SOCK_STREAM) const;
		int Equal(const struct sockaddr_in *, int, int=SOCK_STREAM) const;
		int Equal(const struct sockaddr_in6 *, int, int=SOCK_STREAM) const;
		int Match(const CSocketAddress *) const;
		int Match(const char *) const;
		int Match(const char *, int, int=SOCK_STREAM) const;
		int Match(const char *, const char *) const;
		int Match(const struct sockaddr *, int, int=SOCK_STREAM) const;
		int Match(const struct sockaddr_un *, int, int=SOCK_STREAM) const;
		int Match(const struct sockaddr_in *, int, int=SOCK_STREAM) const;
		int Match(const struct sockaddr_in6 *, int, int=SOCK_STREAM) const;
		const char *Name(void) const { return this->addr->sa_family==0 ? NULL : sockname; }

	private:
		const char *SetUnixAddress(const char *name, int type=SOCK_STREAM);
		const char *SetIPv4Address(const char *name, int port=0, int type=SOCK_STREAM);
		const char *SetIPv6Address(const char *name, int port=0, int type=SOCK_STREAM);
		const char *SetHostAddress(const char *name, int port=0, int type=SOCK_STREAM);
		void BuildName(void);
		void BuildNameBad(void);
		void BuildNameNone(void);
		void BuildNameIpv4(void);
		void BuildNameIpv6(void);
		void BuildNameUnix(void);

	public: // static methods
		static inline int IsUnixSocketPath(const char *path)
		{
			return path[0] == '/' || path[0] == '@';
		}

		static inline int InitUnixSocketAddress(struct sockaddr_un *addr, const char *path)
		{

			bzero (addr, sizeof (struct sockaddr_un));
			addr->sun_family = AF_LOCAL;
			strncpy(addr->sun_path, path, sizeof(addr->sun_path)-1);
			socklen_t addrlen = SUN_LEN(addr);
			if(path[0]=='@')
				addr->sun_path[0] = '\0';
			return addrlen;
		}
		static inline void SetSocketBuffer(int netfd, int rbufsz, int wbufsz ) {
			if(rbufsz) setsockopt(netfd, SOL_SOCKET, SO_RCVBUF, &rbufsz, sizeof(rbufsz));
			if(wbufsz) setsockopt(netfd, SOL_SOCKET, SO_SNDBUF, &wbufsz, sizeof(wbufsz));
		}
};

extern int SockBind (const CSocketAddress *addr, int backlog=0, int rbufsz=0, int wbufsz=0, int reuse=0, int nodelay=0, int defer=0);
#endif
