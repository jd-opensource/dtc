#include <stdlib.h>
#include <stdio.h>
#include <endian.h>
#include <string.h>
#include <alloca.h>
#include <ctype.h>
#include <pthread.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <net/if.h>

#include "sockaddr.h"

static inline int has2colon(const char *name) {
	char c;
	while( (c=*name++) != '\0' ) {
		if(c==':') {
			while( (c=*name++) != '\0' ) {
				if(c==':')
					return 1;
			}
			return 0;
		}
	}
	return 0;
}

static unsigned short name2port(const char *name, int type) {
	const char *proto = type==SOCK_STREAM?"tcp":"udp";
	char buf[1024];
	struct servent result_buf;
	struct servent *result = NULL;
	const char *p = strchr(name, '/');
	if(p) {
		int len = p - name;
		char localname[len+1];
		memcpy(localname, name, len);
		localname[len] = 0;
		getservbyname_r(localname, proto, &result_buf, buf, sizeof(buf), &result);
	} else {
		getservbyname_r(name, proto, &result_buf, buf, sizeof(buf), &result);
	}
	if(result == NULL)
		return 0;
	return ntohs(result_buf.s_port);
}

static unsigned int ip2addr(const char *name) {
	if(name[0]=='*')
		return 0;

	struct in_addr addr;
	const char *p = strrchr(name, ':');
	if(p==NULL) p = strchr(name, '/');
	if(p==NULL)
	{
		if(inet_pton(AF_INET, name, &addr) <= 0)
			return INADDR_NONE;
	} else {
		int len = p - name;
		char localname[len+1];
		memcpy(localname, name, len);
		localname[len] = 0;
		if(inet_pton(AF_INET, localname, &addr) <= 0)
			return INADDR_NONE;
	}

	unsigned int v = ntohl(addr.s_addr);
	if(v < 0x0100000)
		return INADDR_NONE;

	return addr.s_addr;
}

static pthread_mutex_t lifr = PTHREAD_MUTEX_INITIALIZER;
static int nifr = 0;
struct ifreq * volatile ifr = NULL;

// clear interface information cache
// if module unloaded
__attribute__((__destructor__))
static void clean_ifr_cache(void) {
	if(ifr != NULL) {
		free(ifr);
		ifr = NULL;
	}
}

static int getintflist(void) {
	if(ifr != NULL) return 0;
	pthread_mutex_lock(&lifr);
	if(ifr == NULL) {
		int fd = socket(AF_INET, SOCK_DGRAM, 0);
		if( fd >= 0) {
			struct ifconf ifc;

			ifc.ifc_len = 0;
			ifc.ifc_req = NULL;
			if(ioctl(fd, SIOCGIFCONF, &ifc)==0)
			{
				ifr = (struct ifreq *)malloc(ifc.ifc_len>128?ifc.ifc_len:128);
				ifc.ifc_req = ifr;
				if(ioctl(fd, SIOCGIFCONF, &ifc)==0)
					nifr = ifc.ifc_len / sizeof(struct ifreq);
			}
			close(fd);
		}
	}
	pthread_mutex_unlock(&lifr);
	return ifr == NULL ? -1 : 0;
}

static int is_local_ipv4(uint32_t addr) {
	if(addr==0) return 1;
#if __BYTE_ORDER == __BIG_ENDIAN
	if((addr >> 24) == 127)
		return 1;
#elif __BYTE_ORDER == __BIG_ENDIAN
	if((addr & 0xFF) == 127)
		return 1;
#endif

	if(getintflist() < 0)
		return 0;

	int i;
	for(i=0; i<nifr; i++)
	{
	    if(ifr[i].ifr_addr.sa_family==AF_INET) {
		    if(addr == ((struct sockaddr_in *)&ifr[i].ifr_addr)->sin_addr.s_addr)
			    return 1;
	    }
	}

	return 0;
}

static int intf2addr(const char *name, uint32_t addr[4])
{
	if(getintflist() < 0)
		return 0;

	int i;
	for(i=0; i<nifr; i++)
	{
	    if(strncmp(ifr[i].ifr_name, name, sizeof(ifr[i].ifr_name))!=0)
		    continue;

	    if(ifr[i].ifr_addr.sa_family==AF_INET) {
		    addr[0] = ((struct sockaddr_in *)&ifr[i].ifr_addr)->sin_addr.s_addr;
		    return AF_INET;
	    }
	    // never happend, SIOCGIFCONF only applicable to IPv4
	    if(ifr[i].ifr_addr.sa_family==AF_INET6) {
		    memcpy(addr, ((struct sockaddr_in6 *)&ifr[i].ifr_addr)->sin6_addr.s6_addr, 16);
		    return AF_INET6;
	    }
	}

	return 0;
}

static int host2addr(const char *name, uint32_t addr[4])
{
	char buf[1024];
	struct hostent ret;
	struct hostent *result = NULL;
	int err;

	gethostbyname_r(name, &ret, buf, sizeof(buf), &result, &err);
	if(result != NULL) {
		if(ret.h_addrtype==AF_INET && ret.h_length==4) {
			memcpy(addr, ret.h_addr, 4);
			return AF_INET;
		} else if(ret.h_addrtype==AF_INET6 && ret.h_length==16) {
			memcpy(addr, ret.h_addr, 16);
			return AF_INET6;
		}
	}
	return 0;
}

static int name2addr(const char *name, uint32_t addr[4])
{
	int family;

	family = intf2addr(name, addr);
	if(family != 0) return family;

	family = host2addr(name, addr);
	if(family != 0) return family;

	return family;
}

void CSocketAddress::BuildNameNone(void) {
	sockname[0] = 0;
}

void CSocketAddress::BuildNameUnix(void) {
	snprintf(sockname, sizeof(sockname), "%c%.*s",
			un->sun_path[0]?:'@', (int)sizeof(un->sun_path)-1, un->sun_path+1);
}

void CSocketAddress::BuildNameIpv4(void) {
	char ip[32];
	if(in4->sin_addr.s_addr==0) {
		// wild address is *
		ip[0] = '*';
		ip[1] = '\0';
	} else {
		inet_ntop(AF_INET, &in4->sin_addr, ip, sizeof(ip));
	}
	snprintf(sockname, sizeof(sockname), "%s:%d/%s", ip, ntohs(in4->sin_port), socktype==SOCK_STREAM?"tcp":"udp");
}

void CSocketAddress::BuildNameIpv6(void) {
	char ip[64];
	if(IN6_IS_ADDR_UNSPECIFIED(this->in6->sin6_addr.s6_addr32)) {
		// wild address is ::
		ip[0] = ':';
		ip[1] = ':';
		ip[2] = '\0';
	} else {
		inet_ntop(AF_INET6, &in6->sin6_addr, ip, sizeof(ip));
	}
	snprintf(sockname, sizeof(sockname), "%s:%d/%s", ip, ntohs(in6->sin6_port), socktype==SOCK_STREAM?"tcp":"udp");
}

void CSocketAddress::BuildNameBad(void)
{
	snprintf(sockname, sizeof(sockname), "<BAD>");
}

void CSocketAddress::BuildName(void)
{
	switch(addr->sa_family) {
		case 0:
			BuildNameNone();
			break;

		case AF_UNIX:
			BuildNameUnix();
			break;

		case AF_INET:
			BuildNameIpv4();
			break;

		case AF_INET6:
			BuildNameIpv6();
			break;

		default:
			BuildNameBad();
			break;
	}
}

const char * CSocketAddress::SetAddress(const char *name, int port, int type)
{
	if(IsUnixSocketPath(name)) {
		if(port != 0)
			return "UNIX socket path hasnot a port";
		return SetUnixAddress(name, type);
	}

	if(has2colon(name))
		return SetIPv6Address(name, port, type);

	if(name[0]=='*' || isdigit(name[0]))
		return SetIPv4Address(name, port, type);

	return SetHostAddress(name, port, type);
}

const char * CSocketAddress::SetAddress(const char *name, const char *portstr)
{
	if(IsUnixSocketPath(name)) {
		if(portstr != NULL && portstr[0] != '\0')
			return "UNIX socket path hasnot a port";
		return SetUnixAddress(name);
	}

	int port = 0;
	int type = SOCK_STREAM;
	if(portstr != NULL && portstr[0] != '\0') {
		port = isdigit(portstr[0]) ?  atoi(portstr) : name2port(portstr, SOCK_STREAM);
		if(port==0)
			return "Invalid TCP/UDP port";
		const char *p = strrchr(portstr, '/');
        if (p != NULL)
        {
            if(!strcmp(p+1, "udp")) {
                type = SOCK_DGRAM;
            } else if(!strcmp(p+1, "tcp")) {
                type = SOCK_STREAM;
            } else {
                return "Invalid protocol name";
            }
        }
	}

	if(has2colon(name))
		return SetIPv6Address(name, port, type);

	if(name[0]=='*' || isdigit(name[0]))
		return SetIPv4Address(name, port, type);

	return SetHostAddress(name, port, type);
}

const char * CSocketAddress::SetAddress(const struct sockaddr_un *addr, int len, int type)
{
	if(type != SOCK_STREAM && type != SOCK_DGRAM) {
		return "Invalid protocol type\n";
	}
	socktype = type;
	alen = len;
	memcpy(un, addr, len);
	BuildNameUnix();
	return NULL;
}

const char * CSocketAddress::SetAddress(const struct sockaddr_in *addr, int len, int type) {
	if(type != SOCK_STREAM && type != SOCK_DGRAM) {
		return "Invalid protocol type\n";
	}
	if(len < (int)sizeof(struct sockaddr_in)) {
		return "Invalid socklen\n";
	}
	socktype = type;
	alen = sizeof(struct sockaddr_in);
	*in4 = *addr;
	BuildNameIpv4();
	return NULL;
}

const char * CSocketAddress::SetAddress(const struct sockaddr_in6 *addr, int len, int type)
{
	if(type != SOCK_STREAM && type != SOCK_DGRAM) {
		return "Invalid protocol type\n";
	}
	if(len < (int)sizeof(struct sockaddr_in6)) {
		return "Invalid socklen\n";
	}
	socktype = type;
	alen = sizeof(struct sockaddr_in6);
	*in6 = *addr;
	BuildNameIpv6();
	return NULL;
}

const char * CSocketAddress::SetAddress(const struct sockaddr *addr, int len, int type)
{
	switch(addr->sa_family) {
	case AF_UNIX: return SetAddress((const sockaddr_un *)addr, len, type);
	case AF_INET: return SetAddress((const sockaddr_in *)addr, len, type);
	case AF_INET6: return SetAddress((const sockaddr_in6 *)addr, len, type);
	}
	return "Unsupported socket family";
}

const char * CSocketAddress::SetUnixAddress(const char *name, int type)
{
	int namelen = strlen(name);
	if(namelen >= (int)sizeof(this->un->sun_path))
		return "UNIX socket path name too long";
	this->alen = InitUnixSocketAddress(this->un, name);
	if(namelen >= 4 && !strcmp(name+namelen-4, "/udp"))
		this->socktype = SOCK_DGRAM;
	else if(namelen >= 4 && !strcmp(name+namelen-4, "-udp"))
		this->socktype = SOCK_DGRAM;
	else if(namelen >= 6 && !strcmp(name+namelen-6, "/dgram"))
		this->socktype = SOCK_DGRAM;
	else if(namelen >= 6 && !strcmp(name+namelen-6, "-dgram"))
		this->socktype = SOCK_DGRAM;
	else
		this->socktype = type;
	BuildNameUnix();
	return NULL; // SUCC
}

const char * CSocketAddress::SetIPv4Address(const char *name, int port, int type)
{
	const char *p = NULL;

	if( (p = strchr(name, '/')) != NULL)
	{
		p++;
		if(!strcmp(p, "udp"))
			type = SOCK_DGRAM;
		else if(!strcmp(p, "tcp"))
			type = SOCK_STREAM;
		else
			return "Unknown IPv4 protocol";
	}

	if( (p = strrchr(name, ':')) != NULL)
	{
		p++;
		if(strchr(p, '.'))
			return "Invalid TCP/UDP port";
		port = isdigit(p[0]) ?  atoi(p) : name2port(p, type);
		if(port==0)
			return "Invalid TCP/UDP port";
	}

	if(port == 0)
		port = 8888;
	else if(port < 0 || port >= 65536)
		return "TCP/UDP port must between 1-65535";

	unsigned int ipv4 = ip2addr(name);
	if(ipv4 == INADDR_NONE)
		return "Invalid IPv4 address";

	socktype = type;
	alen = sizeof(struct sockaddr_in);
	memset(in4, 0, alen);
	in4->sin_family = AF_INET;
	in4->sin_addr.s_addr = ipv4;
	in4->sin_port = htons(port);
	BuildNameIpv4();
	return NULL;
}

const char * CSocketAddress::SetIPv6Address(const char *name, int port, int type)
{
	const char *p = NULL;

	if( (p = strchr(name, '/')) != NULL )
	{
		p++;
		if(!strcmp(p, "udp"))
			type = SOCK_DGRAM;
		else if(!strcmp(p, "tcp"))
			type = SOCK_STREAM;
		else
			return "Unknown IPv6 protocol";
	}

	if(port == 0)
	{
		// always has 2 colon, seek to right most colon
		p = strrchr(name, ':') + 1;
		if(strchr(p, '.'))
			return "Invalid TCP/UDP port";
		port = isdigit(p[0]) ?  atoi(p) : name2port(p, type);
		if(port==0)
			return "Invalid TCP/UDP port";
	}

	if(port == 0)
		port = 8888;
	else if(port < 0 || port >= 65536)
		return "TCP/UDP port must between 1-65535";

	char addr[16];
	// IPv6 has not '*', use '::' instead

	if(p) {
		// p pointer to : or /

		// make a new copy of name, strip last colon
		// colon is a valid seperator in IPv6 numeric address
		int len = p - name;
		char localname[len];
		memcpy(localname, name, len);
		localname[len-1] = 0;
		if(inet_pton(AF_INET6, localname, &addr) <= 0)
			return "Invalid IPv6 address";
	} else {
		if(inet_pton(AF_INET6, name, &addr) <= 0)
			return "Invalid IPv6 address";
	}

	socktype = type;
	alen = sizeof(struct sockaddr_in6);
	memset(in6, 0, alen);
	in6->sin6_family = AF_INET6;
	memcpy(in6->sin6_addr.s6_addr, addr, 16);
	in6->sin6_port = htons(port);
	BuildNameIpv6();
	return NULL;
}

const char * CSocketAddress::SetHostAddress(const char *name, int port, int type)
{
	const char *p;

	if( (p = strchr(name, '/')) != NULL)
	{
		p++;
		if(!strcmp(p, "udp"))
			type = SOCK_DGRAM;
		else if(!strcmp(p, "tcp"))
			type = SOCK_STREAM;
		else
			return "Unknown IPv4/IPv6 protocol";
	}

	if( (p = strrchr(name, ':')) != NULL)
	{
		p++;
		if(strchr(p, '.'))
			return "Invalid TCP/UDP port";
		port = isdigit(p[0]) ?  atoi(p) : name2port(p, type);
		if(port==0)
			return "Invalid TCP/UDP port";
	}

	if(port == 0)
		port = 8888;
	else if(port < 0 || port >= 65536)
		return "TCP/UDP port must between 1-65535";

	// p pointer to start of port
	if(p==NULL)
		p = strchr(name, '/');

	uint32_t addr[4];
	int family = 0;
	// or if port missing, start of protocol
	if(p==NULL)
	{
		family = name2addr(name, addr);
	} else {
		int len = p - name;
		char localname[len];
		memcpy(localname, name, len);
		localname[len-1] = 0;
		family = name2addr(localname, addr);
	}

	switch(family) {
		default:
			return "Invalid Hostname";

		case AF_INET:
			socktype = type;
			alen = sizeof(struct sockaddr_in);
			memset(in4, 0, alen);
			in4->sin_family = AF_INET;
			in4->sin_addr.s_addr = addr[0];
			in4->sin_port = htons(port);
			BuildNameIpv4();
			break;

		case AF_INET6:
			socktype = type;
			alen = sizeof(struct sockaddr_in6);
			memset(in6, 0, alen);
			in6->sin6_family = AF_INET6;
			memcpy(in6->sin6_addr.s6_addr, addr, 16);
			in6->sin6_port = htons(port);
			BuildNameIpv6();
	};
	return NULL;
}

// Match().... return 1 if address is identical
int CSocketAddress::Equal(const sockaddr *that, int alen, int type) const
{
	if( this->socktype != type ) return 0;
	if( this->addr->sa_family != that->sa_family) return 0;

	switch(this->addr->sa_family) {
	case AF_UNIX:
		{
		const struct sockaddr_un *un = (const struct sockaddr_un *)that;
		if( this->un->sun_path[0] != un->sun_path[0]) {
			return 0;
		}
		if( this->un->sun_path[0] == '/' && un->sun_path[0] == '/') {
			if(!strncmp(this->un->sun_path, un->sun_path, sizeof(un->sun_path))) {
				return 1;
			}
		}
		}
		break;

	case AF_INET:
		{
		const struct sockaddr_in *in4 = (const struct sockaddr_in *)that;
		return  alen >= (int)sizeof(struct sockaddr_in) &&
			this->in4->sin_port == in4->sin_port &&
			this->in4->sin_addr.s_addr == in4->sin_addr.s_addr;
		}
		break;

	case AF_INET6:
		if(alen < (int)sizeof(struct sockaddr_in6))
			return 0;

		{
		const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *)that;
		if( this->in6->sin6_port != in6->sin6_port )
			return 0;

		if(!IN6_ARE_ADDR_EQUAL(this->in6->sin6_addr.s6_addr32, in6->sin6_addr.s6_addr32))
			return 0;
		}

		return 1;
		break;
	};

	if( (int)this->alen != alen ) return 0;
	return !memcmp(&this->addr, addr, alen);
}

int CSocketAddress::Equal(const CSocketAddress *that) const
{
	return Equal(that->addr, that->alen, that->socktype);
}

int CSocketAddress::Equal(const struct sockaddr_un *addr, int len, int type) const
{
	return Equal((const struct sockaddr *)addr, len, type);
}
int CSocketAddress::Equal(const struct sockaddr_in *addr, int len, int type) const
{
	return Equal((const struct sockaddr *)addr, len, type);
}
int CSocketAddress::Equal(const struct sockaddr_in6 *addr, int len, int type) const
{
	return Equal((const struct sockaddr *)addr, len, type);
}

int CSocketAddress::Equal(const char *path) const
{
	CSocketAddress temp;
	if(temp.SetAddress(path, 0) < 0)
		return 0;
	return Equal(&temp);
}

int CSocketAddress::Equal(const char *path, int port, int type) const
{
	CSocketAddress temp;
	if(temp.SetAddress(path, port, type) < 0)
		return 0;
	return Equal(&temp);
}
int CSocketAddress::Equal(const char *path, const char * port) const
{
	CSocketAddress temp;
	if(temp.SetAddress(path, port) < 0)
		return 0;
	return Equal(&temp);
}

// Match().... return wether this object accept the peer address
int CSocketAddress::Match(const CSocketAddress *that) const
{
	if( this->socktype != that->socktype ) return 0;

	int this_family = this->addr->sa_family;
	uint32_t this_ipv4 = INADDR_NONE;
	uint16_t this_port = 0;
	uint32_t that_ipv4 = INADDR_NONE;
	uint16_t that_port = 0;

	// cast down V4MAPPED to ipv4
	switch(this_family) {
	case AF_INET:
		this_ipv4 = this->in4->sin_addr.s_addr;
		this_port = this->in4->sin_port;
		break;
	case AF_INET6:
		if(IN6_IS_ADDR_V4MAPPED(this->in6->sin6_addr.s6_addr32)) {
			this_family = AF_INET;
			this_ipv4 = this->in6->sin6_addr.s6_addr[3];
			this_port = this->in6->sin6_port;
		}
		break;
	}

	// cast down V4MAPPED to ipv4
	switch(that->addr->sa_family) {
	case AF_INET:
		that_ipv4 = that->in4->sin_addr.s_addr;
		that_port = that->in4->sin_port;
		break;
	case AF_INET6:
		if(IN6_IS_ADDR_V4MAPPED(that->in6->sin6_addr.s6_addr32)) {
			that_ipv4 = that->in6->sin6_addr.s6_addr[3];
			that_port = that->in6->sin6_port;
		}
		if(IN6_IS_ADDR_UNSPECIFIED(that->in6->sin6_addr.s6_addr32)) {
			that_ipv4 = INADDR_ANY;
			that_port = that->in6->sin6_port;
		}
		break;
	}

	switch(this_family) {
	case AF_UNIX:
		if( this->un->sun_path[0] != that->un->sun_path[0]) {
			return 0;
		}
		if( this->un->sun_path[0] == '/' && that->un->sun_path[0] == '/') {
			if(!strncmp(this->un->sun_path, that->un->sun_path, sizeof(that->un->sun_path))) {
				return 1;
			}
		}
		break;

	case AF_INET:
		if( this_port != that_port )
			return 0;

		if( this_ipv4 == that_ipv4 )
			return 1;

		// * any address
		if( this_ipv4 == INADDR_ANY ) {
			if( is_local_ipv4(that_ipv4) )
				return 1;
		}

		return 0;
		break;

	case AF_INET6:
		// :: zero address
		this_port = this->in6->sin6_port;

		if(IN6_IS_ADDR_UNSPECIFIED(this->in6->sin6_addr.s6_addr32))
		{
			if( is_local_ipv4(that_ipv4) && this_port == that_port )
				return 1;

			if(that->addr->sa_family==AF_INET6 && this_port == that->in6->sin6_port) {
				if(IN6_IS_ADDR_UNSPECIFIED(that->in6->sin6_addr.s6_addr32))
					return 1; 
				if(IN6_IS_ADDR_LOOPBACK(that->in6->sin6_addr.s6_addr32))
					return 1; 
			}
		}

		if( that->addr->sa_family==AF_INET6 ) {
			if(this_port != that->in6->sin6_port)
				return 0;
			if(IN6_ARE_ADDR_EQUAL(this->in6->sin6_addr.s6_addr32, that->in6->sin6_addr.s6_addr32))
				return 1;
		}

		return 0;
		break;
	};
	if( this->alen != that->alen ) return 0;
	return !memcmp(&this->addr, &that->addr, this->alen);
}

int CSocketAddress::Match(const char *path, int port, int type) const
{
	CSocketAddress temp;
	if(temp.SetAddress(path, port, type) < 0)
		return 0;
	return Match(&temp);
}

int CSocketAddress::Match(const char *path) const
{
	CSocketAddress temp;
	if(temp.SetAddress(path, 0) < 0)
		return 0;
	return Match(&temp);
}

int CSocketAddress::Match(const char *path, const char * port) const
{
	CSocketAddress temp;
	if(temp.SetAddress(path, port) < 0)
		return 0;
	return Match(&temp);
}

int CSocketAddress::Match(const struct sockaddr_un *addr, int len, int type) const
{
	CSocketAddress temp;
	if(temp.SetAddress(addr, len, type) < 0)
		return 0;
	return Match(&temp);
}
int CSocketAddress::Match(const struct sockaddr_in *addr, int len, int type) const
{
	CSocketAddress temp;
	if(temp.SetAddress(addr, len, type) < 0)
		return 0;
	return Match(&temp);
}

int CSocketAddress::Match(const struct sockaddr_in6 *addr, int len, int type) const
{
	CSocketAddress temp;
	if(temp.SetAddress(addr, len, type) < 0)
		return 0;
	return Match(&temp);
}

