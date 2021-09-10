#include <sys/socket.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include <alloca.h>
#include "udppool.h"
#include "log/log.h"

static unsigned int bindip = 0xFFFFFFFF;

unsigned int get_bindip()  
{
    int i;
    int n = 0;
    struct ifconf ifc;
    struct ifreq *ifr = NULL;
    ifc.ifc_len = 0;
    ifc.ifc_req = NULL;
    const char *name = getenv("DTCAPI_UDP_INTERFACE");  
    if (name == NULL || name[0] == 0 || strcmp(name, "*") == 0)
	    return 0;

    int fd = socket(AF_INET, SOCK_DGRAM, 0);  
    if (fd < 0)
        return 0;

    if (ioctl(fd, SIOCGIFCONF, &ifc) == 0) {  
        ifr = (struct ifreq *)alloca(ifc.ifc_len > 128 ? ifc.ifc_len : 128);  
        ifc.ifc_req = ifr;  
        if (ioctl(fd, SIOCGIFCONF, &ifc) == 0)  
            n = ifc.ifc_len / sizeof(struct ifreq);  
    }
    close(fd);
    for (i = 0; i < n; i++) {  
        if (strncmp(ifr[i].ifr_name, name, sizeof(ifr[i].ifr_name)) != 0)  
            continue;
        if (ifr[i].ifr_addr.sa_family == AF_INET)
            return ((struct sockaddr_in *)&ifr[i].ifr_addr)->sin_addr.s_addr;
    }
    return 0;
}

static int create_port_ipv4()  
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (bindip == 0xFFFFFFFF)
        bindip = get_bindip();
    if (bindip != 0) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = bindip;
        if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(fd);
            return -1;
        }
    }
    return fd;
}

static int create_port_ipv6() 
{
    int fd = socket(AF_INET6, SOCK_DGRAM, 0);
    return fd;
}

static int create_port_unix() 
{
    int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    struct sockaddr_un addr;
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path),
		    "@dtcapi-global-%d-%d-%d", getpid(), fd, (int)time(NULL));
    socklen_t len = SUN_LEN(&addr);
    addr.sun_path[0] = 0;  
    if (bind(fd, (const struct sockaddr *)&addr, len) < 0) {
	    close(fd);
	    return -1;
    }
    return fd;
}

/* this destructor only called when unloading libdtc.so */
NCUdpPortList::~NCUdpPortList()
{
	if (pthread_mutex_lock(&lock_) == 0) {
		stop_ = 1;
		while (list_node_ != NULL) {
			NCUdpPort *port = list_node_;
			list_node_ = port->next;
			delete port;
		}
		pthread_mutex_unlock(&lock_);
	}
}

NCUdpPortList* NCUdpPortList::get_port_list(int sa_family)   
{
    switch(sa_family) {
        case AF_INET:
            static NCUdpPortList ipv4List = { 0,
                AF_INET,
                create_port_ipv4,
                PTHREAD_MUTEX_INITIALIZER,
                NULL };
            return &ipv4List;
        case AF_INET6:
            static NCUdpPortList ipv6List = { 0, 
                AF_INET6, 
                create_port_ipv6, 
                PTHREAD_MUTEX_INITIALIZER, 
                NULL };
            return &ipv6List;
        case AF_UNIX:
            static NCUdpPortList unixList = { 0, 
                AF_UNIX, 
                create_port_unix, 
                PTHREAD_MUTEX_INITIALIZER, 
                NULL };
            return &unixList;
	}
	return NULL;
}

NCUdpPort* NCUdpPortList::get_all_port()
{
    NCUdpPort *port = NULL;

    if (pthread_mutex_lock(&lock_) == 0) {
        if (list_node_ != NULL) {
            port = list_node_;
            list_node_ = port->next;
        }
        pthread_mutex_unlock(&lock_);
    } else {
        log4cplus_error("api mutex_lock error,may have fd leak");
    }
    if (port != NULL) {
        if (getpid() == port->pid)
            port->sn++;
        else {
            delete port;
            port = NULL;
        }
    }
    if (port == NULL) {
        int fd = newport();
        if (fd > 0) {
            port = new NCUdpPort;
            port->fd = fd;
            unsigned int seed = fd + (long)port + (long) & port + (long)pthread_self() + (long)port;
            port->sn = rand_r(&seed);
            port->timeout = -1;
            port->pid = getpid();
            port->sa_family = sa_family_;
        }
    }
    return port;
}

void NCUdpPortList::put_family_list_node(NCUdpPort *port)  
{
    if (this != NULL && pthread_mutex_lock(&lock_) == 0) {
        if(stop_) {
            /* always delete port after unloading process */
            port->delete_port();
        } else {
            port->next = list_node_;
            list_node_ = port;
        }
        pthread_mutex_unlock(&lock_);
    } else {
        port->delete_port();
    }
}

NCUdpPort *NCUdpPort::get_family_port(int sa_family)  
{
    NCUdpPortList *portList = NCUdpPortList::get_port_list(sa_family);
    if(portList == NULL)
	    return NULL;
    return portList->get_all_port();
}

void NCUdpPort::put_list_node()
{
    NCUdpPortList *portList = NCUdpPortList::get_port_list(sa_family);
    portList->put_family_list_node(this);
}

void NCUdpPort::delete_port() 
{
    delete this;
}


