#include <stdlib.h>

struct NCPort
{
public:
    uint64_t sn;
    int fd;  
    /* the timeout this socket is */
    int timeout;

public:
    NCPort()
    {
        sn = 0;
        fd = -1;
        timeout = -1;
    }

    NCPort(const NCPort &that)
    {
        sn = that.sn;
        fd = that.fd;
        timeout = that.timeout;
    }

    ~NCPort()
    {
        if (fd >= 0) 
            close(fd);
    }
};

struct NCUdpPort : public NCPort
{
public:
    int sa_family;
    pid_t pid;
    NCUdpPort* next;

public:
    NCUdpPort() { pid = -1; };

public:
    static NCUdpPort *get_family_port(int sa_family);
    /* put back cache */
    void put_list_node();  
    void delete_port();
};

class NCUdpPortList 
{
public:
	int stop_;
	int sa_family_;
	int (*newport)(void);
	pthread_mutex_t lock_;
	NCUdpPort *list_node_;  
public:
	~NCUdpPortList(void);
public:
	NCUdpPort *get_all_port(void);  
	void put_family_list_node(NCUdpPort *);
    static NCUdpPortList* get_port_list(int sa_family);
};

