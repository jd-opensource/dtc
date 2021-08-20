#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>  
#include <sys/stat.h>
#include <time.h>
#include <fcntl.h>

#include "protocol.h"

#include "dtcapi.h"
#include "dtcint.h"
#include "dtcqossvr.h"

using namespace DTC;

/* linux-2.6.38.8/include/linux/compiler.h */
//# define likely(x)	__builtin_expect(!!(x), 1)
//# define unlikely(x)	__builtin_expect(!!(x), 0)

#define random(x) (rand()%x)

const uint32_t ACCESS_KEY_LEN = 40;

DTCQosServer::DTCQosServer():m_status(0), m_weight(0), m_remove_time_stamp(0), 
								m_last_remove_time(0), m_max_time_stamp(0), m_server(NULL)
{
}

DTCQosServer::~DTCQosServer()
{
	if (this->m_server != NULL){
		delete this->m_server;
		this->m_server = NULL;
	}
} 

DTCServers::DTCServers()
	:m_time_out(50), m_agent_time(0), m_key_type(0), m_table_name(NULL),
	m_set_route(false), m_constructed_by_set_i_ps(false), m_bid(0),
	m_idc_no(0), m_buckets_pos(0), m_balance_bucket_size(0), m_bid_version(0),
	m_last_get_ca_time(0), m_refresh_buckets_time(0), m_remove_buckets_time(0),
	m_load_balance_buckets(NULL), m_qos_severs(NULL)
{
}

DTCServers::~DTCServers()
{
	if(this->m_table_name) {
		free(this->m_table_name);
		this->m_table_name = NULL;
	}

	if(this->m_load_balance_buckets) {
		free(this->m_load_balance_buckets);
		this->m_load_balance_buckets = NULL;
	}

	if(this->m_qos_severs) {
		delete[] this->m_qos_severs;
		this->m_qos_severs = NULL;
	}
}

void print_ip_node(ROUTE_NODE RouteNode)
{
	printf("\n");
	printf("\t bid = %d \n", RouteNode.bid);
	printf("\t ip = %s \n", RouteNode.ip);
	printf("\t port = %d \n", RouteNode.port);
	printf("\t weight = %d \n", RouteNode.weight);
	printf("\t status = %d \n", RouteNode.status);
	printf("\n");
}

void DTCServers::set_error_msg(int err, std::string from, std::string msg)
{
	char buffer[1000];
	memset(buffer, 0, 1000);
	sprintf(buffer, "ERROR CODE %d, FROM %s ERRMSG %s", err, from.c_str(), msg.c_str());
	std::string ErrMsg(buffer);
	this->m_err_msg.clear();
	this->m_err_msg = ErrMsg;
}

std::string DTCServers::get_error_msg()
{
	return this->m_err_msg;
}

int InvalidIpaddr(char *str)
{
   if(str == NULL || *str == '\0')
      return 1;

   union
   {
      struct sockaddr addr;
      struct sockaddr_in6 addr6;
      struct sockaddr_in addr4;
   } a;
   memset(&a, 0, sizeof(a));
   if(1 == inet_pton(AF_INET, str, &a.addr4.sin_addr))
      return 0;
   else if(1 == inet_pton(AF_INET6, str, &a.addr6.sin6_addr))
      return 0;
   return 1;
}

int DTCServers::set_route_list(std::vector<ROUTE_NODE>& IPList)
{
	if(IPList.empty())
		return -ER_SET_IPLIST_NULL;
	std::vector<ROUTE_NODE>().swap(this->m_ip_list);	
	std::vector<ROUTE_NODE>::iterator it = IPList.begin();
	for(; it != IPList.end(); ++it) {
		ROUTE_NODE ip;
		ip.bid = it->bid;
		if(it->port > 0 && it->port <= 65535) {
			ip.port = it->port;
		} else {
			set_error_msg(ER_PORT_OUT_RANGE, "DTCServers::set_route_list", "port is out of range!");
			return -ER_PORT_OUT_RANGE;
		}
		if(it->status == 0 || it->status == 1) {
			ip.status = it->status;
		} else {
			set_error_msg(ER_STATUS_ERROR_VALUE, "DTCServers::set_route_list", "status is error value!");
			return -ER_STATUS_ERROR_VALUE;
		}
		if(it->weight > 0) {
			ip.weight = it->weight;
		} else {
			set_error_msg(ER_WEIGHT_ERROR_VALUE, "DTCServers::set_route_list", "weight is error value!");
			return -ER_WEIGHT_ERROR_VALUE;
		}
		if(InvalidIpaddr(it->ip) == 0) {
			memcpy(ip.ip, it->ip, IP_LEN);
		} else {
			set_error_msg(ER_IP_ERROR_VALUE, "DTCServers::set_accesskey", "ip is error value!");
			return -ER_IP_ERROR_VALUE;
		}
#ifdef DEBUG_INFO
		print_ip_node(ip);	
#endif
		this->m_ip_list.push_back(ip);
	}
	this->m_set_route = true;

	return 0;
}

void DTCServers::set_idc_no(int IDCNo)
{
	this->m_idc_no = IDCNo;
}

int DTCServers::set_accesskey(const char *token)
{
	std::string str;
	if(token == NULL)
		return -EC_BAD_ACCESS_KEY;
	else
		str = token;                                                     
	if(str.length() != ACCESS_KEY_LEN) {
		log4cplus_error("Invalid accessKey!");
		this->m_access_token = "";
		set_error_msg(EC_BAD_ACCESS_KEY, "DTCServers::set_accesskey", "Invalid accessKey!");
		return -EC_BAD_ACCESS_KEY;
	} else
		this->m_access_token = str;

	std::string stemp = str.substr(0, 8);
	sscanf(stemp.c_str(), "%d", &(this->m_bid));
	return 0;
}

int DTCServers::set_table_name(const char *tableName)
{
	if(tableName==NULL) return -DTC::EC_BAD_TABLE_NAME;

	if(this->m_table_name)
		return mystrcmp(tableName, this->m_table_name, 256)==0 ? 0 : -DTC::EC_BAD_TABLE_NAME; 

	this->m_table_name = STRDUP(tableName);

	return 0;
}

int DTCServers::set_key_type(int type)
{
	switch(type) {
		case DField::Signed:
		case DField::Unsigned:
		case DField::Float:
		case DField::String:
		case DField::Binary:
			this->m_key_type = type;
			break;
		default:
			return -DTC::EC_BAD_KEY_TYPE;
			break; 
	}
	return 0;
}

void DTCServers::set_agenttime(int t)
{
	this->m_agent_time = t;
}

void DTCServers::set_timeout(int n)
{
	this->m_time_out = n<=0 ? 5000 : n;
}

int DTCServers::construct_servers()
{
	if(this->m_ip_list.empty()) {
		log4cplus_error("ip list is empty!");
		set_error_msg(ER_SET_IPLIST_NULL, "DTCServers::construct_servers", "ip list is empty!");
		return -ER_SET_IPLIST_NULL;
	}
	
	if(this->m_access_token.empty() || (this->m_table_name == NULL) || (this->m_key_type == 0)) {
		log4cplus_error("m_access_token m_table_name or m_key_type is unset");
		set_error_msg(ER_SET_INSTANCE_PROPERTIES_ERR, "DTCServers::construct_servers", "m_access_token m_table_name or m_key_type is unset!");
		return -ER_SET_INSTANCE_PROPERTIES_ERR;
	}

	int i = 0;
	int ret = 0;
	char tmpPort[7];
	memset(tmpPort, 0, sizeof(tmpPort));	
	
	if(this->m_qos_severs) {
		delete[] this->m_qos_severs;
		this->m_qos_severs = NULL;
	}
 	int IPCount = this->m_ip_list.size();
	this->m_qos_severs = new DTCQosServer[IPCount];
	for( ; i < IPCount; ++i) {
		this->m_qos_severs[i].create_dtc_server();
		Server *server = this->m_qos_severs[i].get_dtc_server();
		sprintf(tmpPort, "%d", this->m_ip_list[i].port);
		ret = server->set_address(this->m_ip_list[i].ip, tmpPort);
		if(ret < 0) {
			if(this->m_qos_severs) {
				delete[] this->m_qos_severs;
				this->m_qos_severs = NULL;  
			}                      
			return -ret;
		}
		
		this->m_qos_severs[i].set_weight(this->m_ip_list[i].weight);
		this->m_qos_severs[i].set_status(this->m_ip_list[i].status);
		ret = server->set_table_name(this->m_table_name);
		if(ret < 0) {
			if(this->m_qos_severs) {
				delete[] this->m_qos_severs;
				this->m_qos_severs = NULL;  
			}                      
			return -ret;
		}
		server->set_accesskey(this->m_access_token.c_str());	
		switch(this->m_key_type) {
			case DField::Signed:
			case DField::Unsigned:
				server->int_key();
				break;
			case DField::String:
				server->string_key();
				break;
			case DField::Binary:
				server->string_key();
				break;
			case DField::Float:
			default:
				{
					log4cplus_error("key type is wrong!");
					if(this->m_qos_severs) {
						delete[] this->m_qos_severs;
						this->m_qos_severs = NULL;  
					}                      
					set_error_msg(ER_KEY_TYPE, "DTCServers::construct_servers", "key type is wrong!");
					return -ER_KEY_TYPE;
				}
				break;
		}	
		if(this->m_time_out > 0)
			server->set_timeout(this->m_time_out);
	}
	return 0;
}

int DTCServers::is_server_has_existed(ROUTE_NODE& ip)
{
	unsigned int i;
	for (i = 0; i < this->m_ip_list.size(); i++) {
		if((ip.port == this->m_ip_list[i].port) && (strncmp(ip.ip, this->m_ip_list[i].ip, IP_LENGHT) == 0))
			return i;
	}
	return -1;
}

int DTCServers::construct_servers2(std::vector<ROUTE_NODE>& IPList)
{
	if(IPList.empty()) {
		log4cplus_error("ip list is empty!");
		set_error_msg(ER_SET_IPLIST_NULL, "DTCServers::construct_servers2", "ip list is empty!");
		return -ER_SET_IPLIST_NULL;
	}
	if(this->m_access_token.empty() || (this->m_table_name == NULL) || (this->m_key_type == 0)) {
		log4cplus_error("m_access_token m_table_name or m_key_type is unset");
		set_error_msg(ER_SET_INSTANCE_PROPERTIES_ERR, "DTCServers::construct_servers2", "m_access_token m_table_name or m_key_type is unset!");
		return -ER_SET_INSTANCE_PROPERTIES_ERR;
	}

	int i = 0;
	int ret = 0;
	char tmpPort[7];
	memset(tmpPort, 0, sizeof(tmpPort));

	int IPCount = IPList.size();
	DTCQosServer* tmpQosServer = new DTCQosServer[IPCount];
	for ( ; i < IPCount; i++) {
		int idx = is_server_has_existed(IPList[i]);
		if (idx >= 0) {
			tmpQosServer[i].set_weight(IPList[i].weight);
			tmpQosServer[i].set_status(IPList[i].status);
			tmpQosServer[i].set_remove_time_stamp(this->m_qos_severs[idx].get_remove_time_stamp());
			tmpQosServer[i].set_last_remove_time(this->m_qos_severs[idx].get_last_remove_time());
			tmpQosServer[i].set_max_time_stamp(this->m_qos_severs[idx].get_max_time_stamp());
			tmpQosServer[i].set_dtc_server(this->m_qos_severs[idx].get_dtc_server());
			this->m_qos_severs[idx].reset_dtc_server();
		} else {
			tmpQosServer[i].create_dtc_server();
			Server *server = tmpQosServer[i].get_dtc_server();
			sprintf(tmpPort, "%d", IPList[i].port);
			ret = server->set_address(IPList[i].ip, tmpPort);
			if(ret < 0) {
				if(this->m_qos_severs)
				{
					delete[] this->m_qos_severs;
					this->m_qos_severs = NULL;  
				}   
				delete[] tmpQosServer;                  
				return -ret;
			}
			tmpQosServer[i].set_weight(IPList[i].weight);
			tmpQosServer[i].set_status(IPList[i].status);
			ret = server->set_table_name(this->m_table_name);
			if(ret < 0) {
				if(this->m_qos_severs) {
					delete[] this->m_qos_severs;
					this->m_qos_severs = NULL;  
				}
				delete[] tmpQosServer;                 
				return -ret;
			}
			server->set_accesskey(this->m_access_token.c_str());
			switch(this->m_key_type) {
				case DField::Signed:
				case DField::Unsigned:
					server->int_key();
					break;
				case DField::String:
					server->string_key();
					break;
				case DField::Binary:
					server->string_key();
					break;
				case DField::Float:
				default:
					{
						log4cplus_error("key type is wrong!");
						if(this->m_qos_severs) {
							delete[] this->m_qos_severs;
							this->m_qos_severs = NULL;  
						}
						delete[] tmpQosServer;
						set_error_msg(ER_KEY_TYPE, "DTCServers::construct_servers2", "key type is wrong!");
						return -ER_KEY_TYPE;
					}
					break;
			}
			if(this->m_time_out > 0)
				server->set_timeout(this->m_time_out);
		}
	}
	std::vector<ROUTE_NODE>().swap(this->m_ip_list);
	if(this->m_qos_severs) {
		delete[] this->m_qos_severs;
		this->m_qos_severs = NULL;
	}
	this->m_qos_severs = tmpQosServer;
	tmpQosServer = NULL;
	return 0;
}

void init_random()
{
	uint64_t ticks;
	struct timeval tv;
	int fd;
	gettimeofday(&tv, NULL);
	ticks = tv.tv_sec + tv.tv_usec;
	fd = open("/dev/urandom", O_RDONLY);
	if (fd > 0) {
		uint64_t r;
		int i;
		for (i = 0; i <100 ; i++) {
			read(fd, &r, sizeof(r));
			ticks += r;
		}
		close(fd);
	}
	srand(ticks);
}

unsigned int new_rand()
{
	int fd;
	unsigned int n = 0;
	fd = open("/dev/urandom", O_RDONLY);
	if(fd > 0)
		read(fd, &n, sizeof(n));
	close (fd);
	return n;
}

void DTCServers::disorder_list(int *tmpBuckets, int size)
{
    int randCount = 0;// 索引
    unsigned int position = 0;// 位置
    int k = 0;
//    srand((int)time(0));
    do{
        int r = size - randCount;
//        position = random(r);
		init_random();
		unsigned int tmp = new_rand();
        position = tmp%r;
        this->m_load_balance_buckets[k++] = tmpBuckets[position];
        randCount++; 
		 // 将最后一位数值赋值给已经被使用的position
        tmpBuckets[position] = tmpBuckets[r - 1];   
    }while(randCount < size);  
    return ; 
}

int DTCServers::construct_balance_buckets()
{
	if(!this->m_qos_severs || this->m_ip_list.empty()) {
		log4cplus_error("QOSSevers is null or ip count <0 ");
		set_error_msg(ER_ROUTE_INFO_NULL, "DTCServers::construct_balance_buckets", "QOSSevers is null or ip count <0!");
		return -ER_ROUTE_INFO_NULL;
	}

	int i = 0;
	int totalCount = 0;
	int IPCount = this->m_ip_list.size();
	for( ; i < IPCount; ++i) {
		totalCount += this->m_qos_severs[i].m_weight;	
	}
	FREE_IF(this->m_load_balance_buckets);
	this->m_load_balance_buckets = (int *)malloc(sizeof(int) * totalCount);
	this->m_balance_bucket_size = totalCount;
	int* tmpBuckets = (int *)malloc(sizeof(int) * totalCount);
	int j = 0;
	int pos = 0;
	for( ; j < IPCount; ++j) {
		int k = 0;
		for( ; k < this->m_qos_severs[j].m_weight; ++k) {
			tmpBuckets[pos] = j; 
			pos ++;
		}
	}
	this->m_buckets_pos = 0;
	disorder_list(tmpBuckets, totalCount);
//	FREE_IF(tmpBuckets);
	FREE_CLEAR(tmpBuckets);
	return 0;
} 

void DTCServers::remove_server_from_buckets(uint64_t now)
{
	int i = 0;
	int IPCount = this->m_ip_list.size();
	for( ; i < IPCount; ++i) {
		if(0 == this->m_qos_severs[i].get_status())
			continue;
		uint64_t errorCount = this->m_qos_severs[i].get_dtc_server()->get_error_count();
		if(errorCount >= DEFAULT_REMOVE_ERROR_COUNT) {
			this->m_qos_severs[i].get_dtc_server()->increase_remove_count();
			uint64_t removeTime = this->m_qos_severs[i].get_dtc_server()->get_remove_count() * DEFAULT_ROUTE_INTERVAL_TIME;
			if(removeTime >= DEFAULT_MAX_REMOVE_THRESHOLD_TIME)
				removeTime = DEFAULT_MAX_REMOVE_THRESHOLD_TIME;
			this->m_qos_severs[i].set_remove_time_stamp(removeTime);
			this->m_qos_severs[i].set_last_remove_time(now);
			this->m_qos_severs[i].set_status(0);
			log4cplus_debug("bid=[%d], remove time=[%lu], now=[%lu], address=[%s], pos=[%d]!", \
					this->m_bid, removeTime, now, this->m_qos_severs[i].get_dtc_server()->get_address(), i);
#ifdef DEBUG_INFO 
			printf("remove bid=[%d],now=[%lu],remvoe timeStamp=[%lu],remove count=[%d],address=[%s]!\n", \
					this->m_bid, now, removeTime, this->m_qos_severs[i].get_dtc_server()->get_remove_count(), \
					this->m_qos_severs[i].get_dtc_server()->get_address()); 
#endif
		}
	}
}

int DTCServers::refresh_balance_buckets(uint64_t now)
{
	int i = 0;
	int IPCount = this->m_ip_list.size();
	for( ; i < IPCount; ++i) {
		if(this->m_qos_severs[i].get_status() == 0) {
			if(this->m_qos_severs[i].get_last_remove_time() != 0) {
				if(now - this->m_qos_severs[i].get_last_remove_time() > this->m_qos_severs[i].get_remove_time_stamp()) {
					this->m_qos_severs[i].set_status(1);
					this->m_qos_severs[i].set_remove_time_stamp(0);
					this->m_qos_severs[i].set_last_remove_time(0);
					this->m_qos_severs[i].get_dtc_server()->clear_error_count();
					log4cplus_debug("bid=[%d], address=[%s], pos=[%d]!", \
							this->m_bid, this->m_qos_severs[i].get_dtc_server()->get_address(), i); 
#ifdef DEBUG_INFO 
					printf("refresh bid=[%d],address=[%s],now=[%lu]!\n", \
							this->m_bid, this->m_qos_severs[i].get_dtc_server()->get_address(), now);
#endif 
				}
			}
		}
	}
	return 0;
}

DTC::Server* DTCServers::get_one_server_from_buckets()
{
	int serverPos = 0;
	int lastBucketsPos = this->m_buckets_pos;
	do{
		++this->m_buckets_pos;
		if(this->m_buckets_pos >= this->m_balance_bucket_size)
			this->m_buckets_pos = 0;
		
		if(unlikely(!this->m_load_balance_buckets))
			return NULL;
		serverPos = this->m_load_balance_buckets[this->m_buckets_pos];
		if(this->m_qos_severs[serverPos].get_status()) {
#ifdef DEBUG_INFO 
			printf("get server address=[%s]\n", this->m_qos_severs[serverPos].get_dtc_server()->get_address());
#endif
			return this->m_qos_severs[serverPos].get_dtc_server();
		} else {
			/* 整租服务皆不可用，返回默认位置的server */
			if(lastBucketsPos == this->m_buckets_pos) {
				int tmpPos = this->m_load_balance_buckets[DEFAULT_SERVER_POS];
				return this->m_qos_severs[tmpPos].get_dtc_server();
			}
		}
	}while(1);
}

DTC::Server* DTCServers::get_server()
{
	int ret = 0;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	uint64_t now = tv.tv_sec; 
	if(this->m_set_route && (!this->m_ip_list.empty()) && (!this->m_constructed_by_set_i_ps)) {
#ifdef DEBUG_INFO 
		printf("construct server by set route\n");
#endif
		ret = construct_servers();
		if(ret < 0) {
			log4cplus_error("construct servers by set route failed! error code=[%d]", ret);
			set_error_msg(ret, "DTCServers::get_server", "construct servers by set route failed!");
			return NULL;
		}
		ret = construct_balance_buckets();
		if(ret < 0) {
			log4cplus_error("construct balance bucket failed! error code=[%d]", ret);
			set_error_msg(ret, "DTCServers::get_server", "construct balance bucket failed");
			return NULL;
		}
		this->m_constructed_by_set_i_ps = true;
#ifdef DEBUG_INFO 
		printf("get one server from set route\n");
#endif
		goto GetOneServer;
	}
	
	if(this->m_set_route && (!this->m_ip_list.empty()) && this->m_constructed_by_set_i_ps) {
#ifdef DEBUG_INFO 
		printf("get one server from set route\n");
#endif
		goto GetOneServer;
	}

	if(0 == this->m_bid_version) {
		uint64_t BidVersion = 0;
		int ret = get_version(&BidVersion);
		if(BidVersion <= 0) {
			log4cplus_info("get version from CC error!");
//			set_error_msg(ER_BID_VERSION_ERR, "DTCServers::get_server", "get version from CC error!");
			set_error_msg(ret, "DTCServers::get_server", "get version from CC error!");
			return NULL;
		}
		IP_ROUTE IPRoute;
		memset(&IPRoute, 0, sizeof(IP_ROUTE));
		ret = get_ip_route(this->m_bid, &IPRoute);
		if(ret < 0 || IPRoute.ip_num <= 0 || IPRoute.ip_list == NULL) {
			log4cplus_error("get ip list by bid failed! ip list is null!");
			set_error_msg(0, "DTCServers::get_server", "get ip list by bid failed! ip list is null!");
			free_ip_route(&IPRoute);
			return NULL;
		}
		int i = 0;
		std::vector<ROUTE_NODE>().swap(this->m_ip_list);
		for(; i < IPRoute.ip_num; ++i) {
			ROUTE_NODE RouteNode;
			RouteNode.bid = IPRoute.ip_list[i].bid;
			memcpy(RouteNode.ip, IPRoute.ip_list[i].ip, IP_LEN);
			RouteNode.port = IPRoute.ip_list[i].port;
			RouteNode.status = IPRoute.ip_list[i].status;
			RouteNode.weight = IPRoute.ip_list[i].weight;
#ifdef DEBUG_INFO
			print_ip_node(RouteNode);
#endif
			this->m_ip_list.push_back(RouteNode);	
		}
		free_ip_route(&IPRoute);

#ifdef DEBUG_INFO 
		printf("construct server by cc\n");
#endif
		ret = construct_servers();
		if(ret < 0) {
			log4cplus_error("construct servers failed!");
			set_error_msg(ret, "DTCServers::get_server", "construct servers failed!!");
			return NULL;
		}
		/* TODO cc version管理 */
		ret = construct_balance_buckets();
		if(ret < 0) {
			log4cplus_error("construct balance bucket failed! error code=[%d]", ret);
			set_error_msg(ret, "DTCServers::get_server", "construct balance bucket failed!");
			return NULL;
		}
#ifdef DEBUG_INFO 
		printf("get one server from cc\n");
#endif
		this->m_last_get_ca_time = now; 
		this->m_bid_version = BidVersion;
		goto GetOneServer;
	}
	
	if(now - this->m_last_get_ca_time >= DEFAULT_ROUTE_EXPIRE_TIME) {
		uint64_t BidVersion = 0;
		get_version(&BidVersion);
		this->m_last_get_ca_time = now;
		if(this->m_bid_version >= BidVersion) {
			log4cplus_info("the version is lastest!");
#ifdef DEBUG_INFO 
			printf("get one server from cc\n");
#endif
			goto GetOneServer;
		}
		IP_ROUTE IPRoute;
		memset(&IPRoute, 0, sizeof(IP_ROUTE));
		ret = get_ip_route(this->m_bid, &IPRoute); 
		if(ret < 0 || IPRoute.ip_num <= 0 || IPRoute.ip_list == NULL) {
			log4cplus_error("get ip list by bid failed! ip list is null!");
			set_error_msg(0, "DTCServers::get_server", "get ip list by bid failed! ip list is null!");
			free_ip_route(&IPRoute);
			return NULL;
		}

		int i = 0;
		std::vector<ROUTE_NODE> IPList;

		for(; i < IPRoute.ip_num; ++i) {
			ROUTE_NODE RouteNode;
			RouteNode.bid = IPRoute.ip_list[i].bid;
			memcpy(RouteNode.ip, IPRoute.ip_list[i].ip, IP_LEN);
			RouteNode.port = IPRoute.ip_list[i].port;
			RouteNode.status = IPRoute.ip_list[i].status;
			RouteNode.weight = IPRoute.ip_list[i].weight;
#ifdef DEBUG_INFO
			print_ip_node(RouteNode);
#endif
			IPList.push_back(RouteNode);	
		}
		free_ip_route(&IPRoute);

#ifdef DEBUG_INFO
		printf("construct server by cc\n");
#endif
		ret = construct_servers2(IPList);
		if(ret < 0) {
			std::vector<ROUTE_NODE>().swap(this->m_ip_list);
			this->m_ip_list = IPList;
			log4cplus_error("construct servers failed!");
			set_error_msg(ret, "DTCServers::get_server", "construct servers failed!");
			return NULL;
		}
		this->m_ip_list = IPList;
		ret = construct_balance_buckets();
		if(ret < 0) {
			log4cplus_error("construct balance bucket failed! error code=[%d]", ret);
			set_error_msg(ret, "DTCServers::get_server", "construct balance bucket failed!");
			return NULL;
		}
		this->m_bid_version = BidVersion;
#ifdef DEBUG_INFO
		printf("get one server from cc\n");
#endif
		goto GetOneServer;
	} else {
#ifdef DEBUG_INFO
		printf("get one server from cc\n");
#endif
		goto GetOneServer;
	}
	log4cplus_error("Exception process!");
	set_error_msg(0, "DTCServers::get_server", "Exception process!");
	return NULL;

GetOneServer:
	refresh_balance_buckets(now);
	remove_server_from_buckets(now);
	return get_one_server_from_buckets();
}
