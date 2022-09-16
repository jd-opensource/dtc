#include <stdio.h>
#include <getopt.h>
#include <signal.h>

#include "log.h"
#include "sharding_entry.h"
#include "async_conn_entry.h"
#include "main_entry.h"
#include "cold_wipe_entry.h"
#include "core_entry.h"
#include "agent_entry.h"
#include "proc_title.h"
#include "mxml.h"
#include "../agent/my/my_comm.h"

extern char cache_file[256];
extern char table_file[256];
#define ROOT_PATH "../conf/"
char agent_file[256] = "../conf/agent.xml";
std::map<std::string, std::string> map_dtc_conf; //key:value --> dtc addr:conf file name

#define DA_VERSION_MAJOR	1
#define DA_VERSION_MINOR	0
#define DA_VERSION_BUILD	1

#define DA_STRING_HELPER(str)	#str
#define DA_STRING(x)			DA_STRING_HELPER(x)

#define DA_VERSION_STR \
	DA_STRING(DA_VERSION_MAJOR)"."DA_STRING(DA_VERSION_MINOR)"."\
	DA_STRING(DA_VERSION_BUILD)

static int show_help;
static int show_version;
static int load_datalife;
static int load_agent;
static int load_asyncconn;
static int load_core;
int recovery_mode;
int load_sharding;
int load_all;

volatile int watchdog_stop = 0;

static struct option long_options[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'v' },
		{ "data-lifecycle", no_argument, NULL,'l' },
		{ "agent", no_argument, NULL,'a' },
		{ "async-conn", no_argument, NULL,'y' },
		{ "sharding", no_argument, NULL,'s' },
		{ "recovery", no_argument, NULL,'r' },
		{ "core", no_argument, NULL,'c' },
		{ NULL, 0, NULL, 0 } };

static char short_options[] = "hvlaycsr";


static int get_options(int argc, char **argv) {
	int c, value;

	opterr = 0;
	if(argc == 1)
		load_all = 1;

	for (;;) {
		c = getopt_long(argc, argv, short_options, long_options, NULL);
		if (c == -1) {
			/* no more options */
			break;
		}
		switch (c) {
		case 'h':
			show_version = 1;
			show_help = 1;
			break;
		case 'v':
			show_version = 1;
			break;
		case 'l':
			load_datalife = 1;
			break;
		case 'a':
			load_agent = 1;
			break;
		case 'y':
			load_asyncconn = 1;
			break;
		case 's':
			load_sharding = 1;
			break;		
		case 'r':
			recovery_mode = 1;
			break;					
		case 'c':
			load_core = 1;
			break;				
		default:
			break;
		}

	}
	return 0;
}


static void show_usage(void) {
	printf("Usage: dtc -[hvlaycsr], default loading ALL modules.\n");
	printf("Options:\n"); 
	printf("  -h, --help             		: this help\n");
	printf("  -v, --version          		: show version and exit\n");
	printf("  -a, --agent        			: load agent module\n");
	printf("  -c, --core        			: load dtc core module\n");
	printf("  -l, --data-lifecycle			: load data-lifecycle module\n");
	printf("  -y, --async-conn			: load async-conn module\n");
	printf("  -s, --sharding      			: load sharding module\n");
	printf("  -r, --recovery mode  			: auto restart when crashed\n");

}

int start_async_connector(WatchDog* wdog, int delay)
{
	// start full-data connector
	AsyncConnEntry *async_connector = new AsyncConnEntry(wdog, delay);
	if (async_connector == NULL) {
		log4cplus_error(
			"create Async-Connector object failed, msg: %m");
		return -1;
	}

	if (async_connector->new_proc_fork() < 0)
	{
		return -1;
	}

	return 0;
}

int start_sharding(WatchDog* wdog, int delay)
{
	// start full-data connector
	ShardingEntry *sharding = new ShardingEntry(wdog, delay);
	if (sharding == NULL) {
		log4cplus_error(
			"create ShardingEntry object failed, msg: %m");
		return -1;
	}

	if (sharding->new_proc_fork() < 0)
	{
		return -1;
	}

	return 0;
}

int start_data_lifecycle(WatchDog* wdog, int delay)
{
	// start cold-data earse process.
	DataLifeCycleEntry *colddata_connector = new DataLifeCycleEntry(wdog, delay);
	if (colddata_connector == NULL) {
		log4cplus_error(
			"create DataLifeCycleEntry object failed, msg: %m");
		return -1;
	}
	if (colddata_connector->new_proc_fork() < 0)
		return -1;

	return 0;
}

int start_core(WatchDog* wdog, int delay)
{
	// start dtcd core main process.
	CoreEntry *core_entry = new CoreEntry(wdog, delay);
	if (core_entry == NULL) {
		log4cplus_error(
			"create CoreEntry object failed, msg: %m");
		return -1;
	}
	if (core_entry->new_proc_fork() < 0)
		return -1;
	
	return 0;
}

int start_agent(WatchDog* wdog, int delay)
{
	// start agent main process.
	AgentEntry *agent_entry = new AgentEntry(wdog, delay);
	if (agent_entry == NULL) {
		log4cplus_error(
			"create AgentEntry object failed, msg: %m");
		return -1;
	}
	if (agent_entry->new_proc_fork() < 0)
		return -1;
	
	return 0;
}

static void sigterm_handler(int signo)
{
	watchdog_stop = 1;
}

int init_watchdog()
{
	struct sigaction sa;
	sigset_t sset;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = sigterm_handler;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);
	sigaction(SIGQUIT, &sa, NULL);
	sigaction(SIGHUP, &sa, NULL);

	sigemptyset(&sset);
	sigaddset(&sset, SIGTERM);
	sigaddset(&sset, SIGSEGV);
	sigaddset(&sset, SIGBUS);
	sigaddset(&sset, SIGABRT);
	sigaddset(&sset, SIGILL);
	//sigaddset(&sset, SIGCHLD);
	sigaddset(&sset, SIGFPE);
	sigprocmask(SIG_UNBLOCK, &sset, &sset);

	return 0;
}

bool ParseAgentConf(std::string path){
    FILE *fp = fopen(path.c_str(), "r");
    if (fp == NULL) {
        log4cplus_error("conf: failed to open configuration '%s': %s", path.c_str(), strerror(errno));
        return false;
    }
    mxml_node_t* tree = mxmlLoadFile(NULL, fp, MXML_TEXT_CALLBACK);
    if (tree == NULL) {
        log4cplus_error("mxmlLoadFile error, file: %s", path.c_str());
        return false;
    }
    fclose(fp);

	mxml_node_t *poolnode, *servernode, *instancenode, *lognode;

	for (poolnode = mxmlFindElement(tree, tree, "MODULE",
	NULL, NULL, MXML_DESCEND); poolnode != NULL;
			poolnode = mxmlFindElement(poolnode, tree, "MODULE",
			NULL, NULL, MXML_DESCEND)) 
	{
		
		for (servernode = mxmlFindElement(poolnode, poolnode, "CACHESHARDING",
		NULL, NULL, MXML_DESCEND); servernode != NULL; servernode =
				mxmlFindElement(servernode, poolnode, "CACHESHARDING",
				NULL, NULL, MXML_DESCEND)) 
		{
			char *nodev = (char *)mxmlElementGetAttr(servernode, "ShardingName");
			if (nodev == NULL) {
				return false;
			}
			if(strcmp(nodev, "complex") == 0)
				continue;

			for (instancenode = mxmlFindElement(servernode, servernode, "INSTANCE",
				NULL, NULL, MXML_DESCEND); instancenode != NULL; instancenode =
				mxmlFindElement(instancenode, servernode, "INSTANCE",
					NULL, NULL, MXML_DESCEND)) 
				{
			
					char *argment = (char *)mxmlElementGetAttr(instancenode, "Enable");
					if (argment == NULL) {
						return false;
					}
					if(strcmp(argment, "true") != 0)
						continue;

					argment = (char *)mxmlElementGetAttr(instancenode, "Addr");
					if (argment == NULL) {
						return false;
					}

					std::string listen_on = argment;
					log4cplus_debug("addr:%s %s", argment, (char *)mxmlElementGetAttr(poolnode, "Mid"));
					std::string::size_type pos = listen_on.find_last_of(":");
					if(pos == std::string::npos){
						log4cplus_error("string find error, file: %s", path.c_str());
						return false;
					}
					std::string addr = listen_on.substr(0, pos);
					char filename[250] = {0};
					sprintf(filename, "dtc-conf-%d.yaml", atoi((char *)mxmlElementGetAttr(poolnode, "Mid")));
					map_dtc_conf[addr] = filename;

				}
		}
	}

    mxmlDelete(tree);
    
    return true;
}

std::string send_select_dtcyaml(const char* serverIp, int port)
{
	log4cplus_debug("server ip:%s, port:%d", serverIp, port);
    sockaddr_in sendSockAddr;   
    bzero((char*)&sendSockAddr, sizeof(sendSockAddr)); 
    sendSockAddr.sin_family = AF_INET; 
    sendSockAddr.sin_addr.s_addr = inet_addr(serverIp);
    sendSockAddr.sin_port = htons(port);
    int clientSd = socket(AF_INET, SOCK_STREAM, 0);
    int status = connect(clientSd,
                         (sockaddr*) &sendSockAddr, sizeof(sendSockAddr));
    if(status < 0)
    {
        log4cplus_error("Error connecting to socket!");
		return "";
    }
    log4cplus_debug("Connected to the server!");
    int bytesRead = 0, bytesWritten = 0;
	char greeting[500] = {0};
	bytesRead = recv(clientSd, (char*)&greeting, sizeof(greeting), 0);
	log4cplus_debug("greeting msg len:%d", bytesRead);
	char send_msg[1024] = {0};
	int len = 0;
	struct DTC_HEADER_V2 send_header = {0};
	int cmd_len = strlen("select dtcyaml");
	send_header.version = 2;
	send_header.packet_len = MYSQL_HEADER_SIZE + 3 + cmd_len + sizeof(send_header);
	memcpy(send_msg, &send_header, sizeof(struct DTC_HEADER_V2));
	len += sizeof(struct DTC_HEADER_V2);
	uint8_t send_mysql_header[MYSQL_HEADER_SIZE] = {0};
	int_conv_3(send_mysql_header, (uint)cmd_len+3);
	send_mysql_header[3] = 0; //pkt_nr;
	memcpy(send_msg + sizeof(struct DTC_HEADER_V2), send_mysql_header, MYSQL_HEADER_SIZE);
	len += MYSQL_HEADER_SIZE;
	send_msg[sizeof(struct DTC_HEADER_V2) + MYSQL_HEADER_SIZE] = 0x03; //query
	send_msg[sizeof(struct DTC_HEADER_V2) + MYSQL_HEADER_SIZE + 1] = 0x0;
	send_msg[sizeof(struct DTC_HEADER_V2) + MYSQL_HEADER_SIZE + 2] = 0x01;
	len += 3;
	memcpy(send_msg + sizeof(struct DTC_HEADER_V2) + MYSQL_HEADER_SIZE + 3, "select dtcyaml", cmd_len);
	len += cmd_len;
	bytesWritten = send(clientSd, send_msg, len, 0);
	log4cplus_debug("Awaiting server response..., sent len:%d", len);
	struct DTC_HEADER_V2 header = {0};
	bytesRead = recv(clientSd, (char*)&header, sizeof(header), 0);
	log4cplus_debug("bytesRead: %d, packet len: %d, dbname len:%d ver: %d admin: %d, layer: %d, id: %d", 
			bytesRead, header.packet_len, header.dbname_len, header.version, header.admin, header.layer, header.id);
	std::string conf_str = "";
	if(header.packet_len > 0 && header.packet_len <= 65535)
	{
		int ilen = header.packet_len - sizeof(header) + 1;
		char* msg = new char[ilen];
		memset(msg, 0, ilen);
		log4cplus_debug("ilen: %d", ilen);
		bytesRead = recv(clientSd, (char*)msg, ilen - 1, 0);
		log4cplus_debug("bytesRead: %d", bytesRead);
		conf_str = msg;
		delete msg;
	}
	else
	{
		log4cplus_error("header.packet_len: %d error", header.packet_len);
	}

    close(clientSd);
    log4cplus_debug("Bytes written: %d, Bytes read: %d, Connection closed", bytesWritten, bytesRead);
    return conf_str;
}

int get_all_dtc_confs()
{
	//load xml
	if(false == ParseAgentConf(agent_file)){
        log4cplus_error("DataConf ParseConf error.");
        return -1;
    }

	//get each dtc instance conn
	int success_num = 0;
	std::map<std::string, std::string>::iterator it;
	for(it = map_dtc_conf.begin(); it != map_dtc_conf.end(); it++)
	{
		std::string addr = (*it).first;
		std::string filename = (*it).second;

		char* content = NULL;
		int content_len = 0;
		log4cplus_debug("addr:%s", addr.c_str());
		std::string str = send_select_dtcyaml((addr.substr(0, addr.find(':'))).c_str(), atoi((addr.substr(addr.find(':')+1)).c_str()));
		content = str.c_str();
		content_len = str.length();
		log4cplus_debug("content:%s", content);

		//save conf file and rename.
		if(content != NULL && content_len > 0)
		{
			std::string filepath = string(ROOT_PATH) + filename;
			log4cplus_debug("filepath:%s", filepath.c_str());
			FILE *fp = fopen(filepath.c_str(), "w");
			if (fp == NULL)
			{
				log4cplus_error("open file %s error", filepath.c_str());
				continue;
			}

			fprintf(fp, "%s", content);

			fclose(fp);
			success_num++;
		}
	}

	if(success_num > 0)
		return 0;
	else
		return -2;
}


int main(int argc, char* argv[])
{
	int status;
	WatchDog* wdog = new WatchDog;
	int delay = 5;

	init_log4cplus();
	init_proc_title(argc, argv);

	signal(SIGCHLD, SIG_DFL);

	status = get_options(argc, argv);
	if (status != 0) {
		show_usage();
		exit(1);
	}
	if (show_version) {
		printf("This is dtc watchdog -%s\n", DA_VERSION_STR);
		if (show_help) {
			show_usage();
		}
		exit(0);
	}

	if(load_agent || load_sharding || load_asyncconn || load_all)
	{
		if(get_all_dtc_confs() < 0)
		{
			log4cplus_error("get dtc conf files failed, process exit right now.");
			exit(0);
		}
	}

	if (load_sharding || load_all) {
		if(start_sharding(wdog, delay) < 0)
			log4cplus_error("start sharding failed.");
	}

	if (load_asyncconn || load_all) {
		if(start_async_connector(wdog, delay) < 0)
			log4cplus_error("start full-data failed.");
	}

	if (load_core) {
		if(start_core(wdog, delay) < 0)
			log4cplus_error("start core failed.");
	}

	if (load_agent || load_all) {
		sleep(2);
		if(start_agent(wdog, delay) < 0)
			log4cplus_error("start full-data failed.");
	}

	if (load_datalife || load_all) {
		log4cplus_info("waitting for 5s.");
		sleep(5);
		if(start_data_lifecycle(wdog, delay) < 0)
			log4cplus_error("start data_lifecycle failed.");
	}

	if(init_watchdog() < 0)
	{
		log4cplus_error("init daemon error");
		return -1;
	}

	wdog->run_loop();

	return 0;
}