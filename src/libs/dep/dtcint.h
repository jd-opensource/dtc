#ifndef __CHC_CLI_H
#define __CHC_CLI_H

#include <table/table_def.h>
#include <field/field_api.h>
#include <packet/packet.h>
#include <unistd.h>
#include <dtc_error_code.h>
#include <log/log.h>
#include "key_list.h"
#include <task/task_request.h>
#include <container.h>
#include <socket/socket_addr.h>
#include "udppool.h"
#include "algorithm/compress.h"
#include "poll/poller_base.h"
#include "lock/lock.h"
#include <map>

/*
 * Goal:
 * 	single transaction (*)
 * 	batch transaction (*)
 * 	async transaction
 */


#define HUNDRED 100
#define THOUSAND 1000
#define TENTHOUSAND 10000
#define HUNDREDTHOUSAND 100000
#define MILLION 1000000


enum E_REPORT_TYPE
{
	RT_MIN,
	RT_SHARDING,
	RT_ALL,
	RT_MAX
};

/* tp99 statistical interval, unit us */
static const uint32_t kpi_sample_count[] =
{
	200, 500, 1000,
	2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
	20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000,
	200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000,
	2000000, 500000, 10000000
};


class DtcJob;
class Packet;

class NCResult;
class NCResultInternal;
class NCPool;
struct NCTransaction;
class IDTCTaskExecutor;
class IDTCService;

class NCBase 
{
private:
	AtomicU32 count_;

public:
	NCBase(void) {}
	~NCBase(void) {}

public:
	int increase(void) { return ++count_; }
	int decrease(void) { return --count_; }
	int count(void) { return count_.get(); };
};

class NCRequest;
class DataConnector;

class NCServer: public NCBase  
{
public:
    /* base settings */
	SocketAddress addr_;
	char *tablename_;
	char *appname_;

	/* dtc set server_address_ and server_tablename_ for plugin */
    static char * server_address_;
    static char * server_tablename_;

	int keytype_;
	int auto_update_table_;
	int uto_reconnect_;
    static int network_mode_;
	NCKeyInfo keyinfo_;

	static DataConnector *data_connector_;

	/* error state */
	unsigned completed_:1;
	unsigned badkey_:1;
	unsigned badname_:1;
	unsigned autoping_:1;
	const char *error_str_;

	/* table definition */
	DTCTableDefinition *table_definition_;
	DTCTableDefinition *admin_tdef;  //==================================================================================================

	std::string access_token_;
	NCPool *owner_pool_;
	int owner_id_;
private: 
    /* serialNr manupulation */
	uint64_t last_serialnr_;
	/* timeout settings */
	int timeout_;
	int realtmo_;
	int compress_level_;
	
    /* sync execution */
	IDTCService         *service_;
	IDTCTaskExecutor    *executor_;
	int                 netfd;                                 
	time_t              last_action_;
	NCRequest           *ping_request_;

	uint64_t agent_time_;

	/* add by neolv to QOS */
	/* Keep original unchanged, add three parameters for QOS */
	uint64_t error_count_;
	/* Used to count total requests, total requests are only valid for one cycle */
	uint64_t total_count_;
	/* Used to count total request time, total count is only valid for one cycle */
	uint64_t total_elaps_;
	/* Number of times removed */
	int remove_count_;
public:	
	NCServer();
	NCServer(const NCServer &);
	~NCServer(void);

public:
    /* for compress */
    void set_compress_level(int level){compress_level_=level;}   
    int get_compress_level(void){return compress_level_;}   

	void clone_table_define(const NCServer& source);  
	int set_address(const char *h, const char *p=NULL);  
	int set_table_name(const char *);  
	/* this addres is set by user */
	const char * get_address(void) const { return addr_.Name(); }  
	/* this address is set by dtc */
	const char * get_server_address(void) const { return server_address_; }  
	const char * get_server_table_name(void) const { return server_tablename_;}
	int is_dgram(void) const { return addr_.socket_type()==SOCK_DGRAM; }  
	int is_udp(void) const { return addr_.socket_type()==SOCK_DGRAM && addr_.socket_family()==AF_INET; }  
	const char * get_table_name(void) const { return tablename_; }  
	int int_key(void); 
	int string_key(void); 
	int get_field_type(const char*);
	int is_completed(void) const { return completed_; } 
	int add_key(const char* name, uint8_t type);  
	int get_key_fields_count(void) const { return keyinfo_.get_key_fields_count() ? : keytype_ != DField::None ? 1 : 0; }  
	int allow_batch_key(void) const { return keyinfo_.get_key_fields_count(); }
	int simple_batch_key(void) const { return keyinfo_.get_key_fields_count()==1 && keytype_ == keyinfo_.get_key_type(0); }

	void set_auto_Update_table(bool autoUpdate){ auto_update_table_ = autoUpdate?1:0; }  
	void set_auto_reconnect(int reconnect){ uto_reconnect_ = reconnect; }  
	
	const char *get_error_message(void) const { return error_str_; }  

	void save_definition(NCResult *);  
	DTCTableDefinition* get_table_definition(int cmd) const;  
	int set_accesskey(const char *token);  

	uint64_t get_next_serialnr(void) { ++last_serialnr_; if(last_serialnr_==0) last_serialnr_++; return last_serialnr_; }  
	uint64_t get_last_serialnr(void) { return last_serialnr_; } 

	void set_timeout(int n); 
	int get_timeout(void) const { return timeout_; }  

	void set_agenttime(int t){agent_time_=t;}  
	uint64_t get_agenttime(void){return agent_time_;}  

	int Connect(void);  
	int reconnect(void); 
	void Close(void);   
	void set_fd(int fd) {  Close(); netfd = fd; update_timeout(); }  
	/* stream, connected */
	int send_packet_stream(Packet &);  
	int decode_result_stream(NCResult &); 
	/* dgram, connected or connectless */
	int send_packet_dgram(SocketAddress *peer, Packet &);
	int decode_result_dgram(SocketAddress *peer, NCResult &);
	/* connectless */
	NCUdpPort *get_global_port(void);  
	void put_global_port(NCUdpPort *);  

	void try_ping(void);
	int ping(void);
	void auto_ping(void) { if(!is_dgram()) autoping_ = 1; }
	NCResultInternal *execute_internal(NCRequest &rq, const DTCValue *kptr) { return executor_->task_execute(rq, kptr); }
	int has_internal_executor(void) const { return executor_ != 0; }  

	/* transaction manager, impl at dtcpool.cc */
	int async_connect(int &);  
	void set_owner(NCPool *, int);
	NCResult *decode_buffer(const char *, int);
	static int check_packet_size(const char *, int);

	void increase_error_count_() { ++error_count_; }  
	uint64_t get_error_count() { return error_count_; }  
	void clear_error_count() { error_count_ = 0; } 
    void increase_remove_count() { ++remove_count_; }  
	int get_remove_count() { return remove_count_; }
	void clear_remove_count() { remove_count_ = 0; }  
    void increase_total_request() { ++total_count_; }  
	uint64_t get_total_request() { return  total_count_; } 
	void clear_total_request() { total_count_ = 0; }
	void add_total_elaps(uint64_t iElaps) { total_elaps_ += iElaps; }  
	uint64_t get_total_elaps() { return total_elaps_; }  
	void clear_total_elaps() { total_elaps_ = 0; }  
private:
	int bind_temp_unix_socket(void);  
	void update_timeout(void);  
	void update_timeout_anyway(void);  
	/* this method is weak, and don't exist in libdtc.a */
	__attribute__((__weak__)) void check_internal_service(void);  
};



class NCRequest 
{
public:
	NCServer *server;
	uint8_t cmd;
	uint8_t haskey;
	uint8_t flags;
	int err;
	DTCValue key;
	NCKeyValueList key_value_list_;
	FieldValueByName ui;
	FieldValueByName ci;
	FieldSetByName fs;
	
	DTCTableDefinition *table_definition_;
	char *tablename_;
	int keytype_;

	unsigned int limitStart;
	unsigned int limitCount;
	int adminCode;

	uint64_t hotbackup_id;
	uint64_t master_hotbackup_timestamp_;
	uint64_t slave_hotbackup_timestamp_;

private:
	/* field flag */
	uint64_t compressFlag;
	DTCCompress *gzip;
	int init_compress(void);
	char errmsg_[1024];

public:
	NCRequest(NCServer *, int op);
	~NCRequest(void);
	
public:
	int attach_server(NCServer *);  

	void enable_no_cache(void) { flags |= DRequest::Flag::no_cache; }  
	void enable_no_next_server(void) { flags |= DRequest::Flag::no_next_server; } 
	void add_condition(void) { flags |= DRequest::Flag::NoResult; }  
	int add_condition(const char *n, uint8_t op, uint8_t t, const DTCValue &v);  
	int add_operation(const char *n, uint8_t op, uint8_t t, const DTCValue &v);
    int compress_set(const char *n,const char * v,int len);
    int compress_set_force(const char *n,const char * v,int len);
	int add_value(const char *n, uint8_t t, const DTCValue &v);  
	int need(const char *n, int);
	void limit(unsigned int st, unsigned int cnt) 
	{
		if(cnt==0)
			st = 0;
		limitStart = st;
		limitCount = cnt;
	}
	
	int set_key(int64_t);
	int set_key(const char *, int);
	int unset_key(void);
	int unset_key_value(void);
	int get_field_type(const char* name){ return server?server->get_field_type(name):DField::None; }
	int add_key_value(const char* name, const DTCValue &v, uint8_t type);
	int set_cache_id(int dummy) { return err = -EINVAL; }
	void set_admin_code(int code){ adminCode = code; }
	void set_hotbackup_id(uint64_t v){ hotbackup_id=v; }
	void set_master_hotbackup_timestamp(uint64_t t){ master_hotbackup_timestamp_=t; }
	void set_slave_hotbackup_timestamp(uint64_t t){ slave_hotbackup_timestamp_=t; }
	
	/* never return NULL */
	NCResult *do_execute(const DTCValue *key=NULL);  
	NCResult *execute_stream(const DTCValue *key=NULL);
	NCResult *execute_dgram(SocketAddress *peer, const DTCValue *key = NULL);
	NCResult *execute_network(const DTCValue *key=NULL);
	NCResult *execute_internal(const DTCValue *key=NULL);
	NCResult *do_execute(int64_t);
	NCResult *do_execute(const char *, int);
	/* sync execution */
	NCResult *precheck(const DTCValue *key); 
	/* need compress flag for read,or set compressFlag for write */
    int set_compress_field_name(void);
	int encode(const DTCValue *key, Packet *);
	/* return 1 if tdef changed... */
	int set_table_definition(void);

	int encode_buffer(char *&ptr, int&len, int64_t &magic, const DTCValue *key=NULL);
	int encode_buffer(char *&ptr, int&len, int64_t &magic, int64_t);
	int encode_buffer(char *&ptr, int&len, int64_t &magic, const char *, int);
    const char* get_error_message(void) const { return errmsg_; }

    int set_expire_time(const char* key, int t);
    int get_expire_time(const char* key);

private:
		int check_key(const DTCValue *kptr);
        int set_compress_flag(const char * name)
        {
            if (table_definition_==NULL)
                return -EC_NOT_INITIALIZED;
            if (table_definition_->field_id(name) >= 64) {
                snprintf(errmsg_, sizeof(errmsg_), "compress error:field id must less than 64"); 
                return -EC_COMPRESS_ERROR;
            }
            compressFlag|=(1<<table_definition_->field_id(name));
            return 0;
        }
};


class NCResultLocal 
{
public:
	const uint8_t *vidmap;
	long long apiTag;
	int maxvid;
	DTCCompress *gzip;

private:
	DTCTableDefinition* _tdef;
	uint64_t compressid;

public:
	NCResultLocal(DTCTableDefinition* tdef) :
		vidmap(NULL),
		apiTag(0),
		maxvid(0),
		gzip (NULL),
		_tdef(tdef),
	compressid(-1) {}

	virtual ~NCResultLocal(void) 
	{
		FREE_CLEAR(vidmap);
		DELETE (gzip);
	}

	virtual int field_id_virtual(int id) const { return id > 0 && id <= maxvid ? vidmap[id-1] : 0; } 

	virtual void set_api_tag(long long t) { apiTag = t; } 
	virtual long long get_api_tag(void) const { return apiTag; } 

	void set_virtual_map(FieldSetByName &fs) 
	{
		if(fs.max_virtual_id()) {
			fs.Resolve(_tdef, 0);
			vidmap = fs.virtual_map();
			maxvid = fs.max_virtual_id();
		}
	}

	virtual int init_compress()   
	{
		if(NULL == _tdef)
			return -EC_CHECKSUM_MISMATCH;

		int iret = 0;
		compressid = _tdef->compress_field_id();
		if(compressid<0) return 0;
		if(gzip == NULL)
			NEW(DTCCompress,gzip);
		if (gzip == NULL)
			return -ENOMEM;
		iret = gzip->set_buffer_len(_tdef->max_field_size());
		if(iret) 
			return iret;
		return 0;
	}
	virtual const int compress_id (void)const { return compressid; } 
};

class NCResult: public NCResultLocal, public DtcJob
{
public:
	NCResult(DTCTableDefinition *tdef) : NCResultLocal(tdef), DtcJob(tdef, TaskRoleClient, 0) 
	{
		if(tdef) tdef->increase();
		mark_allow_remote_table();
	}

	NCResult(int err, const char *from, const char *msg) : NCResultLocal(NULL), DtcJob(NULL, TaskRoleClient, 1) 
	{
		resultInfo.set_error_dup(err, from, msg);
	}
	virtual ~NCResult() 
	{
		DTCTableDefinition *tdef = table_definition();
		DEC_DELETE(tdef);
	}
};


class NCResultInternal: public NCResultLocal, public DTCJobOperation
{
public:
	NCResultInternal(DTCTableDefinition* tdef=NULL) : NCResultLocal (tdef){}

	virtual ~NCResultInternal() {}

	static inline int verify_class(void)  
	{
		NCResultInternal *ir = 0;
		NCResult *er = reinterpret_cast<NCResult *>(ir);

		NCResultLocal *il = (NCResultLocal *) ir;
		NCResultLocal *el = (NCResultLocal *) er;

		DtcJob *it = (DtcJob *) ir;
		DtcJob *et = (DtcJob *) er;

		long dl = reinterpret_cast<char *>(il) - reinterpret_cast<char *>(el);
		long dt = reinterpret_cast<char *>(it) - reinterpret_cast<char *>(et);
		return dl==0 && dt==0;
	}
};


/* Module reporting */
class DataConnector
{
private:
	struct businessStatistics
	{
		/* Total request time within 10s */
		uint64_t TotalTime;		
		/* Total request count within 10s */
		uint32_t TotalRequests;		
	public:
		businessStatistics(){ TotalTime = 0; TotalRequests = 0; }
	};
	struct bidCurve
	{
		uint32_t bid;
		uint32_t curve;
        bool operator < (const bidCurve &that) const
		{
			int sum1 = bid * 10 + curve;
			int sum2 = that.bid * 10 + that.curve;
			return sum1 < sum2;
        }
	};
	struct top_percentile_statistics
	{
		uint32_t uiBid;
		uint32_t uiAgentIP;
		uint16_t uiAgentPort;
		/* Total request count within 10s */
		uint32_t uiTotalRequests;	
		/* Total request time within 10s */
		uint64_t uiTotalTime;		
		/* Error count within 10s */
		uint32_t uiFailCount;	
		/* Maximum execution time within 10s */	
		uint64_t uiMaxTime;	
		/* Minimum execution time within 10s */	
		uint64_t uiMinTime;	
		/* Statistical values */		
		uint32_t statArr[sizeof(kpi_sample_count) / sizeof(kpi_sample_count[0])];	
		
		public:
		top_percentile_statistics()
		{
			uiBid = 0;
			uiAgentIP = 0;
			uiAgentPort = 0;
			uiTotalRequests = 0;
			uiTotalTime = 0;
			uiFailCount = 0;
			uiMaxTime = 0;
			uiMinTime = 0;
			memset(statArr, 0, sizeof(statArr));
		}
		top_percentile_statistics(const top_percentile_statistics &that)
		{
			this->uiBid = that.uiBid;
			this->uiAgentIP = that.uiAgentIP;
			this->uiAgentPort = that.uiAgentPort;
			this->uiTotalRequests = that.uiTotalRequests;
			this->uiTotalTime = that.uiTotalTime;
			this->uiFailCount = that.uiFailCount;
			this->uiMaxTime = that.uiMaxTime;
			this->uiMinTime = that.uiMinTime;
			memcpy(this->statArr, that.statArr, sizeof(this->statArr));
		}
		top_percentile_statistics &operator =(const top_percentile_statistics &that)
		{
			this->uiBid = that.uiBid;
			this->uiAgentIP = that.uiAgentIP;
			this->uiAgentPort = that.uiAgentPort;
			this->uiTotalRequests = that.uiTotalRequests;
			this->uiTotalTime = that.uiTotalTime;
			this->uiFailCount = that.uiFailCount;
			this->uiMaxTime = that.uiMaxTime;
			this->uiMinTime = that.uiMinTime;
			memcpy(this->statArr, that.statArr, sizeof(this->statArr));

			return *this;
		}
	};

private:
	std::map<bidCurve, businessStatistics> mapBi;
	/* Lock when reading/writing TotalTime and TotalRequests to prevent dirty data */
	Mutex lock_;			
	std::map<uint64_t, top_percentile_statistics> map_tp_stat_;
	/* Lock when reading/writing tp99 data to prevent dirty data */
	Mutex m_tp_lock;	
	static DataConnector *pDataConnector;
	pthread_t thread_id_;
	DataConnector();
	~DataConnector();

public:
    static DataConnector* getInstance()  
	{
		if( pDataConnector == NULL)
			pDataConnector = new DataConnector();
		return pDataConnector;
	};

	int send_data();
	int set_report_info(const std::string str, const uint32_t curve, const uint64_t t);   
	void get_report_info(std::map<bidCurve, businessStatistics> &mapData); 
	int set_bussiness_id(std::string str);
	
	int send_top_percentile_data(); 
	int set_top_percentile_data(const std::string strAccessKey, const std::string strAgentAddr, const uint64_t elapse, const int status);
	void get_top_percentile_data(std::map<uint64_t, top_percentile_statistics> &mapStat);  
	int set_top_percentile_config(std::string strAccessKey, const std::string strAgentAddr);
		
private:
	int parse_address(const std::string strAddr, uint64_t *uipIP = NULL, uint64_t *uipPort = NULL); 
};


class CTopPercentileSection
{
private:
	/* Execution time less than 1000us */
	static int16_t get_little_thousand(uint64_t elapse); 
	/* Execution time greater than or equal to 1000us and less than 10000us */
	static int16_t get_between_thousand_and_tenthousand(uint64_t elapse);
	/* Execution time greater than or equal to 10000us and less than 100000us */
	static int16_t get_between_tenthousand_and_hundredthousand(uint64_t elapse);
	/* Execution time greater than or equal to 100000us and less than 1000000us */
	static int16_t get_between_hundredthousand_and_million(uint64_t elapse);
	/* Execution time greater than or equal to 1000000us */
	static int16_t get_exceed_million(uint64_t elapse);
	
public:
	static int16_t get_tp_section(uint64_t elapse);
};
#endif



