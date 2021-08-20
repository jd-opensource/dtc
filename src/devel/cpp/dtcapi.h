#ifndef __DTC_API_H__
#define __DTC_API_H__

#include <stdlib.h>
#include <vector>
#include <string>
#include <stdint.h>

#define IP_LENGHT 16

namespace DTC 
{

	typedef struct route_node
	{
		int bid;
		int port;
		int status;
		int weight;
		char ip[IP_LENGHT];
	}ROUTE_NODE;

	class Server;
	class ServerPool;
	class Request;
	class Result;

	const int kRequestSvrAdmin 	= 3;
	const int kRequestGet 		= 4;
	const int kRequestPurge 		= 5;
	const int kRequestInsert 	= 6;
	const int kRequestUpdate 	= 7;
	const int kRequestDelete 	= 8;
	const int kRequestReplace 	= 12;
	const int kRequestFlush 		= 13;
	const int kRequestInvalidate	= 14;

	/* mem monior Request;2014/06/6;by seanzheng */
	const int kRequestMonitor    = 15;

	/* sub-code of admin cmd */
	const int RegisterHB		= 1;
	const int LogoutHB		= 2;
	const int GetKeyList		= 3;
	const int GetUpdateKey		= 4;
	const int GetRawData		= 5;
	const int ReplaceRawData	= 6;
	const int AdjustLRU        	= 7;
	const int VerifyHBT		= 8;
	const int GetHBTime		= 9;
	const int kSetReadOnly	= 10;
	const int kSetReadWrite	= 11;
	const int kQueryBinlogID		= 12;
	const int kNodeHandleChange  = 13;
	const int Migrate = 14;
    const int kReloadClusterNodeList = 15;
    const int kSetClusterNodeState  = 16;
    const int kChangeNodeAddress  = 17;
    const int kGetClusterState  = 18;
    const int kPurgeForHit    = 19;
	const int ClearCache = 21;
	const int ColExpandStatus = 24;
	const int kColExpand = 25;
	const int ColExpandDone = 26;
	const int ColExpandKey = 27;
	 
	/* undefined */
	const int kKeyTypeNone		= 0;	
	/* Signed Integer */
	const int kKeyTypeInt		= 1;	
	/* String, case insensitive, null ended */
	const int kKeyTypeString		= 4;	
	/* undefined */
	const int kFieldTypeNone		= 0;    
	/* Signed Integer */
	const int kFieldTypeSigned	= 1;
	/* Unsigned Integer */
	const int kFieldTypeUnsigned	= 2;	
	/* float */
	const int kFieldTypeFloat	= 3;	
	/* String, case insensitive, null ended */
	const int kFieldTypeString	= 4;	
	/* binary */
	const int kFieldTypeBinary	= 5;	

	void init_log (const char *app, const char *dir = NULL);
	void set_log_level(int n);
	/* 设置批量操作一次最多多少个key(默认最多32个) */
	int  set_key_value_max(unsigned int count); 

	class Result;
	class Server 
	{
	private:
		void *addr_;
		long check;
	public:
		friend class Request;
		friend class Result;
		friend class ServerPool;
		friend class QOSServerPool;

		Server(void);
		~Server(void);
		Server(const Server &);
		void clone_table_define(const Server& source);
		int set_address(const char *host, const char *port=0);
		int set_table_name(const char *);
		/* for compress */
		void set_compress_level(int);
		/* get address and tablename set by user */
		const char * get_address(void) const;
		const char * get_table_name(void) const;
		/* get address and tablename set by dtc frame,for plugin only; */
		const char * get_server_address(void) const;
		const char * get_server_table_name(void) const;
		int int_key(void);
		int binary_key(void);
		int string_key(void);
		int add_key(const char* name, int type);
		int get_field_type(const char* name);
		const char *get_error_message(void) const;
		void SetTimeout(int);
		void set_timeout(int);
		int Connect(void);
		void Close(void);
		int ping(void);
		void auto_ping(void);
		/* UNSUPPORTED API */
		void set_fd(int); 
		void set_auto_Update_table(bool autoUpdate);
		void set_auto_reconnect(int auto_reconnect);
		int decode_packet(Result &, const char *, int);
		int check_packet_size(const char *, int);

		void set_accesskey(const char *token);

	public:
		void increase_error_count_();
		uint64_t get_error_count();
		void clear_error_count();
		
		void increase_remove_count();
		int get_remove_count();
		void clear_remove_count();

		void increase_total_request();
		uint64_t get_total_request();
		void clear_total_request();

		void add_total_elaps(uint64_t iElaps);
		uint64_t get_total_elaps();
		void clear_total_elaps();
	};

	class DTCQosServer;

	class DTCServers
	{
		public:
			DTCServers();
			~DTCServers();

		private:
			DTCServers(const DTCServers&);
			DTCServers& operator=(const DTCServers&);

		public:
			int set_route_list(std::vector<ROUTE_NODE>& ip_list);
			void set_idc_no(int IDCNo);
			Server* get_server();

			int set_accesskey(const char *token);
			int set_table_name(const char *tableName);
		    /**
			 * Signed=1,  Signed Integer
			 * String=4,  String, case insensitive, null ended
			 * Binary=5,  opaque binary data
			 */
			int set_key_type(int type);
			void set_agenttime(int t);
			void set_timeout(int n);
			std::string get_error_msg();

		private:
			int is_server_has_existed(ROUTE_NODE& ip);
			int construct_servers();
			int construct_servers2(std::vector<ROUTE_NODE>& IPList);
			void disorder_list(int *tmpBuckets, int size);
			int construct_balance_buckets();
			int refresh_balance_buckets(uint64_t );			
			void remove_server_from_buckets(uint64_t );
			Server* get_one_server_from_buckets();
			void set_error_msg(int err, std::string from, std::string msg);

		private:
			int m_time_out;
			int m_agent_time; 
			int m_key_type;
			char* m_table_name;
			std::string m_access_token;
			std::string m_err_msg;

		private:
			bool m_set_route;	
			bool m_constructed_by_set_i_ps;
			int m_bid;
			int m_idc_no;
			int m_buckets_pos;
			int m_balance_bucket_size;
			uint64_t m_bid_version;
			uint64_t m_last_get_ca_time;
			uint64_t m_refresh_buckets_time;
			uint64_t m_remove_buckets_time;
			int* m_load_balance_buckets;
			DTCQosServer* m_qos_severs;
			std::vector<ROUTE_NODE> m_ip_list;
	};

	class Request 
	{
		private:
			void *addr_;
			long check;
			Request(const Request &);
		public:
			friend class Server;
			friend class Result;
			friend class ServerPool;

			Request(Server *srv, int op);
			Request(void);
			~Request(void);
			void Reset(void);
			void Reset(int);
			int attach_server(Server *srv);

			void set_admin_code(int code);
			void set_hotbackup_id(long long);
			void set_master_hotbackup_timestamp(long long);
			void set_slave_hotbackup_timestamp(long long);

#define _API_DEFINE_LONG_(op, t) int op(const char *n, t a) { return op(n, (long long)a); }
#define _API_DEFINE_FLOAT_(op, t) int op(const char *n, t a) { return op(n, (double)a); }
			int need(const char *);
			int need(const char *, int);
			int get_field_type(const char*);
			void no_cache(void);
			void no_next_server(void);
			void limit(unsigned int, unsigned int);
			int EQ(const char *, long long);
			int NE(const char *, long long);
			int LT(const char *, long long);
			int LE(const char *, long long);
			int GT(const char *, long long);
			int GE(const char *, long long);
			int EQ(const char *, const char *);
			int NE(const char *, const char *);
			int EQ(const char *, const char *, int);
			int NE(const char *, const char *, int);

			_API_DEFINE_LONG_(EQ, unsigned long long);
			_API_DEFINE_LONG_(EQ, long);
			_API_DEFINE_LONG_(EQ, unsigned long);
			_API_DEFINE_LONG_(EQ, int);
			_API_DEFINE_LONG_(EQ, unsigned int);
			_API_DEFINE_LONG_(EQ, short);
			_API_DEFINE_LONG_(EQ, unsigned short);
			_API_DEFINE_LONG_(EQ, char);
			_API_DEFINE_LONG_(EQ, unsigned char);

			_API_DEFINE_LONG_(NE, unsigned long long);
			_API_DEFINE_LONG_(NE, long);
			_API_DEFINE_LONG_(NE, unsigned long);
			_API_DEFINE_LONG_(NE, int);
			_API_DEFINE_LONG_(NE, unsigned int);
			_API_DEFINE_LONG_(NE, short);
			_API_DEFINE_LONG_(NE, unsigned short);
			_API_DEFINE_LONG_(NE, char);
			_API_DEFINE_LONG_(NE, unsigned char);

			_API_DEFINE_LONG_(GT, unsigned long long);
			_API_DEFINE_LONG_(GT, long);
			_API_DEFINE_LONG_(GT, unsigned long);
			_API_DEFINE_LONG_(GT, int);
			_API_DEFINE_LONG_(GT, unsigned int);
			_API_DEFINE_LONG_(GT, short);
			_API_DEFINE_LONG_(GT, unsigned short);
			_API_DEFINE_LONG_(GT, char);
			_API_DEFINE_LONG_(GT, unsigned char);

			_API_DEFINE_LONG_(GE, unsigned long long);
			_API_DEFINE_LONG_(GE, long);
			_API_DEFINE_LONG_(GE, unsigned long);
			_API_DEFINE_LONG_(GE, int);
			_API_DEFINE_LONG_(GE, unsigned int);
			_API_DEFINE_LONG_(GE, short);
			_API_DEFINE_LONG_(GE, unsigned short);
			_API_DEFINE_LONG_(GE, char);
			_API_DEFINE_LONG_(GE, unsigned char);

			_API_DEFINE_LONG_(LT, unsigned long long);
			_API_DEFINE_LONG_(LT, long);
			_API_DEFINE_LONG_(LT, unsigned long);
			_API_DEFINE_LONG_(LT, int);
			_API_DEFINE_LONG_(LT, unsigned int);
			_API_DEFINE_LONG_(LT, short);
			_API_DEFINE_LONG_(LT, unsigned short);
			_API_DEFINE_LONG_(LT, char);
			_API_DEFINE_LONG_(LT, unsigned char);

			_API_DEFINE_LONG_(LE, unsigned long long);
			_API_DEFINE_LONG_(LE, long);
			_API_DEFINE_LONG_(LE, unsigned long);
			_API_DEFINE_LONG_(LE, int);
			_API_DEFINE_LONG_(LE, unsigned int);
			_API_DEFINE_LONG_(LE, short);
			_API_DEFINE_LONG_(LE, unsigned short);
			_API_DEFINE_LONG_(LE, char);
			_API_DEFINE_LONG_(LE, unsigned char);

			int Set(const char *, long long);
			int OR(const char *, long long);
			int Add(const char *, long long);
			int Sub(const char *, long long);
			int Set(const char *, double);
			int Add(const char *, double);
			int Sub(const char *, double);
			int Set(const char *, const char *);
			int Set(const char *, const char *, int);

			/* just for compress,only support binary field */
			int compress_set(const char *, const char *, int);
			/* just compress and set. Don't need compressflag */
			int compress_set_force(const char *, const char *, int);
			/* bits op */
			int set_multi_bits(const char *, int, int, unsigned int);
			int set_bit  (const char *f, int o) { return set_multi_bits(f, o, 1, 1);}
			int clear_bit(const char *f, int o) { return set_multi_bits(f, o, 1, 0);}

			_API_DEFINE_LONG_(Set, unsigned long long);
			_API_DEFINE_LONG_(Set, long);
			_API_DEFINE_LONG_(Set, unsigned long);
			_API_DEFINE_LONG_(Set, int);
			_API_DEFINE_LONG_(Set, unsigned int);
			_API_DEFINE_LONG_(Set, short);
			_API_DEFINE_LONG_(Set, unsigned short);
			_API_DEFINE_LONG_(Set, char);
			_API_DEFINE_LONG_(Set, unsigned char);
			_API_DEFINE_FLOAT_(Set, float);
			_API_DEFINE_FLOAT_(Set, long double);

			_API_DEFINE_LONG_(OR, unsigned long long);
			_API_DEFINE_LONG_(OR, long);
			_API_DEFINE_LONG_(OR, unsigned long);
			_API_DEFINE_LONG_(OR, int);
			_API_DEFINE_LONG_(OR, unsigned int);
			_API_DEFINE_LONG_(OR, short);
			_API_DEFINE_LONG_(OR, unsigned short);

			_API_DEFINE_LONG_(Add, unsigned long long);
			_API_DEFINE_LONG_(Add, long);
			_API_DEFINE_LONG_(Add, unsigned long);
			_API_DEFINE_LONG_(Add, int);
			_API_DEFINE_LONG_(Add, unsigned int);
			_API_DEFINE_LONG_(Add, short);
			_API_DEFINE_LONG_(Add, unsigned short);
			_API_DEFINE_LONG_(Add, char);
			_API_DEFINE_LONG_(Add, unsigned char);
			_API_DEFINE_FLOAT_(Add, float);
			_API_DEFINE_FLOAT_(Add, long double);

			_API_DEFINE_LONG_(Sub, unsigned long long);
			_API_DEFINE_LONG_(Sub, long);
			_API_DEFINE_LONG_(Sub, unsigned long);
			_API_DEFINE_LONG_(Sub, int);
			_API_DEFINE_LONG_(Sub, unsigned int);
			_API_DEFINE_LONG_(Sub, short);
			_API_DEFINE_LONG_(Sub, unsigned short);
			_API_DEFINE_LONG_(Sub, char);
			_API_DEFINE_LONG_(Sub, unsigned char);
			_API_DEFINE_FLOAT_(Sub, float);
			_API_DEFINE_FLOAT_(Sub, long double);
#undef _API_DEFINE_LONG_

			void unset_key(void);
			int set_key(long long);
			int set_key(const char *);
			int set_key(const char *, int);
#define _API_DEFINE_LONG_(t) int set_key(t a) { return set_key((long long)a); }
			_API_DEFINE_LONG_(unsigned long long);
			_API_DEFINE_LONG_(long);
			_API_DEFINE_LONG_(unsigned long);
			_API_DEFINE_LONG_(int);
			_API_DEFINE_LONG_(unsigned int);
			_API_DEFINE_LONG_(short);
			_API_DEFINE_LONG_(unsigned short);
			_API_DEFINE_LONG_(char);
			_API_DEFINE_LONG_(unsigned char);
#undef _API_DEFINE_LONG_

			int add_key_value(const char* name, long long v);
			int add_key_value(const char* name, const char *str);
			int add_key_value(const char* name, const char *ptr, int len);
#define _API_DEFINE_LONG_(t) int add_key_value(const char* name, t a) { return add_key_value(name, (long long)a); }
			_API_DEFINE_LONG_(unsigned long long);
			_API_DEFINE_LONG_(long);
			_API_DEFINE_LONG_(unsigned long);
			_API_DEFINE_LONG_(int);
			_API_DEFINE_LONG_(unsigned int);
			_API_DEFINE_LONG_(short);
			_API_DEFINE_LONG_(unsigned short);
			_API_DEFINE_LONG_(char);
			_API_DEFINE_LONG_(unsigned char);
#undef _API_DEFINE_LONG_

			Result *do_execute(void);
			Result *do_execute(long long);
			Result *do_execute(const char *);
			Result *do_execute(const char *, int);

#define _API_DEFINE_LONG_(t) Result *do_execute(t a) { return do_execute((long long)a); }
			_API_DEFINE_LONG_(unsigned long long);
			_API_DEFINE_LONG_(long);
			_API_DEFINE_LONG_(unsigned long);
			_API_DEFINE_LONG_(int);
			_API_DEFINE_LONG_(unsigned int);
			_API_DEFINE_LONG_(short);
			_API_DEFINE_LONG_(unsigned short);
			_API_DEFINE_LONG_(char);
			_API_DEFINE_LONG_(unsigned char);
#undef _API_DEFINE_LONG_

			int do_execute(Result&);
			int do_execute(Result&, long long);
			int do_execute(Result&, const char *);
			int do_execute(Result&, const char *, int);

#define _API_DEFINE_LONG_(t) int do_execute(Result &r, t a) { return do_execute(r, (long long)a); }
			_API_DEFINE_LONG_(unsigned long long);
			_API_DEFINE_LONG_(long);
			_API_DEFINE_LONG_(unsigned long);
			_API_DEFINE_LONG_(int);
			_API_DEFINE_LONG_(unsigned int);
			_API_DEFINE_LONG_(short);
			_API_DEFINE_LONG_(unsigned short);
			_API_DEFINE_LONG_(char);
			_API_DEFINE_LONG_(unsigned char);
#undef _API_DEFINE_LONG_

			int encode_packet(char *&, int&, long long&);
			int encode_packet(char *&, int&, long long&, long long);
			int encode_packet(char *&, int&, long long&, const char *);
			int encode_packet(char *&, int&, long long&, const char *, int);

#define _API_DEFINE_LONG_(t) int encode_packet(char *&p, int &l, long long &m, t a) { return encode_packet(p,l,m,(long long)a); }
			_API_DEFINE_LONG_(unsigned long long);
			_API_DEFINE_LONG_(long);
			_API_DEFINE_LONG_(unsigned long);
			_API_DEFINE_LONG_(int);
			_API_DEFINE_LONG_(unsigned int);
			_API_DEFINE_LONG_(short);
			_API_DEFINE_LONG_(unsigned short);
			_API_DEFINE_LONG_(char);
			_API_DEFINE_LONG_(unsigned char);
#undef _API_DEFINE_LONG_

			int set_cache_id(long long);
#define _API_DEFINE_LONG_(t) int set_cache_id(t a) {return set_cache_id((long long)a);}
			_API_DEFINE_LONG_(unsigned long long);
			_API_DEFINE_LONG_(long);
			_API_DEFINE_LONG_(unsigned long);
			_API_DEFINE_LONG_(int);
			_API_DEFINE_LONG_(unsigned int);
			_API_DEFINE_LONG_(short);
			_API_DEFINE_LONG_(unsigned short);
			_API_DEFINE_LONG_(char);
			_API_DEFINE_LONG_(unsigned char);
#undef _API_DEFINE_LONG_
            const char *get_error_message(void) const;
			/* 无源模式超时时间 */
			int set_expire_time(const char* key, int time);
			int get_expire_time(const char* key);
	};

	class GetRequest : public Request 
	{
	public:
		GetRequest(Server *srv): Request(srv, kRequestGet) {}
		GetRequest(): Request((Server *)0, kRequestGet) {}
	};

	class InsertRequest : public Request 
	{
	public:
		InsertRequest(Server *srv) : Request(srv, kRequestInsert) {}
		InsertRequest() : Request((Server *)0, kRequestInsert) {}
	};

	class DeleteRequest : public Request 
	{
	public:
		DeleteRequest(Server *srv) : Request(srv, kRequestDelete) {}
		DeleteRequest() : Request((Server *)0, kRequestDelete) {}
	};

	class UpdateRequest : public Request 
	{
	public:
		UpdateRequest(Server *srv) : Request(srv, kRequestUpdate) {}
		UpdateRequest() : Request((Server *)0, kRequestUpdate) {}
	};

	class PurgeRequest : public Request 
	{
	public:
		PurgeRequest(Server *srv) : Request(srv, kRequestPurge) {}
		PurgeRequest() : Request((Server *)0, kRequestPurge) {}
	};

	class ReplaceRequest:public Request 
	{
	public:
		ReplaceRequest(Server *srv) : Request(srv, kRequestReplace) {}
		ReplaceRequest() : Request((Server *)0, kRequestReplace) {}
	};

	class FlushRequest : public Request 
	{
	public:
		FlushRequest(Server *srv) : Request(srv, kRequestFlush) {}
		FlushRequest(void) : Request((Server *)0, kRequestFlush) {}
	};

	class InvalidateRequest: public Request 
	{
	public:
		InvalidateRequest(Server *srv) : Request(srv, kRequestInvalidate) {}
	};

	class SvrAdminRequest: public Request 
	{
	public:
		SvrAdminRequest(Server *srv) : Request(srv, kRequestSvrAdmin) {}
	};

	class MonitorRequest: public Request 
	{
	public:
		MonitorRequest(Server *srv) : Request(srv, kRequestMonitor){}
	};

	class Result 
	{
	private:
		void *addr_;
		long check;
		Result(const Result &);
		char *ServerInfo() const;
	public:
		friend class Server;
		friend class Request;
		friend class ServerPool;

		Result(void);
		~Result(void);
		void Reset(void);

		/* from will not dupped */
		void set_error(int errcode, const char *from, const char *detail); 
		int get_result_code(void) const;
		const char *get_error_message(void) const;
		const char *get_error_from(void) const;
		long long get_hotbackup_id() const;
		long long get_master_hotbackup_timestamp() const;
		long long slave_hotbackup_timestamp() const;
		long long get_binlog_id() const;
		long long get_binlog_offset() const;
		long long get_mem_size() const;
		long long get_datd_size() const;
		int get_num_row_size(void) const;
		int get_total_rows_size(void) const;
		int get_affected_rows_size(void) const;
		int get_num_fields_size(void) const;
		const char* get_field_name(int n) const;
		int get_field_present(const char* name) const;
		int get_field_type(int n) const;
		long long get_tag(void) const;
		void *get_tag_ptr(void) const;
		long long magic(void) const;
		long long get_server_timestamp(void) const;
		long long get_insert_id(void) const;
		long long int_key(void) const;
		const char *binary_key(void) const;
		const char *binary_key(int *) const;
		const char *binary_key(int &) const;
		const char *string_key(void) const;
		const char *string_key(int *) const;
		const char *string_key(int &) const;
		long long int_value(const char *) const;
		double float_value(const char *) const;
		const char *string_value(const char *) const;
		const char *string_value(const char *, int*) const;
		const char *string_value(const char *, int&) const;
		const char *binary_value(const char *) const;
		const char *binary_value(const char *, int*) const;
		const char *binary_value(const char *, int&) const;
		int uncompress_binary_value(const char *name,char **buf,int *lenp);
		/* Uncompress Binary Value without check compressflag */
		int uncompress_binary_value_force(const char *name,char **buf,int *lenp);
		const char * uncompress_error_message() const;
		long long int_value(int) const;
		double float_value(int) const;
		const char *string_value(int) const;
		const char *string_value(int, int*) const;
		const char *string_value(int, int&) const;
		const char *binary_value(int) const;
		const char *binary_value(int, int*) const;
		const char *binary_value(int, int&) const;
		int fetch_row(void);
		int rewind(void);
	};

	class ServerPool 
	{
	private:
		void *addr_;
		long check;
		ServerPool(ServerPool &);
	public:
		friend class Server;
		friend class Request;
		friend class Result;

		ServerPool(int max_servers, int max_requests);
		~ServerPool(void);

		int get_epoll_fd(int size);
		int add_server(Server *srv, int mReq=1, int mConn=0);
		int add_request(Request *, long long);
		int add_request(Request *, long long, long long);
		int add_request(Request *, long long, const char *);
		int add_request(Request *, long long, const char *, int);
		int add_request(Request *, void *);
		int add_request(Request *, void *, long long);
		int add_request(Request *, void *, const char *);
		int add_request(Request *, void *, const char *, int);
		int do_execute(int msec);
		int execute_all(int msec);
		int cancel_request(int);
		int cancel_all_request(int type);
		int abort_request(int);
		int abort_all_request(int type);
		Result *get_result(void);
		Result *get_result(int);
		int get_result(Result&);
		int get_result(Result&, int);

		int server_count(void) const;
		int request_count(int type) const;
		int request_state(int reqId) const;
	};

	const int WAIT = 1;
	const int SEND = 2;
	const int RECV = 4;
	const int DONE = 8;
	const int ALL_STATE = WAIT|SEND|RECV|DONE;

	enum 
	{
		EC_ERROR_BASE = 2000,
		/* unsupported command */
		EC_BAD_COMMAND,		
		/* missing mandatory section */
		EC_MISSING_SECTION,	
		/* incompatible section present */
		EC_EXTRA_SECTION,	
		/* same tag appear twice */
		EC_DUPLICATE_TAG,

		/* 5: same field appear twice in .need() */
		EC_DUPLICATE_FIELD,	
		/* section length too short */
		EC_BAD_SECTION_LENGTH,	
		/* value length not allow */
		EC_BAD_VALUE_LENGTH,	
		/* string value w/o NULL */
		EC_BAD_STRING_VALUE,	
		/* invalid float format */
		EC_BAD_FLOAT_VALUE,	
        /* 10: invalid total field */
		EC_BAD_FIELD_NUM,	
		/* section length too large */
		EC_EXTRA_SECTION_DATA,	
		/* incompatible value type */
		EC_BAD_VALUE_TYPE,	
		/* incompatible operator/comparison */
		EC_BAD_OPERATOR,	
		/* invalid field ID */
		EC_BAD_FIELD_ID,	

        /* 15: invalud field name */
		EC_BAD_FIELD_NAME,	
		/* invalid field type */
		EC_BAD_FIELD_TYPE,	
		/* invalid field size */
		EC_BAD_FIELD_SIZE,	
		/* table defined twice */
		EC_TABLE_REDEFINED,	
		/* request table != server table */
		EC_TABLE_MISMATCH,	
        
		/* 20: unsupported protocol version */
		EC_VERSION_MISMATCH,	
		/* table hash not equal */
		EC_CHECKSUM_MISMATCH,	
		/* End of Result */
		EC_NO_MORE_DATA,	
		/* only full field set accepted by helper */
		EC_NEED_FULL_FIELDSET,	
		/* key type incompatible */
		EC_BAD_KEY_TYPE,	

        /* 25: key size incompatible */
		EC_BAD_KEY_SIZE,
		/* server error */
		EC_SERVER_BUSY,		
		/* network failed */
		EC_BAD_SOCKET,		
		/* object didn't initialized */
		EC_NOT_INITIALIZED,	
		EC_BAD_HOST_STRING,
        
		/* 30 */
		EC_BAD_TABLE_NAME,	
		EC_TASK_NEED_DELETE,
		EC_KEY_NEEDED,
		EC_SERVER_ERROR,
		EC_UPSTREAM_ERROR,
        
		/* 35 */
		EC_KEY_OVERFLOW,	
		EC_BAD_MULTIKEY,
		EC_READONLY_FIELD,
		EC_BAD_ASYNC_CMD,
		EC_OUT_OF_KEY_RANGE,

        /* 40 */
		EC_REQUEST_ABORTED,     
		EC_PARALLEL_MODE,
		EC_KEY_NOTEXIST,
		EC_SERVER_READONLY,
		EC_BAD_INVALID_FIELD,
        
		/* 45 */
		EC_DUPLICATE_KEY,    	
		EC_TOO_MANY_KEY_VALUE,
		EC_BAD_KEY_NAME,
		EC_BAD_RAW_DATA,
		EC_BAD_HOTBACKUP_JID,
        
		/* 50 */
		EC_FULL_SYNC_COMPLETE,  
		EC_FULL_SYNC_STAGE,
		EC_INC_SYNC_STAGE,
        EC_ERR_SYNC_STAGE,
        EC_NOT_ALLOWED_INSERT,
        
		/* 55 */
        EC_COMPRESS_ERROR,  
        EC_UNCOMPRESS_ERROR,
        EC_TASKPOOL,
        EC_STATE_ERROR,
        EC_DATA_NEEDED,

    	EC_TASK_TIMEOUT,
        
		/* 62 */
        EC_BUSINESS_WITHOUT_EXPIRETIME, 
        EC_EMPTY_TBDEF, 
    	EC_INVALID_KEY_VALUE, 
    	EC_INVALID_EXPIRETIME, 

    	EC_GET_EXPIRETIME_END_OF_RESULT, 
	};

	enum 
	{
		ER_HASHCHK=1000,
		ER_NISAMCHK=1001,
		ER_NO=1002,
		ER_YES=1003,
		ER_CANT_CREATE_FILE=1004,
		ER_CANT_CREATE_TABLE=1005,
		ER_CANT_CREATE_DB=1006,
		ER_DB_CREATE_EXISTS=1007,
		ER_DB_DROP_EXISTS=1008,
		ER_DB_DROP_DELETE=1009,
		ER_DB_DROP_RMDIR=1010,
		ER_CANT_DELETE_FILE=1011,
		ER_CANT_FIND_SYSTEM_REC=1012,
		ER_CANT_GET_STAT=1013,
		ER_CANT_GET_WD=1014,
		ER_CANT_LOCK=1015,
		ER_CANT_OPEN_FILE=1016,
		ER_FILE_NOT_FOUND=1017,
		ER_CANT_READ_DIR=1018,
		ER_CANT_SET_WD=1019,
		ER_CHECKREAD=1020,
		ER_DISK_FULL=1021,
		ER_DUP_KEY=1022,
		ER_ERROR_ON_CLOSE=1023,
		ER_ERROR_ON_READ=1024,
		ER_ERROR_ON_RENAME=1025,
		ER_ERROR_ON_WRITE=1026,
		ER_FILE_USED=1027,
		ER_FILSORT_ABORT=1028,
		ER_FORM_NOT_FOUND=1029,
		ER_GET_ERRNO=1030,
		ER_ILLEGAL_HA=1031,
		ER_KEY_NOT_FOUND=1032,
		ER_NOT_FORM_FILE=1033,
		ER_NOT_KEYFILE=1034,
		ER_OLD_KEYFILE=1035,
		ER_OPEN_AS_READONLY=1036,
		ER_OUTOFMEMORY=1037,
		ER_OUT_OF_SORTMEMORY=1038,
		ER_UNEXPECTED_EOF=1039,
		ER_CON_COUNT_ERROR=1040,
		ER_OUT_OF_RESOURCES=1041,
		ER_BAD_HOST_ERROR=1042,
		ER_HANDSHAKE_ERROR=1043,
		ER_DBACCESS_DENIED_ERROR=1044,
		ER_ACCESS_DENIED_ERROR=1045,
		ER_NO_DB_ERROR=1046,
		ER_UNKNOWN_COM_ERROR=1047,
		ER_BAD_NULL_ERROR=1048,
		ER_BAD_DB_ERROR=1049,
		ER_TABLE_EXISTS_ERROR=1050,
		ER_BAD_TABLE_ERROR=1051,
		ER_NON_UNIQ_ERROR=1052,
		ER_SERVER_SHUTDOWN=1053,
		ER_BAD_FIELD_ERROR=1054,
		ER_WRONG_FIELD_WITH_GROUP=1055,
		ER_WRONG_GROUP_FIELD=1056,
		ER_WRONG_SUM_SELECT=1057,
		ER_WRONG_VALUE_COUNT=1058,
		ER_TOO_LONG_IDENT=1059,
		ER_DUP_FIELDNAME=1060,
		ER_DUP_KEYNAME=1061,
		ER_DUP_ENTRY=1062,
		ER_WRONG_FIELD_SPEC=1063,
		ER_PARSE_ERROR=1064,
		ER_EMPTY_QUERY=1065,
		ER_NONUNIQ_TABLE=1066,
		ER_INVALID_DEFAULT=1067,
		ER_MULTIPLE_PRI_KEY=1068,
		ER_TOO_MANY_KEYS=1069,
		ER_TOO_MANY_KEY_PARTS=1070,
		ER_TOO_LONG_KEY=1071,
		ER_KEY_COLUMN_DOES_NOT_EXITS=1072,
		ER_BLOB_USED_AS_KEY=1073,
		ER_TOO_BIG_FIELDLENGTH=1074,
		ER_WRONG_AUTO_KEY=1075,
		ER_READY=1076,
		ER_NORMAL_SHUTDOWN=1077,
		ER_GOT_SIGNAL=1078,
		ER_SHUTDOWN_COMPLETE=1079,
		ER_FORCING_CLOSE=1080,
		ER_IPSOCK_ERROR=1081,
		ER_NO_SUCH_INDEX=1082,
		ER_WRONG_FIELD_TERMINATORS=1083,
		ER_BLOBS_AND_NO_TERMINATED=1084,
		ER_TEXTFILE_NOT_READABLE=1085,
		ER_FILE_EXISTS_ERROR=1086,
		ER_LOAD_INFO=1087,
		ER_ALTER_INFO=1088,
		ER_WRONG_SUB_KEY=1089,
		ER_CANT_REMOVE_ALL_FIELDS=1090,
		ER_CANT_DROP_FIELD_OR_KEY=1091,
		ER_INSERT_INFO=1092,
		ER_INSERT_TABLE_USED=1093,
		ER_NO_SUCH_THREAD=1094,
		ER_KILL_DENIED_ERROR=1095,
		ER_NO_TABLES_USED=1096,
		ER_TOO_BIG_SET=1097,
		ER_NO_UNIQUE_LOGFILE=1098,
		ER_TABLE_NOT_LOCKED_FOR_WRITE=1099,
		ER_TABLE_NOT_LOCKED=1100,
		ER_BLOB_CANT_HAVE_DEFAULT=1101,
		ER_WRONG_DB_NAME=1102,
		ER_WRONG_TABLE_NAME=1103,
		ER_TOO_BIG_SELECT=1104,
		ER_UNKNOWN_ERROR=1105,
		ER_UNKNOWN_PROCEDURE=1106,
		ER_WRONG_PARAMCOUNT_TO_PROCEDURE=1107,
		ER_WRONG_PARAMETERS_TO_PROCEDURE=1108,
		ER_UNKNOWN_TABLE=1109,
		ER_FIELD_SPECIFIED_TWICE=1110,
		ER_INVALID_GROUP_FUNC_USE=1111,
		ER_UNSUPPORTED_EXTENSION=1112,
		ER_TABLE_MUST_HAVE_COLUMNS=1113,
		ER_RECORD_FILE_FULL=1114,
		ER_UNKNOWN_CHARACTER_SET=1115,
		ER_TOO_MANY_TABLES=1116,
		ER_TOO_MANY_FIELDS=1117,
		ER_TOO_BIG_ROWSIZE=1118,
		ER_STACK_OVERRUN=1119,
		ER_WRONG_OUTER_JOIN=1120,
		ER_NULL_COLUMN_IN_INDEX=1121,
		ER_CANT_FIND_UDF=1122,
		ER_CANT_INITIALIZE_UDF=1123,
		ER_UDF_NO_PATHS=1124,
		ER_UDF_EXISTS=1125,
		ER_CANT_OPEN_LIBRARY=1126,
		ER_CANT_FIND_DL_ENTRY=1127,
		ER_FUNCTION_NOT_DEFINED=1128,
		ER_HOST_IS_BLOCKED=1129,
		ER_HOST_NOT_PRIVILEGED=1130,
		ER_PASSWORD_ANONYMOUS_USER=1131,
		ER_PASSWORD_NOT_ALLOWED=1132,
		ER_PASSWORD_NO_MATCH=1133,
		ER_UPDATE_INFO=1134,
		ER_CANT_CREATE_THREAD=1135,
		ER_WRONG_VALUE_COUNT_ON_ROW=1136,
		ER_CANT_REOPEN_TABLE=1137,
		ER_INVALID_USE_OF_NULL=1138,
		ER_REGEXP_ERROR=1139,
		ER_MIX_OF_GROUP_FUNC_AND_FIELDS=1140,
		ER_NONEXISTING_GRANT=1141,
		ER_TABLEACCESS_DENIED_ERROR=1142,
		ER_COLUMNACCESS_DENIED_ERROR=1143,
		ER_ILLEGAL_GRANT_FOR_TABLE=1144,
		ER_GRANT_WRONG_HOST_OR_USER=1145,
		ER_NO_SUCH_TABLE=1146,
		ER_NONEXISTING_TABLE_GRANT=1147,
		ER_NOT_ALLOWED_COMMAND=1148,
		ER_SYNTAX_ERROR=1149,
		ER_DELAYED_CANT_CHANGE_LOCK=1150,
		ER_TOO_MANY_DELAYED_THREADS=1151,
		ER_ABORTING_CONNECTION=1152,
		ER_NET_PACKET_TOO_LARGE=1153,
		ER_NET_READ_ERROR_FROM_PIPE=1154,
		ER_NET_FCNTL_ERROR=1155,
		ER_NET_PACKETS_OUT_OF_ORDER=1156,
		ER_NET_UNCOMPRESS_ERROR=1157,
		ER_NET_READ_ERROR=1158,
		ER_NET_READ_INTERRUPTED=1159,
		ER_NET_ERROR_ON_WRITE=1160,
		ER_NET_WRITE_INTERRUPTED=1161,
		ER_TOO_LONG_STRING=1162,
		ER_TABLE_CANT_HANDLE_BLOB=1163,
		ER_TABLE_CANT_HANDLE_AUTO_INCREMENT=1164,
		ER_DELAYED_INSERT_TABLE_LOCKED=1165,
		ER_WRONG_COLUMN_NAME=1166,
		ER_WRONG_KEY_COLUMN=1167,
		ER_WRONG_MRG_TABLE=1168,
		ER_DUP_UNIQUE=1169,
		ER_BLOB_KEY_WITHOUT_LENGTH=1170,
		ER_PRIMARY_CANT_HAVE_NULL=1171,
		ER_TOO_MANY_ROWS=1172,
		ER_REQUIRES_PRIMARY_KEY=1173,
		ER_NO_RAID_COMPILED=1174,
		ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE=1175,
		ER_KEY_DOES_NOT_EXITS=1176,
		ER_CHECK_NO_SUCH_TABLE=1177,
		ER_CHECK_NOT_IMPLEMENTED=1178,
		ER_CANT_DO_THIS_DURING_AN_TRANSACTION=1179,
		ER_ERROR_DURING_COMMIT=1180,
		ER_ERROR_DURING_ROLLBACK=1181,
		ER_ERROR_DURING_FLUSH_LOGS=1182,
		ER_ERROR_DURING_CHECKPOINT=1183,
		ER_NEW_ABORTING_CONNECTION=1184,
		ER_DUMP_NOT_IMPLEMENTED=   1185,
		ER_FLUSH_MASTER_BINLOG_CLOSED=1186,
		ER_INDEX_REBUILD= 1187,
		ER_MASTER=1188,
		ER_MASTER_NET_READ=1189,
		ER_MASTER_NET_WRITE=1190,
		ER_FT_MATCHING_KEY_NOT_FOUND=1191,
		ER_LOCK_OR_ACTIVE_TRANSACTION=1192,
		ER_UNKNOWN_SYSTEM_VARIABLE=1193,
		ER_CRASHED_ON_USAGE=1194,
		ER_CRASHED_ON_REPAIR=1195,
		ER_WARNING_NOT_COMPLETE_ROLLBACK=1196,
		ER_TRANS_CACHE_FULL=1197,
		ER_SLAVE_MUST_STOP=1198,
		ER_SLAVE_NOT_RUNNING=1199,
		ER_BAD_SLAVE=1200,
		ER_MASTER_INFO=1201,
		ER_SLAVE_THREAD=1202,
		ER_TOO_MANY_USER_CONNECTIONS=1203,
		ER_SET_CONSTANTS_ONLY=1204,
		ER_LOCK_WAIT_TIMEOUT=1205,
		ER_LOCK_TABLE_FULL=1206,
		ER_READ_ONLY_TRANSACTION=1207,
		ER_DROP_DB_WITH_READ_LOCK=1208,
		ER_CREATE_DB_WITH_READ_LOCK=1209,
		ER_WRONG_ARGUMENTS=1210,
		ER_NO_PERMISSION_TO_CREATE_USER=1211,
		ER_UNION_TABLES_IN_DIFFERENT_DIR=1212,
		ER_LOCK_DEADLOCK=1213,
		ER_TABLE_CANT_HANDLE_FULLTEXT=1214,
		ER_CANNOT_ADD_FOREIGN=1215,
		ER_NO_REFERENCED_ROW=1216,
		ER_ROW_IS_REFERENCED=1217,
		ER_CONNECT_TO_MASTER=1218,
		ER_QUERY_ON_MASTER=1219,
		ER_ERROR_WHEN_EXECUTING_COMMAND=1220,
		ER_WRONG_USAGE=1221,
		ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT=1222,
		ER_CANT_UPDATE_WITH_READLOCK=1223,
		ER_MIXING_NOT_ALLOWED=1224,
		ER_DUP_ARGUMENT=1225,
		ER_USER_LIMIT_REACHED=1226,
		ER_SPECIFIC_ACCESS_DENIED_ERROR=1227,
		ER_LOCAL_VARIABLE=1228,
		ER_GLOBAL_VARIABLE=1229,
		ER_NO_DEFAULT=1230,
		ER_WRONG_VALUE_FOR_VAR=1231,
		ER_WRONG_TYPE_FOR_VAR=1232,
		ER_VAR_CANT_BE_READ=1233,
		ER_CANT_USE_OPTION_HERE=1234,
		ER_NOT_SUPPORTED_YET=1235,
		ER_MASTER_FATAL_ERROR_READING_BINLOG=1236,
		ER_SLAVE_IGNORED_TABLE=1237,
		ER_INCORRECT_GLOBAL_LOCAL_VAR=1238,
		CR_UNKNOWN_ERROR=1900,
		CR_SOCKET_CREATE_ERROR=1901,
		CR_CONNECTION_ERROR=1902,
		CR_CONN_HOST_ERROR=1903,
		CR_IPSOCK_ERROR	=1904,
		CR_UNKNOWN_HOST	=1905,
		CR_SERVER_GONE_ERROR=1906,
		CR_VERSION_ERROR=1907,
		CR_OUT_OF_MEMORY=1908,
		CR_WRONG_HOST_INFO=1909,
		CR_LOCALHOST_CONNECTION=1910,
		CR_TCP_CONNECTION=1911,
		CR_SERVER_HANDSHAKE_ERR=1912,
		CR_SERVER_LOST=1913,
		CR_COMMANDS_OUT_OF_SYNC=1914,
		CR_NAMEDPIPE_CONNECTION=1915,
		CR_NAMEDPIPEWAIT_ERROR=1916,
		CR_NAMEDPIPEOPEN_ERROR=1917,
		CR_NAMEDPIPESETSTATE_ERROR=1918,
		CR_CANT_READ_CHARSET=1919,
		CR_NET_PACKET_TOO_LARGE=1920,
		CR_EMBEDDED_CONNECTION=1921,
		CR_PROBE_SLAVE_STATUS=1922,
		CR_PROBE_SLAVE_HOSTS=1923,
		CR_PROBE_SLAVE_CONNECT=1924,
		CR_PROBE_MASTER_CONNECT=1925,
		CR_SSL_CONNECTION_ERROR=1926,
		CR_MALFORMED_PACKET=1927,
		CR_WRONG_LICENSE=1928,
	};

	enum
	{
		ER_SET_IPLIST_NULL = 20001,
		ER_SET_INSTANCE_PROPERTIES_ERR,
		ER_KEY_TYPE,
		ER_CC_VERSION_ERR,
		ER_ROUTE_INFO_NULL,
		ER_BID_VERSION_ERR,
		ER_PORT_OUT_RANGE,
		ER_STATUS_ERROR_VALUE,
		ER_WEIGHT_ERROR_VALUE,
		ER_IP_ERROR_VALUE,
	};

};

#endif
