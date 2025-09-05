#ifndef __DTC_API_H__
#define __DTC_API_H__

#include <stdlib.h>
#include <vector>
#include <string>
#include <stdint.h>

#define IP_LENGHT 16

namespace DTC {

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

	const int RequestSvrAdmin 	= 3;
	const int RequestGet 		= 4;
	const int RequestPurge 		= 5;
	const int RequestInsert 	= 6;
	const int RequestUpdate 	= 7;
	const int RequestDelete 	= 8;
	const int RequestReplace 	= 12;
	const int RequestFlush 		= 13;
	const int RequestInvalidate	= 14;

	//mem monior Request;2014/06/6;by seanzheng
	const int RequestMonitor    = 15;

	// sub-code of admin cmd
	const int RegisterHB		= 1;
	const int LogoutHB		= 2;
	const int GetKeyList		= 3;
	const int GetUpdateKey		= 4;
	const int GetRawData		= 5;
	const int ReplaceRawData	= 6;
	const int AdjustLRU        	= 7;
	const int VerifyHBT		= 8;
	const int GetHBTime		= 9;
	const int SET_READONLY	= 10;
	const int SET_READWRITE	= 11;
	const int QueryBinlogID		= 12;
	const int NodeHandleChange  = 13;
	const int Migrate = 14;
    const int ReloadClusterNodeList = 15;
    const int SetClusterNodeState = 16;
    const int ChangeNodeAddress = 17;
    const int GetClusterState = 18;
    const int PurgeForHit   = 19;
	const int ClearCache = 21;
	const int ColExpandStatus = 24;
	const int ColExpand = 25;
	const int ColExpandDone = 26;
	const int ColExpandKey = 27;
	
	const int KeyTypeNone		= 0;	// undefined
	const int KeyTypeInt		= 1;	// Signed Integer
	const int KeyTypeString		= 4;	// String, case insensitive, null ended

	const int FieldTypeNone		= 0;    // undefined
	const int FieldTypeSigned	= 1;	// Signed Integer
	const int FieldTypeUnsigned	= 2;	// Unsigned Integer
	const int FieldTypeFloat	= 3;	// float
	const int FieldTypeString	= 4;	// String, case insensitive, null ended
	const int FieldTypeBinary	= 5;	// binary

	void init_log (const char *app, const char *dir = NULL);
	void set_log_level(int n);
	int  set_key_value_max(unsigned int count); // Set maximum number of keys for batch operations (default max 32)
#ifndef WIN32
	void write_log (int level,
			const char*file, const char *func, int lineno,
			const char *fmt, ...)
		__attribute__((format(printf,5,6)));
#endif

	class Result;
	class Server {
		private:
			void *addr;
			long check;
		public:
			friend class Request;
			friend class Result;
			friend class ServerPool;
			friend class QOSServerPool;

			Server(void);
			~Server(void);
			Server(const Server &);
			void CloneTabDef(const Server& source);
			int SetAddress(const char *host, const char *port=0);
			int SetTableName(const char *);
            //for compress
            void SetCompressLevel(int);
            //get address and tablename set by user
			const char * GetAddress(void) const;
			const char * GetTableName(void) const;
            //get address and tablename set by ttc frame,for plugin only;
			const char * GetServerAddress(void) const;
			const char * GetServerTableName(void) const;
			int IntKey(void);
			int BinaryKey(void);
			int StringKey(void);
			int AddKey(const char* name, int type);
			int FieldType(const char* name);
			const char *ErrorMessage(void) const;
			void SetTimeout(int);
			void SetMTimeout(int);
			int Connect(void);
			void Close(void);
			int Ping(void);
			void AutoPing(void);
			void SetFD(int); // UNSUPPORTED API
			void SetAutoUpdateTab(bool autoUpdate);
			void SetAutoReconnect(int autoReconnect);
			int DecodePacket(Result &, const char *, int);
			int CheckPacketSize(const char *, int);

			void SetAccessKey(const char *token);

		public:
			void IncErrCount();
			uint64_t GetErrCount();
			void ClearErrCount();
			
			void IncRemoveCount();
			int GetRemoveCount();
			void ClearRemoveCount();

			void IncTotalReq();
			uint64_t GetTotalReq();
			void ClearTotalReq();

			void AddTotalElaps(uint64_t iElaps);
			uint64_t GetTotalElaps();
			void ClearTotalElaps();
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
			int SetRouteList(std::vector<ROUTE_NODE>& ip_list);
			void SetIDCNo(int IDCNo);
			Server* GetServer();

			int SetAccessKey(const char *token);
			int SetTableName(const char *tableName);
			/*
				Signed=1,   // Signed Integer
				String=4,   // String, case insensitive, null ended
				Binary=5,   // opaque binary data
			*/
			int SetKeyType(int type);
			void SetAgentTime(int t);
			void SetMTimeout(int n);
			std::string GetErrorMsg();

		private:
			int IsServerHasExisted(ROUTE_NODE& ip);
			int ConstructServers();
			int ConstructServers2(std::vector<ROUTE_NODE>& IPList);
			void DisorderList(int *tmpBuckets, int size);
			int ConstructBalanceBuckets();
			int RefreshBalanceBuckets(uint64_t );			
			void RemoveServerFromBuckets(uint64_t );
			Server* GetOneServerFromBuckets();
			void SetErrorMsg(int err, std::string from, std::string msg);

		private:
			int m_TimeOut;
			int m_AgentTime; 
			int m_KeyType;
			char* m_TableName;
			std::string m_AccessToken;
			std::string m_ErrMsg;

		private:
			bool m_SetRoute;	
			bool m_ConstructedBySetIPs;
			int m_Bid;
			int m_IDCNo;
			int m_BucketsPos;
			int m_BalanceBucketSize;
			uint64_t m_BidVersion;
			uint64_t m_LastGetCaTime;
			uint64_t m_RefreshBucketsTime;
			uint64_t m_RemoveBucketsTime;
			int* m_LoadBalanceBuckets;
			DTCQosServer* m_QOSSevers;
			std::vector<ROUTE_NODE> m_IPList;
	};

	class Request {
		private:
			void *addr;
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
			int AttachServer(Server *srv);

			void SetAdminCode(int code);
			void SetHotBackupID(long long);
			void SetMasterHBTimestamp(long long);
			void SetSlaveHBTimestamp(long long);

#define _REDIR_(op, t) int op(const char *n, t a) { return op(n, (long long)a); }
#define _REDIRF_(op, t) int op(const char *n, t a) { return op(n, (double)a); }
			int Need(const char *);
			int Need(const char *, int);
			int FieldType(const char*);
			void NoCache(void);
			void NoNextServer(void);
			void Limit(unsigned int, unsigned int);
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

			_REDIR_(EQ, unsigned long long);
			_REDIR_(EQ, long);
			_REDIR_(EQ, unsigned long);
			_REDIR_(EQ, int);
			_REDIR_(EQ, unsigned int);
			_REDIR_(EQ, short);
			_REDIR_(EQ, unsigned short);
			_REDIR_(EQ, char);
			_REDIR_(EQ, unsigned char);

			_REDIR_(NE, unsigned long long);
			_REDIR_(NE, long);
			_REDIR_(NE, unsigned long);
			_REDIR_(NE, int);
			_REDIR_(NE, unsigned int);
			_REDIR_(NE, short);
			_REDIR_(NE, unsigned short);
			_REDIR_(NE, char);
			_REDIR_(NE, unsigned char);

			_REDIR_(GT, unsigned long long);
			_REDIR_(GT, long);
			_REDIR_(GT, unsigned long);
			_REDIR_(GT, int);
			_REDIR_(GT, unsigned int);
			_REDIR_(GT, short);
			_REDIR_(GT, unsigned short);
			_REDIR_(GT, char);
			_REDIR_(GT, unsigned char);

			_REDIR_(GE, unsigned long long);
			_REDIR_(GE, long);
			_REDIR_(GE, unsigned long);
			_REDIR_(GE, int);
			_REDIR_(GE, unsigned int);
			_REDIR_(GE, short);
			_REDIR_(GE, unsigned short);
			_REDIR_(GE, char);
			_REDIR_(GE, unsigned char);

			_REDIR_(LT, unsigned long long);
			_REDIR_(LT, long);
			_REDIR_(LT, unsigned long);
			_REDIR_(LT, int);
			_REDIR_(LT, unsigned int);
			_REDIR_(LT, short);
			_REDIR_(LT, unsigned short);
			_REDIR_(LT, char);
			_REDIR_(LT, unsigned char);

			_REDIR_(LE, unsigned long long);
			_REDIR_(LE, long);
			_REDIR_(LE, unsigned long);
			_REDIR_(LE, int);
			_REDIR_(LE, unsigned int);
			_REDIR_(LE, short);
			_REDIR_(LE, unsigned short);
			_REDIR_(LE, char);
			_REDIR_(LE, unsigned char);

			int Set(const char *, long long);
			int OR(const char *, long long);
			int Add(const char *, long long);
			int Sub(const char *, long long);
			int Set(const char *, double);
			int Add(const char *, double);
			int Sub(const char *, double);
			int Set(const char *, const char *);
			int Set(const char *, const char *, int);

            //just for compress,only support binary field
			int CompressSet(const char *, const char *, int);
			//just compress and set. Don't need compressflag
			int CompressSetForce(const char *, const char *, int);

			//bits op
			int SetMultiBits(const char *, int, int, unsigned int);
			int SetBit  (const char *f, int o) { return SetMultiBits(f, o, 1, 1);}
			int ClearBit(const char *f, int o) { return SetMultiBits(f, o, 1, 0);}

			_REDIR_(Set, unsigned long long);
			_REDIR_(Set, long);
			_REDIR_(Set, unsigned long);
			_REDIR_(Set, int);
			_REDIR_(Set, unsigned int);
			_REDIR_(Set, short);
			_REDIR_(Set, unsigned short);
			_REDIR_(Set, char);
			_REDIR_(Set, unsigned char);
			_REDIRF_(Set, float);
			_REDIRF_(Set, long double);

			_REDIR_(OR, unsigned long long);
			_REDIR_(OR, long);
			_REDIR_(OR, unsigned long);
			_REDIR_(OR, int);
			_REDIR_(OR, unsigned int);
			_REDIR_(OR, short);
			_REDIR_(OR, unsigned short);

			_REDIR_(Add, unsigned long long);
			_REDIR_(Add, long);
			_REDIR_(Add, unsigned long);
			_REDIR_(Add, int);
			_REDIR_(Add, unsigned int);
			_REDIR_(Add, short);
			_REDIR_(Add, unsigned short);
			_REDIR_(Add, char);
			_REDIR_(Add, unsigned char);
			_REDIRF_(Add, float);
			_REDIRF_(Add, long double);

			_REDIR_(Sub, unsigned long long);
			_REDIR_(Sub, long);
			_REDIR_(Sub, unsigned long);
			_REDIR_(Sub, int);
			_REDIR_(Sub, unsigned int);
			_REDIR_(Sub, short);
			_REDIR_(Sub, unsigned short);
			_REDIR_(Sub, char);
			_REDIR_(Sub, unsigned char);
			_REDIRF_(Sub, float);
			_REDIRF_(Sub, long double);
#undef _REDIR_

			void UnsetKey(void);
			int SetKey(long long);
			int SetKey(const char *);
			int SetKey(const char *, int);
#define _REDIR_(t) int SetKey(t a) { return SetKey((long long)a); }
			_REDIR_(unsigned long long);
			_REDIR_(long);
			_REDIR_(unsigned long);
			_REDIR_(int);
			_REDIR_(unsigned int);
			_REDIR_(short);
			_REDIR_(unsigned short);
			_REDIR_(char);
			_REDIR_(unsigned char);
#undef _REDIR_

			int AddKeyValue(const char* name, long long v);
			int AddKeyValue(const char* name, const char *str);
			int AddKeyValue(const char* name, const char *ptr, int len);
#define _REDIR_(t) int AddKeyValue(const char* name, t a) { return AddKeyValue(name, (long long)a); }
			_REDIR_(unsigned long long);
			_REDIR_(long);
			_REDIR_(unsigned long);
			_REDIR_(int);
			_REDIR_(unsigned int);
			_REDIR_(short);
			_REDIR_(unsigned short);
			_REDIR_(char);
			_REDIR_(unsigned char);
#undef _REDIR_

			Result *Execute(void);
			Result *Execute(long long);
			Result *Execute(const char *);
			Result *Execute(const char *, int);

#define _REDIR_(t) Result *Execute(t a) { return Execute((long long)a); }
			_REDIR_(unsigned long long);
			_REDIR_(long);
			_REDIR_(unsigned long);
			_REDIR_(int);
			_REDIR_(unsigned int);
			_REDIR_(short);
			_REDIR_(unsigned short);
			_REDIR_(char);
			_REDIR_(unsigned char);
#undef _REDIR_

			int Execute(Result&);
			int Execute(Result&, long long);
			int Execute(Result&, const char *);
			int Execute(Result&, const char *, int);

#define _REDIR_(t) int Execute(Result &r, t a) { return Execute(r, (long long)a); }
			_REDIR_(unsigned long long);
			_REDIR_(long);
			_REDIR_(unsigned long);
			_REDIR_(int);
			_REDIR_(unsigned int);
			_REDIR_(short);
			_REDIR_(unsigned short);
			_REDIR_(char);
			_REDIR_(unsigned char);
#undef _REDIR_

			int EncodePacket(char *&, int&, long long&);
			int EncodePacket(char *&, int&, long long&, long long);
			int EncodePacket(char *&, int&, long long&, const char *);
			int EncodePacket(char *&, int&, long long&, const char *, int);

#define _REDIR_(t) int EncodePacket(char *&p, int &l, long long &m, t a) { return EncodePacket(p,l,m,(long long)a); }
			_REDIR_(unsigned long long);
			_REDIR_(long);
			_REDIR_(unsigned long);
			_REDIR_(int);
			_REDIR_(unsigned int);
			_REDIR_(short);
			_REDIR_(unsigned short);
			_REDIR_(char);
			_REDIR_(unsigned char);
#undef _REDIR_

			int SetCacheID(long long);
#define _REDIR_(t) int SetCacheID(t a) {return SetCacheID((long long)a);}
			_REDIR_(unsigned long long);
			_REDIR_(long);
			_REDIR_(unsigned long);
			_REDIR_(int);
			_REDIR_(unsigned int);
			_REDIR_(short);
			_REDIR_(unsigned short);
			_REDIR_(char);
			_REDIR_(unsigned char);
#undef _REDIR_
            const char *ErrorMessage(void) const;

            //No-source mode timeout   add by xuxinxin, 2014/12/09
			int SetExpireTime(const char* key, int time);
			int GetExpireTime(const char* key);
	};

	class GetRequest : public Request {
		public:
			GetRequest(Server *srv): Request(srv, RequestGet) {}
			GetRequest(): Request((Server *)0, RequestGet) {}
	};

	class InsertRequest : public Request {
		public:
			InsertRequest(Server *srv) : Request(srv, RequestInsert) {}
			InsertRequest() : Request((Server *)0, RequestInsert) {}
	};

	class DeleteRequest : public Request {
		public:
			DeleteRequest(Server *srv) : Request(srv, RequestDelete) {}
			DeleteRequest() : Request((Server *)0, RequestDelete) {}
	};

	class UpdateRequest : public Request {
		public:
			UpdateRequest(Server *srv) : Request(srv, RequestUpdate) {}
			UpdateRequest() : Request((Server *)0, RequestUpdate) {}
	};

	class PurgeRequest : public Request {
		public:
			PurgeRequest(Server *srv) : Request(srv, RequestPurge) {}
			PurgeRequest() : Request((Server *)0, RequestPurge) {}
	};

	class ReplaceRequest:public Request {
		public:
			ReplaceRequest(Server *srv) : Request(srv, RequestReplace) {}
			ReplaceRequest() : Request((Server *)0, RequestReplace) {}
	};

	class FlushRequest : public Request {
		public:
			FlushRequest(Server *srv) : Request(srv, RequestFlush) {}
			FlushRequest(void) : Request((Server *)0, RequestFlush) {}
	};

	class InvalidateRequest: public Request {
		public:
			InvalidateRequest(Server *srv) : Request(srv, RequestInvalidate) {}
	};

	class SvrAdminRequest: public Request {
		public:
			SvrAdminRequest(Server *srv) : Request(srv, RequestSvrAdmin) {}
	};

	class MonitorRequest: public Request {
		public:
			MonitorRequest(Server *srv) : Request(srv, RequestMonitor){}
	};

	class Result {
		private:
			void *addr;
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

			void SetError(int errcode, const char *from, const char *detail); // from will not dupped
			int ResultCode(void) const;
			const char *ErrorMessage(void) const;
			const char *ErrorFrom(void) const;
			long long HotBackupID() const;
			long long MasterHBTimestamp() const;
			long long SlaveHBTimestamp() const;
			long long BinlogID() const;
			long long BinlogOffset() const;
			long long MemSize() const;
			long long DataSize() const;
			int NumRows(void) const;
			int TotalRows(void) const;
			int AffectedRows(void) const;
			int NumFields(void) const;
			const char* FieldName(int n) const;
			int FieldPresent(const char* name) const;
			int FieldType(int n) const;
			long long Tag(void) const;
			void *TagPtr(void) const;
			long long Magic(void) const;
			long long ServerTimestamp(void) const;
			long long InsertID(void) const;
			long long IntKey(void) const;
			const char *BinaryKey(void) const;
			const char *BinaryKey(int *) const;
			const char *BinaryKey(int &) const;
			const char *StringKey(void) const;
			const char *StringKey(int *) const;
			const char *StringKey(int &) const;
			long long IntValue(const char *) const;
			double FloatValue(const char *) const;
			const char *StringValue(const char *) const;
			const char *StringValue(const char *, int*) const;
			const char *StringValue(const char *, int&) const;
			const char *BinaryValue(const char *) const;
			const char *BinaryValue(const char *, int*) const;
			const char *BinaryValue(const char *, int&) const;
			int UnCompressBinaryValue(const char *name,char **buf,int *lenp);
			//Uncompress Binary Value without check compressflag
			int UnCompressBinaryValueForce(const char *name,char **buf,int *lenp);
            const char * UnCompressErrorMessage() const;
			long long IntValue(int) const;
			double FloatValue(int) const;
			const char *StringValue(int) const;
			const char *StringValue(int, int*) const;
			const char *StringValue(int, int&) const;
			const char *BinaryValue(int) const;
			const char *BinaryValue(int, int*) const;
			const char *BinaryValue(int, int&) const;
			int FetchRow(void);
			int Rewind(void);
	};

	class ServerPool {
		private:
			void *addr;
			long check;
			ServerPool(ServerPool &);
		public:
			friend class Server;
			friend class Request;
			friend class Result;

			ServerPool(int maxServers, int maxRequests);
			~ServerPool(void);

			int GetEpollFD(int size);
			int AddServer(Server *srv, int mReq=1, int mConn=0);
			int AddRequest(Request *, long long);
			int AddRequest(Request *, long long, long long);
			int AddRequest(Request *, long long, const char *);
			int AddRequest(Request *, long long, const char *, int);
			int AddRequest(Request *, void *);
			int AddRequest(Request *, void *, long long);
			int AddRequest(Request *, void *, const char *);
			int AddRequest(Request *, void *, const char *, int);
			int Execute(int msec);
			int ExecuteAll(int msec);
			int CancelRequest(int);
			int CancelAllRequest(int type);
			int AbortRequest(int);
			int AbortAllRequest(int type);
			Result *GetResult(void);
			Result *GetResult(int);
			int GetResult(Result&);
			int GetResult(Result&, int);

			int ServerCount(void) const;
			int RequestCount(int type) const;
			int RequestState(int reqId) const;
	};

	const int WAIT = 1;
	const int SEND = 2;
	const int RECV = 4;
	const int DONE = 8;
	const int ALL_STATE = WAIT|SEND|RECV|DONE;

	enum {
		EC_ERROR_BASE = 2000,
		EC_BAD_COMMAND,		// unsupported command
		EC_MISSING_SECTION,	// missing mandatory section
		EC_EXTRA_SECTION,	// incompatible section present
		EC_DUPLICATE_TAG,	// same tag appear twice

		EC_DUPLICATE_FIELD,	//5: same field appear twice in .Need()
		EC_BAD_SECTION_LENGTH,	// section length too short
		EC_BAD_VALUE_LENGTH,	// value length not allow
		EC_BAD_STRING_VALUE,	// string value w/o NULL
		EC_BAD_FLOAT_VALUE,	// invalid float format

		EC_BAD_FIELD_NUM,	//10: invalid total field#
		EC_EXTRA_SECTION_DATA,	// section length too large
		EC_BAD_VALUE_TYPE,	// incompatible value type
		EC_BAD_OPERATOR,	// incompatible operator/comparison
		EC_BAD_FIELD_ID,	// invalid field ID

		EC_BAD_FIELD_NAME,	//15: invalud field name
		EC_BAD_FIELD_TYPE,	// invalid field type
		EC_BAD_FIELD_SIZE,	// invalid field size
		EC_TABLE_REDEFINED,	// table defined twice
		EC_TABLE_MISMATCH,	// request table != server table

		EC_VERSION_MISMATCH,	//20: unsupported protocol version
		EC_CHECKSUM_MISMATCH,	// table hash not equal
		EC_NO_MORE_DATA,	// End of Result
		EC_NEED_FULL_FIELDSET,	// only full field set accepted by helper
		EC_BAD_KEY_TYPE,	// key type incompatible

		EC_BAD_KEY_SIZE,	// 25: key size incompatible
		EC_SERVER_BUSY,		//server error
		EC_BAD_SOCKET,		// network failed
		EC_NOT_INITIALIZED,	// object didn't initialized
		EC_BAD_HOST_STRING,

		EC_BAD_TABLE_NAME,	// 30
		EC_TASK_NEED_DELETE,
		EC_KEY_NEEDED,
		EC_SERVER_ERROR,
		EC_UPSTREAM_ERROR,

		EC_KEY_OVERFLOW,	// 35
		EC_BAD_MULTIKEY,
		EC_READONLY_FIELD,
		EC_BAD_ASYNC_CMD,
		EC_OUT_OF_KEY_RANGE,

		EC_REQUEST_ABORTED,     // 40
		EC_PARALLEL_MODE,
		EC_KEY_NOTEXIST,
		EC_SERVER_READONLY,
		EC_BAD_INVALID_FIELD,

		EC_DUPLICATE_KEY,    	// 45
		EC_TOO_MANY_KEY_VALUE,
		EC_BAD_KEY_NAME,
		EC_BAD_RAW_DATA,
		EC_BAD_HOTBACKUP_JID,

		EC_FULL_SYNC_COMPLETE,  //50
		EC_FULL_SYNC_STAGE,
		EC_INC_SYNC_STAGE,
        EC_ERR_SYNC_STAGE,
        EC_NOT_ALLOWED_INSERT,

        EC_COMPRESS_ERROR,  //55
        EC_UNCOMPRESS_ERROR,
        EC_TASKPOOL,
        EC_STATE_ERROR,
        EC_DATA_NEEDED,

		EC_BAD_ACCESS_KEY,	//60
		EC_TASK_TIMEOUT,
        EC_BUSINESS_WITHOUT_EXPIRETIME, //62
        EC_EMPTY_TBDEF, //63
    	EC_INVALID_KEY_VALUE, //64

    	EC_INVALID_EXPIRETIME, //65
    	EC_GET_EXPIRETIME_END_OF_RESULT, //66
	};

	enum {
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

	enum{
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
