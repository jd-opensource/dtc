#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <new>

#include "dtcapi.h"
#include "dtcint.h"

/**
 * NCRequest's methods always assign and return
 * cache the logical error code
 */
#define return_err(x)	return err = (x)
#define return_err_res(x,y,z)   return new NCResult(x,y,z)
#define MAX_EXPIRETIME 30*24*3600
#define CLIENT_CURVE 5
#define AGENT_CURVE 8

NCRequest::NCRequest(NCServer *s, int op)
{
    compressFlag = 0;
	/* MonitorRequest need not table check */
	if( op != DRequest::Monitor && (s && s->is_completed()==0) )
		s = NULL;
	server = s;
	if(server) server->increase();
	table_definition_ = NULL;
	tablename_ = NULL;
	keytype_ = 0;

	switch(op) {
		case DRequest::TYPE_PASS:
		case DRequest::Get:
		case DRequest::Purge:
		case DRequest::Flush:
		case DRequest::Insert:
		case DRequest::Update:
		case DRequest::Delete:
		case DRequest::Replace:
			if(server) {
				table_definition_ = server->table_definition_;
				tablename_ = server->tablename_;
				keytype_ = server->keytype_;
			}
			break;
		/* 跨IDC分布支持 */
		case DRequest::TYPE_SYSTEM_COMMAND:
		case DRequest::Invalidate:
			if(server) {
				table_definition_ = server->admin_tdef;
				tablename_ =(char *) "@HOT_BACKUP";
				keytype_ = DField::Unsigned;
			}
			break;
		case DRequest::Monitor:
			break;
		default:
			op = DRequest::result_code;
	}
	cmd = op;
	err = 0;
	haskey = 0;
	flags = 0;
	key.u64 = 0;
	key.bin.ptr = NULL;
	key.bin.len = 0;

	limitStart = 0;
	limitCount = 0;
	adminCode = 0;
	hotbackup_id = 0;
	master_hotbackup_timestamp_ = 0;
	slave_hotbackup_timestamp_ = 0;
	if(server)
		key_value_list_.keyinfo_ = &server->keyinfo_;
    gzip = NULL;
}

NCRequest::~NCRequest(void){
	unset_key_value();
	unset_key();
	if(server) DEC_DELETE(server);
    if(gzip) DELETE(gzip);
}

/* attach_server() error is transient, don't cache it */
int NCRequest::attach_server(NCServer *s)
{
	/* NO-OP */
	if(s==server)
		return 0;

	switch(cmd) {
		case DRequest::TYPE_PASS:
		case DRequest::Get:
		case DRequest::Purge:
		case DRequest::Flush:
		case DRequest::Insert:
		case DRequest::Update:
		case DRequest::Delete:
		case DRequest::Replace:
			break;
		default:
			/* don't allow change admin target server */
			return -EPERM;
	}

	/* new server must be initialized */
	if(s==NULL || s->is_completed()==0)
		return -EC_NOT_INITIALIZED;

	/* precheck same table */
	if(server) {
		if(keytype_ != s->keytype_)
			return -EC_BAD_KEY_TYPE;
		if(!server->keyinfo_.equal_key_name(s->keyinfo_))
			return -EC_BAD_KEY_TYPE;
		if(strcmp(tablename_, s->tablename_) != 0)
			return -EC_BAD_TABLE_NAME;
		if(table_definition_==NULL) {
			/* no current tabledef */
		} else if(table_definition_ == server->table_definition_) {
			/* same tabledef */
		} else if(table_definition_->is_same_table(server->table_definition_)) {
			/* hash equal */
		} else {
			/* force re-resolve fieldnames */
			ui.Unresolve();
			ci.Unresolve();
			fs.Unresolve();
		}
		/* release old server */
		DEC_DELETE(server);
	}
	/* switch to new server */
	server = s;
	server->increase();
	table_definition_ = server->table_definition_;
	tablename_ = server->tablename_;
	keytype_ = server->keytype_;
	key_value_list_.keyinfo_ = &server->keyinfo_;
	return 0;
}

int NCRequest::need(const char *n, int vid)
{
	if(server==NULL)
		return_err(-EC_NOT_INITIALIZED);
	if(cmd!=DRequest::Get && cmd!=DRequest::TYPE_SYSTEM_COMMAND)
		return_err(-EC_BAD_OPERATOR);
	int ret = fs.add_field(n, vid);
	if(ret)
		err = ret;
	return ret;
}

int NCRequest::add_condition(const char *n, uint8_t op, uint8_t t, const DTCValue &v)
{
	if(server==NULL)
		return_err(-EC_NOT_INITIALIZED);
	if(cmd==DRequest::Insert || cmd==DRequest::Replace)
		return_err(-EC_BAD_OPERATOR);
	switch(t) {
		case DField::Signed:
		case DField::Unsigned:
			if(op >= DField::TotalComparison)
				return_err(-EC_BAD_OPERATOR);
			break;
		case DField::String:
		case DField::Binary:
			if(op != DField::EQ && op != DField::NE)
				return_err(-EC_BAD_OPERATOR);
			break;
		default:
			return_err(-EC_BAD_FIELD_TYPE);
	}
	int ret = ci.add_value(n, op, t, v);
	if(ret) err = ret;
	return ret;
}

int NCRequest::add_operation(const char *n, uint8_t op, uint8_t t, const DTCValue &v)
{
	if(server==NULL) return_err(-EC_NOT_INITIALIZED);
	switch(cmd) {
		case DRequest::Insert:
		case DRequest::Replace:
		case DRequest::TYPE_SYSTEM_COMMAND:
			if(op != DField::Set)
				return_err(-EC_BAD_OPERATOR);
			break;
		case DRequest::Update:
			break;
		default:
			return_err(-EC_BAD_OPERATOR);
			break;
	}
	switch(t) {
		case DField::Signed:
		case DField::Unsigned:
			if(op >= DField::TotalOperation)
				return_err(-EC_BAD_OPERATOR);
			break;

		case DField::String:
		case DField::Binary:
			if(op == DField::Add||op == DField::OR) return_err(-EC_BAD_OPERATOR);
			break;

		case DField::Float:
			if(op == DField::SetBits || op == DField::OR)
				return_err(-EC_BAD_OPERATOR);
			break;
		default:
			return_err(-EC_BAD_FIELD_TYPE);
	}
	int ret = ui.add_value(n, op, t, v);
	if(ret) err = ret;
	return ret;
}

int NCRequest::init_compress()
{
    int iret = 0;
    if (server==NULL)
        return -EC_NOT_INITIALIZED;
    if (table_definition_==NULL) {
		/* ping and get tabledef */
        iret = server->ping();
        table_definition_ = server->table_definition_;
    }
    if (iret)
        return iret;
    if (gzip==NULL)
        NEW(DTCCompress,gzip);
    if (gzip==NULL)
        return -ENOMEM;
    if (table_definition_==NULL)
        return -EC_CHECKSUM_MISMATCH;
    if (server->get_compress_level())
        gzip->set_compress_level(server->get_compress_level());
    return gzip->set_buffer_len(table_definition_->max_field_size());
}

int NCRequest::compress_set(const char *n,const char * v,int len)
{
    int iret = 0;
    if (len < 0) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:fieldlen is invalid");
        return -EC_COMPRESS_ERROR;
    }

    if (n == NULL) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:fieldname is invalid");
        return -EC_COMPRESS_ERROR;
    }

    if (v == NULL) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:fieldvalue is invalid");
        return -EC_COMPRESS_ERROR;
    }

    if (gzip==NULL) {
        iret = init_compress();
        if (iret) return iret;
    }
    if (table_definition_->compress_field_id() <= 0) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:DTC must add a field for compress(get_field_type=2,FieldSize=8,DefaultValue=compressflag)");
        return -EC_COMPRESS_ERROR;
    }

    if (table_definition_->field_type(table_definition_->field_id(n)) != DField::Binary) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:compress just support binary field");
        return - EC_BAD_VALUE_TYPE;
    }

    iret = gzip->compress(v,len);
    if (iret) {
        if (iret==-111111)
             snprintf(errmsg_, sizeof(errmsg_), "compress error:compress buff is null,sth sucks happend");
        else
             snprintf(errmsg_, sizeof(errmsg_), "compress error:zlib return code is %d.",iret);
             return -EC_COMPRESS_ERROR ;
    }
    iret = set_compress_flag(n);
    if (iret)
		return iret;
    return add_operation(n,DField::Set,DField::String,DTCValue::Make(gzip->get_buf(),gzip->get_len()));
}

int NCRequest::compress_set_force(const char *n,const char * v,int len)
{
    int iret = 0;
    if (len < 0) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:fieldlen is invalid");
        return -EC_COMPRESS_ERROR;
    }

    if (n == NULL) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:fieldname is invalid");
        return -EC_COMPRESS_ERROR;
    }

    if (v == NULL) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:fieldvalue is invalid");
        return -EC_COMPRESS_ERROR;
    }

    if (gzip==NULL) {
        iret = init_compress();
        if (iret) return iret;
    }

    if (table_definition_->field_type(table_definition_->field_id(n)) != DField::Binary) {
        snprintf(errmsg_, sizeof(errmsg_), "compress error:compress just support binary field");
        return - EC_BAD_VALUE_TYPE;
    }

    iret = gzip->compress(v,len);
    if (iret) {
        if (iret==-111111)
             snprintf(errmsg_, sizeof(errmsg_), "compress error:compress buff is null,sth sucks happend");
        else
             snprintf(errmsg_, sizeof(errmsg_), "compress error:zlib return code is %d.",iret);
             return -EC_COMPRESS_ERROR ;
    }
    if (iret) 
		return iret;

    return add_operation(n,DField::Set,DField::String,DTCValue::Make(gzip->get_buf(),gzip->get_len()));
}

int NCRequest::add_value(const char *n, uint8_t t, const DTCValue &v)
{
	if(server==NULL) 
		return_err(-EC_NOT_INITIALIZED);
	if(cmd!=DRequest::Insert && cmd!=DRequest::Replace) 
		return_err(-EC_BAD_COMMAND);
	int ret = ui.add_value(n, DField::Set, t, v);
	if(ret) 
		err = ret;
	return ret;
}

int NCRequest::unset_key(void) 
{
	if(haskey) {
		if(server && server->keytype_ == DField::String) {
			DELETE_ARRAY(key.bin.ptr);
			key.Set(NULL, 0);
		}
		haskey = 0;
	}
	return 0;
}

int NCRequest::unset_key_value(void) 
{
	key_value_list_.unset_key();
	flags &= ~DRequest::Flag::MultiKeyValue;
	return 0;
}

int NCRequest::set_key(int64_t k) 
{
	if(server==NULL)
		return_err(-EC_NOT_INITIALIZED);
	if(server->keytype_ != DField::Signed)
		return_err(-EC_BAD_KEY_TYPE);
	key = k;
	haskey = 1;
	unset_key_value();
	return 0;
}

int NCRequest::set_key(const char *name, int l) 
{
	if(server==NULL)
		return_err(-EC_NOT_INITIALIZED);
	if(server->keytype_ != DField::String)
		return_err(-EC_BAD_KEY_TYPE);
	char *a = new char[l];
	memcpy(a, name, l);
	DELETE_ARRAY(key.bin.ptr);
	haskey = 1;
	key.Set(a, l);
	unset_key_value();
	return 0;
}

int NCRequest::add_key_value(const char* name, const DTCValue &v, uint8_t type)
{
	if(server==NULL)
		return_err(-EC_NOT_INITIALIZED);
	if(server->allow_batch_key()==0)
		return_err(-EC_NOT_INITIALIZED);

	int ret = key_value_list_.add_value(name, v, type);
	if(ret < 0)
		return err = ret;

	flags |= DRequest::Flag::MultiKeyValue;
	unset_key();
	return 0;
}

int NCRequest::set_table_definition(void)
{
	int ret = 0;
	if(server) {
		switch(cmd) {
			case DRequest::TYPE_PASS:
			case DRequest::Get:
			case DRequest::Purge:
			case DRequest::Flush:
			case DRequest::Insert:
			case DRequest::Update:
			case DRequest::Delete:
			case DRequest::Replace:
				ret = table_definition_ != server->table_definition_;
				table_definition_ = server->table_definition_;
				tablename_ = server->tablename_;
				keytype_ = server->keytype_;
				break;
			/* 跨IDC分布支持 */
			case DRequest::TYPE_SYSTEM_COMMAND:
			case DRequest::Invalidate:
				ret = table_definition_ != server->admin_tdef;
				table_definition_ = server->admin_tdef;
				tablename_ = (char *)"@HOT_BACKUP";
				keytype_ = DField::Unsigned;
				break;
			default:
				break;
		}
	}
	return ret;
}

int NCRequest::check_key(const DTCValue *kptr)
{
	int keyType = table_definition_->key_type();
	int keySize = table_definition_->key_size();
	/* 多key查询时 kptr为空 */
	if(kptr) {
		if(keyType == 1 || keyType == 2) {
			return check_int_value( *kptr, keyType, keySize);
		} else if(keyType == 4 || keyType == 5) {
			if(keySize <  kptr->str.len)
				return -EC_BAD_FIELD_SIZE_ON_CHECKKEY;
		} else {
			return -EC_EXCEPTION_ON_CHECKKEY;	
		}
	}
	return 0;
}

int NCRequest::encode(const DTCValue *kptr, Packet *pkt)
{
	int err;
	int force = set_table_definition();

	if(table_definition_ &&(	(err = check_key(kptr)) ||
				(err = ui.Resolve(table_definition_, force)) ||
				(err = ci.Resolve(table_definition_, force)) ||
				(err = fs.Resolve(table_definition_, force)) 
			  ))
		return err;
	if((err = pkt->encode_request(*this, kptr)) != 0) 
		return err;
	return 0;
}

NCResult *NCRequest::execute_stream(const DTCValue *kptr)
{
	int resend = 1;
	int nrecv = 0;
	int nsent = 0;

	while(1) {
		int err;
		if(resend) {
			Packet pk;
			if((err=encode(kptr, &pk)) < 0)
				return_err_res(err, "API::encoding", "client api encode error");
			if((err = server->Connect()) != 0) {
				server->increase_error_count_();
				if(err==-EAGAIN) {
					/* unix socket return EAGAIN if listen queue overflow */
					err = -EC_SERVER_BUSY;
				}
				log4cplus_error("dtc connect error %d\n", err);
				return_err_res(err, "API::connecting", "client api connect server error");
			}
			nsent++;
			if((err = server->send_packet_stream(pk)) != 0) {
				if(server->uto_reconnect_ && nsent <= 1 && (err==-ECONNRESET || err==-EPIPE)) {
					resend = 1;
					continue;
				}
				return_err_res(err, "API::sending", "client api send packet error");
			}
		}
		NCResult *res = new NCResult(table_definition_);
		res->versionInfo.set_serial_nr(server->get_last_serialnr());
		err = server->decode_result_stream(*res);
		if(err < 0) {
			if(cmd!=DRequest::TYPE_PASS && err==-EAGAIN && nsent<=1) {
				resend = 1;
				delete res;
				continue;
			}
			/* network error always aborted */
			return res;
		}
		nrecv++;
		if(res->versionInfo.serial_nr() != server->get_last_serialnr()) {
			log4cplus_debug("SN different, receive again. my SN: %lu, result SN: %lu",
					(unsigned long)server->get_last_serialnr(),
					(unsigned long)res->versionInfo.serial_nr());
			/* receive again */
			resend = 0;
		} else if(res->result_code() == -EC_CHECKSUM_MISMATCH && nrecv <= 1) {
			resend = 1;
			/* tabledef changed, resend,agent restart reconnect */
			if(res->versionInfo.ReConnect()) {
				server->Close();	
			}
		} else {
			/* got valid result */
			if(res->result_code() >= 0 && cmd == DRequest::Get)
				res->set_virtual_map(fs);
			if(0 != server->get_remove_count())
				server->clear_remove_count();
			uint64_t time = 0;
			if(res->resultInfo.tag_present(7)) {
				char *t = res->resultInfo.time_info();
				if(t) {
					DTCTimeInfo *t_info = (DTCTimeInfo *)t;
					time = (t_info->time)>0 ? (t_info->time):0;
					server->set_agenttime(time);
				}
			}
			/* agent restart reconnect */
			if(res->versionInfo.ReConnect()) {
				server->Close();	
			}	
			return res;
		}
		/* delete invalid result and loop */
		delete res; 
	}
	/* UNREACHABLE */
	return NULL;
}

NCResult *NCRequest::execute_dgram(SocketAddress *peer, const DTCValue *kptr)
{
	int resend = 1;
	int nrecv = 0;
	int nsent = 0;
	while (1) {
		int err;
		if(resend) {
			Packet pk;
			if ((err = encode(kptr, &pk)) < 0)
				return_err_res(err, "API::encoding", "client api encode error");
			nsent++;
			if ((err = server->send_packet_dgram(peer, pk)) != 0)
			{
				if (peer==NULL && server->uto_reconnect_ && nsent <= 1 &&
						(err==-ECONNRESET || err==-EPIPE || err==-ECONNREFUSED || err==-ENOTCONN)) {
					if((err = server->reconnect()) != 0) {
						if(err==-EAGAIN) {
							/* unix socket return EAGAIN if listen queue overflow */
							err = -EC_SERVER_BUSY;
						}
						return_err_res(err, "API::connecting", "client api connect server error");
					}
					resend = 1;
					continue;
				}
				return_err_res(err, "API::sending", "client api send packet error");
			}
		}

		NCResult *res = new NCResult(table_definition_);
		res->versionInfo.set_serial_nr(server->get_last_serialnr());
		err = server->decode_result_dgram(peer, *res);
		if(err < 0) {
			/* network error encountered */
			return res;
		}
		nrecv++;

		if (res->versionInfo.serial_nr() != server->get_last_serialnr()) {
			log4cplus_debug("SN different, receive again. my SN: %lu, result SN: %lu",
					(unsigned long)server->get_last_serialnr(),
					(unsigned long)res->versionInfo.serial_nr());
			/* receive again */
			resend = 0;
		} else if (res->result_code() == -EC_CHECKSUM_MISMATCH && nrecv <= 1) {
			resend = 1;
			/* tabledef changed, resend */
		} else {
			/* got valid result */
			if (res->result_code() >= 0 && cmd == DRequest::Get)
				res->set_virtual_map(fs);
			uint64_t time = 0;
			if(res->resultInfo.tag_present(7)) {
				char *t = res->resultInfo.time_info();
				if(t) {
					DTCTimeInfo *t_info = (DTCTimeInfo *)t;
					time = (t_info->time)>0 ? (t_info->time):0;
					server->set_agenttime(time);
				}
			}
			return res;
		}
		/* delete invalid result and loop */
		delete res; 
	}
	/* UNREACHABLE */
	return NULL;
}

NCResult *NCRequest::execute_network(const DTCValue *kptr) 
{
	NCResult *res = NULL;
	if(!server->is_dgram()) {
		res = execute_stream(kptr);
	} else if(server->is_dgram()) {
		NCUdpPort *port = server->get_global_port();
		/* get UDPPORT{SN,fd} from udp pool */
		if (port  == NULL)
			return_err_res(err, "API::do_execute", "get udp port error.udp port may exhaust");
		res = execute_dgram(&server->addr_, kptr);
		server->put_global_port(port);
	} else {
		int err;
		if((err = server->Connect()) != 0) {
			if(err==-EAGAIN)
				/* unix socket return EAGAIN if listen queue overflow */
				err = -EC_SERVER_BUSY;
			return_err_res(err, "API::connecting", "client api connect server error");
		}
		res = execute_dgram(NULL, kptr);
	}
	return res;
}

NCResult *NCRequest::execute_internal(const DTCValue *kptr)
{
	if(table_definition_ == NULL) {
		return_err_res(err, "API::encoding", "internal error: request has no tdef");
	int force = set_table_definition();
	int err;
	if(
			(err = ui.Resolve(table_definition_, force)) ||
			(err = ci.Resolve(table_definition_, force)) ||
			(err = fs.Resolve(table_definition_, force))
	  ) {
		return_err_res(err, "API::encoding", "client api encode error");
	}

	NCResultInternal *res = server->execute_internal(*this, kptr);
	if(res == NULL) {
		return_err_res(err, "API::sending", "internal client api do_execute error");
	}
	if(res->result_code() >= 0 && cmd==DRequest::Get)
		res->set_virtual_map(fs);

	return reinterpret_cast<NCResult *>(res);
	}
}

NCResult *NCRequest::precheck(const DTCValue *kptr) 
{

	if(cmd == DRequest::Monitor) {
		/**
		 * MonitorRequest is a stub of kRequestSvrAdmin.Agent should add MonitorRequest type later
		 * Monitor Request need not check
		 */
		cmd = DRequest::TYPE_SYSTEM_COMMAND;
		return 	NULL;
	}
	if(err)
		return_err_res(err, "API::encoding", "do_init Operation Error");
	if(server==NULL)
		return_err_res(-EC_NOT_INITIALIZED, "API::encoding", "Server Not Initialized");
	if(cmd==DRequest::result_code)
		return_err_res(-EC_BAD_COMMAND, "API::encoding", "Unknown Request Type");
	if(server->badkey_)
		return_err_res(-EC_BAD_KEY_TYPE, "API::encoding", "Key Type Mismatch");
	if(server->badname_)
		return_err_res(-EC_BAD_TABLE_NAME, "API::encoding", "Table Name Mismatch");
	if(cmd!=DRequest::Insert && cmd!=DRequest::TYPE_SYSTEM_COMMAND &&
			!(flags & DRequest::Flag::MultiKeyValue) && kptr==NULL)
		return_err_res(-EINVAL, "API::encoding", "Missing Key");
	if((flags & DRequest::Flag::MultiKeyValue) && key_value_list_.is_key_flat()==0)
		return_err_res(-EINVAL, "API::encoding", "Missing Key Value");
	if (table_definition_==NULL) {
		int ret = 0;
		uint64_t time_before = GET_TIMESTAMP();
		/* ping and get tabledef */
		ret = server->ping();
		uint64_t time_after = GET_TIMESTAMP();
		table_definition_ = server->table_definition_;

		uint64_t timeInterval = 0;
		if(time_after > time_before)
			timeInterval = time_after - time_before;
		
		/* log4cplus_info("timeInterval: %lu", timeInterval); */
		std::string accessKey = server->access_token_;
		if(ret == -ETIMEDOUT) {
			//server->data_connector_->set_report_info(accessKey, CLIENT_CURVE, server->get_timeout());
			server->data_connector_->set_report_info(accessKey, AGENT_CURVE, server->get_timeout());
		} else {
			//server->data_connector_->set_report_info(accessKey, CLIENT_CURVE, timeInterval);
			server->data_connector_->set_report_info(accessKey, AGENT_CURVE, (server->get_agenttime() != 0)?server->get_agenttime():timeInterval);
		}
		server->data_connector_->set_report_info(accessKey, CLIENT_CURVE, timeInterval);
		
		//top_percentile_report(accessKey, server->get_address(), timeInterval, ret, RT_SHARDING);
		//top_percentile_report(accessKey, "", timeInterval, ret, RT_ALL);
		server->data_connector_->set_top_percentile_data(accessKey, server->get_address(), timeInterval, ret);
		
		log4cplus_info("NCRequest::precheck,seq:%lu, clientTime:%lu, agentTime:%lu", server->get_last_serialnr(), timeInterval, server->get_agenttime());
		/* 清除  ping 之后获取的 agent-dtc 响应耗时 */
		server->set_agenttime(0);
	}
	return NULL;
}

int NCRequest::set_compress_field_name()
{
    int iret = 0;
	/* 启用压缩必定有这个字段 */
    if (table_definition_ && table_definition_->compress_field_id()>0) {
		/* 读请求需要need字段 */
        if (gzip==NULL) {
            if (cmd != DRequest::Get)
                return 0;
            iret = need(table_definition_->field_name(table_definition_->compress_field_id()),0);
            if (iret) {
                snprintf(errmsg_, sizeof(errmsg_), "need CompressField error,errorcode is %d",iret);
                return -1;
            }
        } else {
			/* 写请求 */
			if  (cmd == DRequest::Insert||cmd == DRequest::Replace) {
				iret = add_operation(table_definition_->field_name(table_definition_->compress_field_id()),//name
						DField::Set,DField::Signed,DTCValue::Make(compressFlag));
			} else if (cmd == DRequest::Update) {
				iret = add_operation(table_definition_->field_name(table_definition_->compress_field_id()),//name
						DField::OR,DField::Signed,DTCValue::Make(compressFlag));
			} else {
				 snprintf(errmsg_, sizeof(errmsg_), "Request type[%d] not support compressset",cmd);
				 return -1;
			}
            if (iret) {
                snprintf(errmsg_, sizeof(errmsg_), "set CompressField error,errorcode is %d",iret);
                return -1;
            }
        }
    }
    return 0;
}
/* if get,need compressfield,if write set compressfield; */
NCResult *NCRequest::do_execute(const DTCValue *kptr) 
{
	NCResult *ret = NULL;
	if(kptr==NULL && haskey) kptr = &key;
	ret = precheck(kptr);
	if(ret==NULL) {
        if (set_compress_field_name())
            return_err_res(-EC_COMPRESS_ERROR, "API::do_execute", "set_compress_field_name error. please check request.get_error_message");;
        /*date:2014/06/09, author:xuxinxin 模调上报 */
        uint64_t time_before = GET_TIMESTAMP();
        if(server->has_internal_executor())
			ret = execute_internal(kptr);
		else
			ret = execute_network(kptr);
        uint64_t time_after = GET_TIMESTAMP();

		uint64_t timeInterval = 0;
		if(time_after > time_before)
			timeInterval = time_after - time_before;
        
        std::string accessKey = server->access_token_;

        if(ret != NULL && ret->resultInfo.result_code() == -ETIMEDOUT) {
        	server->data_connector_->set_report_info(accessKey, AGENT_CURVE, server->get_timeout());
        } else {
        	server->data_connector_->set_report_info(accessKey, AGENT_CURVE, (server->get_agenttime() != 0)?server->get_agenttime():timeInterval);
        }
		server->data_connector_->set_report_info(accessKey, CLIENT_CURVE, timeInterval);
		
		server->data_connector_->set_top_percentile_data(accessKey, server->get_address(), timeInterval, ret ? ret->resultInfo.result_code() : 1);
		
		std::string stemp = accessKey.substr(0, 8);
		uint32_t bid = 0;
		sscanf(stemp.c_str(), "%u", &bid);
		log4cplus_info("NCRequest::do_execute,seq:%lu, clientTime:%lu, agentTime:%lu, bid:%u", server->get_last_serialnr(), timeInterval, server->get_agenttime(), bid);
		/* 清除  解包 之后获取的 agent-dtc 响应耗时 */
		server->set_agenttime(0);
	} else {
		if(NULL != server)
			log4cplus_info("NCRequest::do_execute,seq:%lu, precheck return NULL", server->get_last_serialnr());
	}
	return ret;
}

NCResult *NCRequest::do_execute(int64_t k) 
{
	if(server == NULL)
	    return_err_res(-EC_NOT_INITIALIZED, "API::encoding", "Server Not Initialized");
	if(server->keytype_ != DField::Signed)
	    return_err_res(-EC_BAD_KEY_TYPE, "API::encoding", "Key Type Mismatch");
	DTCValue v(k);
	return do_execute(&v);
}

NCResult *NCRequest::do_execute(const char *k, int l) 
{
	if(server == NULL)
	    return_err_res(-EC_NOT_INITIALIZED, "API::encoding", "Server Not Initialized");
	if(server->keytype_ != DField::String)
	    return_err_res(-EC_BAD_KEY_TYPE, "API::encoding", "Key Type Mismatch");
	DTCValue v(k, l);
	return do_execute(&v);
}

/* packet encoding don't cache error code */
int NCRequest::encode_buffer(char *&ptr, int &len, int64_t &magic, const DTCValue *kptr) 
{
	if(kptr==NULL && haskey) kptr = &key;
	int err = 0;
	NCResult *ret = precheck(kptr);
	if(ret!=NULL) {
		err = ret->result_code();
		delete ret;
		return err;
	}

	set_table_definition();
	if(table_definition_ &&(
				(err = ui.Resolve(table_definition_, 0)) ||
				(err = ci.Resolve(table_definition_, 0)) ||
				(err = fs.Resolve(table_definition_, 0))
			  ))
		return err;
	if((err = Packet::encode_simple_request(*this, kptr, ptr, len)) != 0)
		return err;
	magic = server->get_last_serialnr();
	return 0;
}

int NCRequest::encode_buffer(char *&ptr, int &len, int64_t &magic, int64_t k) 
{
	if(server == NULL)
	    return -EC_NOT_INITIALIZED;
	if(server->keytype_ != DField::Signed)
	    return -EC_BAD_KEY_TYPE;
	if(cmd==DRequest::TYPE_SYSTEM_COMMAND)
	    return -EC_BAD_COMMAND;
	DTCValue v(k);
	return encode_buffer(ptr, len, magic, &v);
}

int NCRequest::encode_buffer(char *&ptr, int &len, int64_t &magic, const char *k, int l) 
{
	if(server == NULL)
	    return -EC_NOT_INITIALIZED;
	if(server->keytype_ != DField::String)
	    return -EC_BAD_KEY_TYPE;
	DTCValue v(k, l);
	return encode_buffer(ptr, len, magic, &v);
}
/*
 * @descr: 无源模式设置超时时间
 * @param: key, dtc key value
 * @param: t, 超时时间，相对时间
 * add by xuxinxin
 * date: 2014/12/09
 */
int NCRequest::set_expire_time(const char* key, int t)
{
	int ret = 0;
	if(key == NULL) {
		log4cplus_error("invalid key value! key is null");
		return -EC_INVALID_KEY_VALUE;
	}
	if(t <= 0) {
		log4cplus_error("invalid expireTime! expireTime:%d", t);
		return -EC_INVALID_EXPIRETIME;
	}
	if(cmd != DRequest::Update) {
		log4cplus_error("invalid requset must be request!");  
		return -EC_BAD_OPERATOR;
	}

	switch(server->keytype_) {
		case DField::Signed:
		case DField::Unsigned:
			ret = set_key(atoll(key));
			break;
		case DField::String:
		case DField::Binary:
			ret = set_key(key, strlen(key));
			break;
		default:
			break;
	}
	if(ret != 0) return ret; 

	ret = add_operation("_dtc_sys_expiretime", DField::Set, DField::Signed, DTCValue::Make(t));
	if(ret != 0) return ret;
	NCResult * rst = do_execute(); //rst 不会为空
	ret = rst->result_code();
	if(ret != 0) {
		log4cplus_error("set expireTime fail, errmsg:%s, errfrom:%s",  rst->resultInfo.error_message(),  rst->resultInfo.error_from());
	}
	delete rst;
	rst = NULL;
	return ret;
}

/*
 * @descr: 无源模式获取超时时间
 * @param: key, dtc key value
 * add by xuxinxin
 * date: 2014/12/11
 */
int NCRequest::get_expire_time(const char* key)
{
	int ret = 0;
	if(key == NULL) {
		log4cplus_error("invalid key value! key is null");
		return -EC_INVALID_KEY_VALUE;
	}

	if(cmd != DRequest::Get) {
		log4cplus_error("invalid requset must be request!");  
		return -EC_BAD_OPERATOR;
	}

	switch(server->keytype_) {
		case DField::Signed:
		case DField::Unsigned:
			ret = set_key(atoll(key));
			break;
		case DField::String:
		case DField::Binary:
			ret = set_key(key, strlen(key));
			break;
		default:
			break;
	}

	ret = need("_dtc_sys_expiretime", 0);
	if(ret != 0) {
		log4cplus_error("get expireTime fail, need error, errcode: %d", ret);
		return ret;
	}
	/* rst 不会为空 */
	NCResult * rst = do_execute(); 
	ret = rst->result_code();
	if(ret < 0) {
		log4cplus_error("get expireTime fail, errmsg:%s, errfrom:%s", rst->resultInfo.error_message(), rst->resultInfo.error_from());
		delete rst;
		rst = NULL;
		return ret;
	}
	if(rst->result==NULL) {
		log4cplus_error("result is null [rst->result==NULL]");
		delete rst;
		rst = NULL;
		return -EC_GET_EXPIRETIME_RESULT_NULL;
	}
	if(rst->result->total_rows() <= 0) {
		log4cplus_error("get expireTime fail, no data exist in dtc for key:%s", key);
		delete rst;
		rst = NULL;
		return -EC_GET_EXPIRETIME_END_OF_RESULT;
	}
	ret = rst->result->decode_row();
	if(ret < 0) {
		log4cplus_error("get expireTime fail, fetch_row error, errmsg:%s, errfrom:%s", rst->resultInfo.error_message(), rst->resultInfo.error_from());
		delete rst;
		rst = NULL;
		return ret;
	}
	int expiretime = 0;
	int id = rst->field_id("_dtc_sys_expiretime");
	if(id >= 0) {
		const DTCValue *v;
		if(id==0 && !(rst->result->field_present(0)))
			v = rst->result_key();
		else
			v = rst->result->field_value(id);

		if(v) {
			switch(rst->field_type(id)) {
				case DField::Signed:
				case DField::Unsigned:
					{
						expiretime =  v->s64;
						break;
					}
				case DField::Float:
					{
						expiretime =  llround(v->flt);
						//return (int64_t)(v->flt);
						break;
					}
				case DField::String:
					{
						if(v->str.ptr)
							expiretime =  atoll(v->str.ptr);
						break;
					}
			}
		}
	} else {
		log4cplus_error("can not find field expiretime");
		delete rst;
		rst = NULL;
		return -EC_GET_EXPIRETIME_FIELD_EXPIRETIME_NULL;
	}
	delete rst;
	rst = NULL;
	return expiretime;
}
