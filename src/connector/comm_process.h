/*
* Copyright [2021] JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
* 
*/
#ifndef __COMM_PROCESS_H__
#define __COMM_PROCESS_H__

#include <value.h>
#include <field/field.h>
#include <namespace.h>
#include "helper_log_api.h"

DTC_BEGIN_NAMESPACE

class CommHelper;
class HelperMain;
class DTCTableDefinition;
class DbConfig;
class RowValue;
class PacketHeader;
class DTCVersionInfo;
class DTCConfig;
union DTCValue;
class DTCFieldValue;
class DTCFieldSet;

typedef CommHelper *(*CreateHandle)(void);

/* helper插件的基类 
使用者需要继承这个类，并实现析构函数、do_inital_init()、Init()、Execute()、process_get()等virtual函数接口。
*/
class CommHelper {
    protected:
	int _group_id;
	int _role;
	int _timeout;

    public:
	// 构造函数。如果需要建立跟别的server的连接do_init构造函数连接。需要在Init或者Execute里执行。
	CommHelper();
	virtual ~CommHelper();

	// 服务需要实现的3个函数
	/* helper是一个进程组。这个函数是fork之前调用的 */
	virtual int global_init(void);

	/* 这个函数是fork之后调用的 */
	virtual int do_init(void);

	/* 每个请求调用一次这个函数处理 */
	virtual int do_execute();

	/* 处理一个请求的超时时间 ，单位为秒*/
	void set_proc_timeout(int timeout)
	{
		_timeout = timeout;
	}

	friend class HelperMain;

    protected:
	/* 处理Get请求的接口函数 */
	virtual int process_get() = 0;

	/* 处理Insert请求接口函数 */
	virtual int process_insert() = 0;

	/* 处理Delete请求接口函数 */
	virtual int process_delete() = 0;

	/* 处理Update请求接口函数 */
	virtual int process_update() = 0;

	/* 处理Replace请求接口函数 */
	virtual int process_replace() = 0;

	/* 查询服务器地址 */
	const char *get_server_address(void) const;

	/* 查询配置文件 */
	int get_int_val(const char *sec, const char *key, int def = 0);
	unsigned long long get_size_val(const char *sec, const char *key,
					unsigned long long def = 0,
					char unit = 0);
	int get_idx_val(const char *, const char *, const char *const *,
			int = 0);
	const char *get_str_val(const char *sec, const char *key);
	bool has_section(const char *sec);
	bool has_key(const char *sec, const char *key);

	/* 查询表定义 */
	DTCTableDefinition *Table(void) const;
	int field_type(int n) const;
	int field_size(int n) const;
	int field_id(const char *n) const;
	const char *field_name(int id) const;
	int num_fields(void) const;
	int key_fields(void) const;

	/* 获取请求包头信息 */
	const PacketHeader *Header() const;

	/* 获取请求包基本信息 */
	const DTCVersionInfo *version_info() const;

	/* 请求命令字 */
	int request_code() const;

	/* 请求是否有key的值（自增量key，insert请求没有key值） */
	int has_request_key() const;

	/* 请求的key值 */
	const DTCValue *request_key() const;

	/* 整型key的值 */
	unsigned int int_key() const;

	/* 请求的where条件 */
	const DTCFieldValue *request_condition() const;

	/* update请求、insert请求、replace请求的更新信息 */
	const DTCFieldValue *request_operation() const;

	/* get请求select的字段列表 */
	const DTCFieldSet *request_fields() const;

	/* 设置错误信息：错误码、错误发生地方、详细错误信息 */
	void set_error(int err, const char *from, const char *msg);

	/* 是否只是select count(*)，而不需要返回结果集 */
	int count_only(void) const;

	/* 是否：没有where条件 */
	int all_rows(void) const;

	/* 根据请求已有的更新数据row。通常将符合条件的row一一调用这个接口更新，然后重新保存回数据层。（如果自己根据request_operation更新，则不需要用这个接口）*/
	int update_row(RowValue &row);

	/* 比较已有的行row，是否满足请求条件。可以指定只比较前面n个字段 */
	int compare_row(const RowValue &row, int iCmpFirstNField = 256) const;

	/* 在往结果集添加一行数据前，必须先调用这个接口初始化 */
	int prepare_result(void);

	/* 将key的值复制到r */
	void update_key(RowValue *r);

	/* 将key的值复制到r */
	void update_key(RowValue &r);

	/* 设置总行数（对于select * from table limit m,n的请求，total-rows一般比结果集的行数大） */
	int set_total_rows(unsigned int nr);

	/* 设置实际更新的行数 */
	int set_affected_rows(unsigned int nr);

	/* 往结果集添加一行数据 */
	int append_row(const RowValue &r);
	int append_row(const RowValue *r);

    private:
	void *addr;
	long check;
	char _name[16];
	char _title[80];
	int _titlePrefixSize;
	const char *_server_string;
	const DbConfig *_dbconfig;
	DTCConfig *_config;
	DTCTableDefinition *_tdef;

    private:
	CommHelper(CommHelper &);
	void do_attach(void *);

	void init_title(int group, int role);
	void set_title(const char *status);
	const char *Name(void) const
	{
		return _name;
	}

    public:
	HelperLogApi logapi;
};

DTC_END_NAMESPACE

#endif
