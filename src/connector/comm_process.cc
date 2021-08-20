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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "comm_process.h"
#include <protocol.h>
#include <config/dbconfig.h>
#include <log/log.h>
#include <task/task_base.h>

#include "proc_title.h"

#define CAST(type, var) type *var = (type *)addr

CommHelper::CommHelper() : addr(NULL), check(0)
{
	_timeout = 0;
}

CommHelper::~CommHelper()
{
	addr = NULL;
}

CommHelper::CommHelper(CommHelper &proc)
{
}

void CommHelper::init_title(int group, int role)
{
	snprintf(_name, sizeof(_name), "helper%d%c", group,
		 MACHINEROLESTRING[role]);

	snprintf(_title, sizeof(_title), "%s: ", _name);
	_titlePrefixSize = strlen(_title);
	_title[sizeof(_title) - 1] = '\0';
}

void CommHelper::set_title(const char *status)
{
	strncpy(_title + _titlePrefixSize, status,
		sizeof(_title) - 1 - _titlePrefixSize);
	set_proc_title(_title);
}

int CommHelper::global_init(void)
{
	return 0;
}

int CommHelper::do_init(void)
{
	return 0;
}

int CommHelper::do_execute()
{
	int ret = -1;

	log4cplus_debug("request code: %d", request_code());

	switch (request_code()) {
	case DRequest::TYPE_PASS:
	case DRequest::Purge:
	case DRequest::Flush:
		return 0;

	case DRequest::Get:
		logapi.start();
		ret = process_get();
		logapi.done(__FILE__, __LINE__, "SELECT", ret, ret);
		break;

	case DRequest::Insert:
		logapi.start();
		ret = process_insert();
		logapi.done(__FILE__, __LINE__, "INSERT", ret, ret);
		break;

	case DRequest::Update:
		logapi.start();
		ret = process_update();
		logapi.done(__FILE__, __LINE__, "UPDATE", ret, ret);
		break;

	case DRequest::Delete:
		logapi.start();
		ret = process_delete();
		logapi.done(__FILE__, __LINE__, "DELETE", ret, ret);
		break;

	case DRequest::Replace:
		logapi.start();
		ret = process_replace();
		logapi.done(__FILE__, __LINE__, "REPLACE", ret, ret);
		break;

	default:
		log4cplus_error("unknow request code");
		set_error(-EC_BAD_COMMAND, __FUNCTION__,
			  "[Helper]invalid request-code");
		return -1;
	};

	return ret;
}

void CommHelper::do_attach(void *p)
{
	addr = p;
}

DTCTableDefinition *CommHelper::Table(void) const
{
	CAST(DtcJob, job);
	return job->table_definition();
}

const PacketHeader *CommHelper::Header() const
{
#if 0
	CAST(DtcJob, job);
	return &(job->headerInfo);
#else
	// NO header anymore
	return NULL;
#endif
}

const DTCVersionInfo *CommHelper::version_info() const
{
	CAST(DtcJob, job);
	return &(job->versionInfo);
}

int CommHelper::request_code() const
{
	CAST(DtcJob, job);
	return job->request_code();
}

int CommHelper::has_request_key() const
{
	CAST(DtcJob, job);
	return job->has_request_key();
}

const DTCValue *CommHelper::request_key() const
{
	CAST(DtcJob, job);
	return job->request_key();
}

unsigned int CommHelper::int_key() const
{
	CAST(DtcJob, job);
	return job->int_key();
}

const DTCFieldValue *CommHelper::request_condition() const
{
	CAST(DtcJob, job);
	return job->request_condition();
}

const DTCFieldValue *CommHelper::request_operation() const
{
	CAST(DtcJob, job);
	return job->request_operation();
}

const DTCFieldSet *CommHelper::request_fields() const
{
	CAST(DtcJob, job);
	return job->request_fields();
}

void CommHelper::set_error(int err, const char *from, const char *msg)
{
	CAST(DtcJob, job);
	job->set_error(err, from, msg);
}

int CommHelper::count_only(void) const
{
	CAST(DtcJob, job);
	return job->count_only();
}

int CommHelper::all_rows(void) const
{
	CAST(DtcJob, job);
	return job->all_rows();
}

int CommHelper::update_row(RowValue &row)
{
	CAST(DtcJob, job);
	return job->update_row(row);
}

int CommHelper::compare_row(const RowValue &row, int iCmpFirstNField) const
{
	CAST(DtcJob, job);
	return job->compare_row(row, iCmpFirstNField);
}

int CommHelper::prepare_result(void)
{
	CAST(DtcJob, job);
	return job->prepare_result();
}

void CommHelper::update_key(RowValue *r)
{
	CAST(DtcJob, job);
	job->update_key(r);
}

void CommHelper::update_key(RowValue &r)
{
	update_key(&r);
}

int CommHelper::set_total_rows(unsigned int nr)
{
	CAST(DtcJob, job);
	return job->set_total_rows(nr);
}

int CommHelper::set_affected_rows(unsigned int nr)
{
	CAST(DtcJob, job);
	job->resultInfo.set_affected_rows(nr);
	return 0;
}

int CommHelper::append_row(const RowValue &r)
{
	return append_row(&r);
}

int CommHelper::append_row(const RowValue *r)
{
	CAST(DtcJob, job);
	int ret = job->append_row(r);
	if (ret > 0)
		ret = 0;
	return ret;
}

const char *CommHelper::get_server_address(void) const
{
	return _server_string;
}

int CommHelper::get_int_val(const char *sec, const char *key, int def)
{
	return _config->get_int_val(sec, key, def);
}

unsigned long long CommHelper::get_size_val(const char *sec, const char *key,
					    unsigned long long def, char unit)
{
	return _config->get_size_val(sec, key, def, unit);
}

int CommHelper::get_idx_val(const char *v1, const char *v2,
			    const char *const *v3, int v4)
{
	return _config->get_idx_val(v1, v2, v3, v4);
}

const char *CommHelper::get_str_val(const char *sec, const char *key)
{
	return _config->get_str_val(sec, key);
}

bool CommHelper::has_section(const char *sec)
{
	return _config->has_section(sec);
}

bool CommHelper::has_key(const char *sec, const char *key)
{
	return _config->has_key(sec, key);
}

int CommHelper::field_type(int n) const
{
	return _tdef->field_type(n);
}
int CommHelper::field_size(int n) const
{
	return _tdef->field_size(n);
}
int CommHelper::field_id(const char *n) const
{
	return _tdef->field_id(n);
}
const char *CommHelper::field_name(int id) const
{
	return _tdef->field_name(id);
}
int CommHelper::num_fields(void) const
{
	return _tdef->num_fields();
}
int CommHelper::key_fields(void) const
{
	return _tdef->key_fields();
}
