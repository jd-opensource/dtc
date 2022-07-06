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
*/
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "table/table_def_manager.h"
#include "mem_check.h"
#include "table/hotbackup_table_def.h"

TableDefinitionManager::TableDefinitionManager()
	: _hotbackup(NULL)
	, _buf(NULL)
	, _table(NULL)
	, _dbconfig(NULL)
	, _save_dbconfig(NULL)
{
	_cur = 0;
	_new = 0;
	_def[0] = NULL;
	_def[1] = NULL;
}

TableDefinitionManager::~TableDefinitionManager()
{
	DEC_DELETE(_def[0]);
	DEC_DELETE(_def[1]);
}

bool TableDefinitionManager::set_new_table_def(DTCTableDefinition *t, int idx)
{
	_new = idx;
	DEC_DELETE(_def[_new % 2]);
	_def[_new % 2] = t;
	_def[_new % 2]->increase();
	return true;
}

bool TableDefinitionManager::set_cur_table_def(DTCTableDefinition *t, int idx)
{
	_cur = idx;
	DEC_DELETE(_def[_cur % 2]);
	_def[_cur % 2] = t;
	_def[_cur % 2]->increase();
	return true;
}

bool TableDefinitionManager::renew_cur_table_def()
{
	_cur = _new;
	return true;
}

bool TableDefinitionManager::save_new_table_conf()
{
	_save_dbconfig->cfgObj->Dump("../conf/table.yaml", false);
	_save_dbconfig->destory();
	_save_dbconfig = NULL;
	return true;
}

DTCTableDefinition *TableDefinitionManager::get_cur_table_def()
{
	return _def[_cur % 2];
}

DTCTableDefinition *TableDefinitionManager::get_new_table_def()
{
	return _def[_new % 2];
}

DTCTableDefinition *TableDefinitionManager::get_old_table_def()
{
	return _def[(_cur + 1) % 2];
}

int TableDefinitionManager::get_cur_table_idx()
{
	return _cur;
}

DTCTableDefinition *TableDefinitionManager::get_table_def_by_idx(int idx)
{
	return _def[idx % 2];
}

DTCTableDefinition *TableDefinitionManager::get_hot_backup_table_def()
{
	return _hotbackup;
}

bool TableDefinitionManager::build_hot_backup_table_def()
{
	_hotbackup = build_hot_backup_table();
	if (_hotbackup)
		return true;
	return false;
}

bool TableDefinitionManager::save_db_config()
{
	if (_save_dbconfig) {
		log4cplus_error("_save_dbconfig not empty, maybe error");
		_save_dbconfig->destory();
	}
	_save_dbconfig = _dbconfig;
	_dbconfig = NULL;
	return true;
}

DTCTableDefinition *TableDefinitionManager::load_buffered_table(const char *buf)
{
	DTCTableDefinition *table = NULL;
	char *bufLocal = (char *)MALLOC(strlen(buf) + 1);
	memset(bufLocal, 0, strlen(buf) + 1);
	strcpy(bufLocal, buf);
	if (_dbconfig) {
		_dbconfig->destory();
		_dbconfig = NULL;
	}
	_dbconfig = DbConfig::load_buffered(bufLocal);
	if(bufLocal)
		FREE(bufLocal);
	if (!_dbconfig) {
		log4cplus_error("new dbconfig error");
		return table;
		//	return false;
	}
	table = _dbconfig->build_table_definition();
	if (!table)
		log4cplus_error("build table def error");
	return table;
}

DTCTableDefinition *TableDefinitionManager::load_table(const char *file)
{
	char *buf = NULL;
	int fd, len, readlen;
	DTCTableDefinition *ret = NULL;
	if (!file || file[0] == '\0' || (fd = open(file, O_RDONLY)) < 0) {
		log4cplus_error("open config file error");
		return NULL;
	}
	lseek(fd, 0L, SEEK_SET);
	len = lseek(fd, 0L, SEEK_END);
	lseek(fd, 0L, SEEK_SET);
	_buf = (char *)MALLOC(len + 1);
	buf = (char *)MALLOC(len + 1);
	readlen = read(fd, _buf, len);
	if (readlen < 0 || readlen == 0)
		return ret;
	_buf[len] = '\0';
	close(fd);
	// should copy one, as load_buffered_table will modify buf
	strncpy(buf, _buf, len);
	buf[len] = '\0';
	log4cplus_debug("read file(%s) to buf, len: %d", file, len);
	ret = load_buffered_table(buf);
	if (!ret)
		FREE_CLEAR(_buf);
	FREE_CLEAR(buf);
	_table = ret;
	return ret;
}

DTCTableDefinition *TableDefinitionManager::table_file_table_def()
{
	return _table;
}

const char *TableDefinitionManager::table_file_buffer()
{
	return _buf;
}

bool TableDefinitionManager::release_table_file_def_and_buffer()
{
	FREE_CLEAR(_buf);
	DEC_DELETE(_table);
	return true;
}

bool TableDefinitionManager::renew_table_file_def(const char *buf, int len)
{
	FREE_CLEAR(_buf);
	_buf = (char *)malloc(len + 1);
	strncpy(_buf, buf, len);
	_buf[len] = '\0';
	return true;
}
