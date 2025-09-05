/*
* Copyright JD.com, Inc.
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
#include <sys/mman.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <mem_check.h>
#include <log/log.h>
#include <sys/file.h>

#include "lock/system_lock.h"
#include "stat_manager.h"

#if HAS_ATOMIC8
int64_t StatItem::stat_item_dummy;
#else
uint32_t StatCounter::stat_item_u32_dummy;
int32_t StatCounter::stat_item_s32_dummy;
StatItemObject StatItem::stat_item_Object_dummy;
#endif
int64_t StatSampleObject::dummy_count[2];
const DTCStatInfo StatSampleObject::stat_dummy_info = { 0, 0, SA_SAMPLE, 0 };
StatSampleObject StatSample::stat_sample_object_dummy;

int64_t StatSampleObject::count(unsigned int n)
{
	P a(this);
	if (n > dtc_stat_info->before_count)
		return 0;
	return stat_sample_object_count[n];
}

int64_t StatSampleObject::sum(void)
{
	P a(this);
	return stat_sample_object_count[0];
}

int64_t StatSampleObject::average(int64_t o)
{
	P a(this);
	return stat_sample_object_count[1] ?
		       stat_sample_object_count[0] /
			       stat_sample_object_count[1] :
		       o;
}

void StatSampleObject::push(int64_t v)
{
	P a(this);
	stat_sample_object_count[0] += v;
	stat_sample_object_count[1]++;
	for (unsigned int n = 0; n < dtc_stat_info->before_count; n++)
		if (v >= dtc_stat_info->vptr[n])
			stat_sample_object_count[2 + n]++;
}

void StatSampleObject::output(int64_t *v)
{
	P a(this);
	memcpy(v, stat_sample_object_count, (2 + 16) * sizeof(int64_t));
	memset(stat_sample_object_count, 0, (2 + 16) * sizeof(int64_t));
}

StatManager::StatManager()
{
	header_ = NULL;
	index_size_ = 0;
	memset(map_, 0, sizeof(map_));
	stat_info_ = NULL;
	stat_num_info_ = 0;
	socket_lock_device_ = 0;
	socket_lock_ino_ = 0;
	socket_lock_fd_ = -1;
}

StatManager::~StatManager()
{
	if (header_) {
		for (unsigned int i = 0; i < get_map_size(); i++)
			if (map_[i])
				munmap(map_[i], header_->data_size);
		munmap(header_, index_size_);
	}
	if (stat_info_)
		delete[] stat_info_;
	if (socket_lock_fd_ >= 0)
		close(socket_lock_fd_);
}

int StatManager::init_stat_info(const char *name, const char *indexfile,
				int isc)
{
	P __a(this);

	int fd;

	fd = open(indexfile, O_RDWR);
	if (fd < 0) {
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "cannot open index file, checking privilege and stat directory: %s", indexfile);
		return -1;
	}

	struct stat st;
	if (fstat(fd, &st) != 0) {
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "fstat() failed on index file");
		close(fd);
		return -2;
	}
	socket_lock_device_ = st.st_dev;
	socket_lock_ino_ = st.st_ino;

	if (isc == 0) {
		if (get_socket_lock_fd("serv") < 0) {
			snprintf(stat_error_message_,
				 sizeof(stat_error_message_),
				 "stat data locked by other process");
			close(fd);
			return -2;
		}
	}

	index_size_ = lseek(fd, 0L, SEEK_END);
	if (index_size_ < sizeof(DTCStatHeader) + sizeof(DTCStatInfo)) {
		// file too short
		close(fd);
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "index file too short");
		return -1;
	}

	// On successful execution, mmap() returns a pointer to the mapped region, munmap() returns 0. On failure, mmap() returns MAP_FAILED [value is (void *)-1], munmap returns -1.
	header_ = (DTCStatHeader *)mmap(NULL, index_size_, PROT_READ,
					MAP_SHARED, fd, 0);
	close(fd);

	if (header_ == MAP_FAILED) {
		header_ = NULL;
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "mmap index file failed");
		return -2;
	}

	if (header_->signature != *(unsigned int *)"sTaT") {
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "bad index file signature");
		return -1;
	}
	if (header_->version != 1) {
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "bad index file version");
		return -1;
	}
	if (index_size_ < header_->index_size) {
		// file too short
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "index file too short");
		return -1;
	}
	if (header_->index_size > (4 << 20)) {
		// data too large
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "index size too large");
		return -1;
	}
	if (header_->data_size > (1 << 20)) {
		// data too large
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "data size too large");
		return -1;
	}
	if (strncmp(name, header_->name, sizeof(header_->name)) != 0) {
		// name mismatch
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "stat name mismatch");
		return -1;
	}

	if (header_->num_info == 0) {
		snprintf(stat_error_message_, sizeof(stat_error_message_),
			 "No Stat ID defined");
		return -1;
	}

	stat_num_info_ = header_->num_info;
	stat_info_ = new StatInfo[stat_num_info_];
	DTCStatInfo *si = header_->first();
	for (unsigned int i = 0; i < stat_num_info_; i++, si = si->next()) {
		if (si->next() > header_->last()) {
			snprintf(stat_error_message_,
				 sizeof(stat_error_message_),
				 "index info exceed EOF");
			return -1;
		}

		if (si->offset + si->data_size() > header_->data_size) {
			snprintf(stat_error_message_,
				 sizeof(stat_error_message_),
				 "data offset exceed EOF");
			return -1;
		}

		// first 16 bytes reserved by header_
		if (si->offset < 16) {
			snprintf(stat_error_message_,
				 sizeof(stat_error_message_),
				 "data offset < 16");
			return -1;
		}

		if (si->before_count + si->after_count > 16) {
			snprintf(stat_error_message_,
				 sizeof(stat_error_message_),
				 "too many base value");
			return -1;
		}

		stat_info_[i].stat_manager_owner = this;
		stat_info_[i].stat_info = si;

		id_map_[si->id] = &stat_info_[i];
	}

	mprotect(header_, index_size_, PROT_READ);

	char buf[strlen(indexfile) + 10];
	strncpy(buf, indexfile, strlen(indexfile) + 10);
	char *p = strrchr(buf, '.');
	if (p == NULL)
		p = buf + strlen(buf);

	map_[SC_CUR] = (char *)get_map_file_info(NULL, header_->data_size);
	strncpy(p, ".10s", 5);
	map_[SC_10S] = (char *)get_map_file_info(buf, header_->data_size);
	strncpy(p, ".10m", 5);
	map_[SC_10M] = (char *)get_map_file_info(buf, header_->data_size);
	strncpy(p, ".all", 5);
	map_[SC_ALL] = (char *)get_map_file_info(buf, header_->data_size);

	at_cur(2 * 8) = header_->creation_time;
	if (isc == 0) {
		int n;
		do {
			n = 0;
			for (unsigned int i = 0; i < stat_num_info_; i++) {
				if (!stat_info_[i].is_expr())
					continue;
				if (stat_info_[i].stat_expression != NULL)
					continue;
				unsigned int cnt =
					stat_info_[i].stat_info->before_count +
					stat_info_[i].stat_info->after_count;
				if (cnt == 0)
					continue;
				stat_info_[i].stat_expression =
					init_stat_expression(
						cnt,
						stat_info_[i].stat_info->vptr);
				if (stat_info_[i].stat_expression) {
					stat_info_expression_.push_back(
						&stat_info_[i]);
					n++;
				}
			}
		} while (n > 0);
		at_cur(3 * 8) = time(NULL); // startup time
		atomic_t *a0 = (atomic_t *)&at_cur(0);
		int v = atomic_add_return(1, a0);
		atomic_set(a0 + 1, v);
	}
	return 0;
}

static const DTCStatDefinition SysStatDefinition[] = {
	{ STAT_CREATE_TIME, "statinfo create time", SA_CONST, SU_DATETIME },
	{ STAT_STARTUP_TIME, "startup time", SA_CONST, SU_DATETIME },
	{ STAT_CHECKPOINT_TIME, "checkpoint time", SA_CONST, SU_DATETIME },
};
#define NSYSID (sizeof(SysStatDefinition) / sizeof(DTCStatDefinition))

int StatManager::create_stat_index(const char *name, const char *indexfile,
				   const DTCStatDefinition *stat_definition,
				   char *stat_error_message,
				   int stat_error__message_length)
{
	if (access(indexfile, F_OK) == 0) {
		snprintf(stat_error_message, stat_error__message_length,
			 "index file already exists");
		return -1;
	}

	int numinfo = NSYSID;
	int indexsize = sizeof(DTCStatHeader) + sizeof(DTCStatInfo) * NSYSID;
	int datasize = 16 + sizeof(int64_t) * NSYSID;

	unsigned int i;
	for (i = 0; stat_definition[i].id; i++) {
		if (stat_definition[i].before_count +
			    stat_definition[i].after_count >
		    16) {
			snprintf(stat_error_message, stat_error__message_length,
				 "too many argument counts");
			return -1;
		}
		numinfo++;
		indexsize += sizeof(DTCStatInfo);
		switch (stat_definition[i].type) {
		case SA_SAMPLE:
			indexsize += 16 * sizeof(int64_t);
			datasize += 18 * sizeof(int64_t);
			break;
		case SA_EXPR:
			indexsize += (stat_definition[i].before_count +
				      stat_definition[i].after_count) *
				     sizeof(int64_t);
		default:
			datasize += sizeof(int64_t);
		}
	}

	if (indexsize > (4 << 20)) {
		snprintf(stat_error_message, stat_error__message_length,
			 "index file size too large");
		return -1;
	}
	if (datasize > (1 << 20)) {
		snprintf(stat_error_message, stat_error__message_length,
			 "data file size too large");
		return -1;
	}

	DTCStatHeader *header =
		(DTCStatHeader *)get_map_file_info(indexfile, indexsize);
	if (header == NULL) {
		snprintf(stat_error_message, stat_error__message_length,
			 "map stat file error");
		return -1;
	}

	header->signature = *(int *)"sTaT";
	header->version = 1;
	header->num_info = numinfo;
	header->index_size = indexsize;
	header->data_size = datasize;
	strncpy(header->name, name, sizeof(header->name));

	DTCStatInfo *si = header->first();
	unsigned int offset = 16;

	for (i = 0; i < NSYSID; i++, si = si->next()) {
		si->id = SysStatDefinition[i].id;
		si->type = SysStatDefinition[i].type;
		si->unit = SysStatDefinition[i].unit;
		si->offset = offset;
		si->before_count = 0;
		si->after_count = 0;
		strncpy(si->name, SysStatDefinition[i].name, sizeof(si->name));
		offset += si->data_size();
	}
	for (i = 0; stat_definition[i].id; i++, si = si->next()) {
		si->id = stat_definition[i].id;
		si->type = stat_definition[i].type;
		si->unit = stat_definition[i].unit;
		si->offset = offset;
		si->before_count = 0;
		si->after_count = 0;
		strncpy(si->name, stat_definition[i].name, sizeof(si->name));
		offset += si->data_size();

		if (si->is_sample()) {
			unsigned char &j = si->before_count;
			for (j = 0; j < 16 && stat_definition[i].arg[j]; j++)
				si->vptr[j] = stat_definition[i].arg[j];
		} else if (si->is_expr()) {
			si->before_count = stat_definition[i].before_count;
			si->after_count = stat_definition[i].after_count;
			for (int j = 0; j < si->before_count + si->after_count;
			     j++)
				si->vptr[j] = stat_definition[i].arg[j];
		}
	}
	header->creation_time = time(NULL); // create time
	munmap(header, indexsize);
	char buf[strlen(indexfile) + 10];
	strncpy(buf, indexfile, strlen(indexfile) + 10);
	char *p = strrchr(buf, '.');
	if (p == NULL)
		p = buf + strlen(buf);

	strncpy(p, ".dat", 5);
	unlink(buf);
	strncpy(p, ".10s", 5);
	unlink(buf);
	strncpy(p, ".10m", 5);
	unlink(buf);
	strncpy(p, ".all", 5);
	unlink(buf);
	return 0;
}

void *StatManager::get_map_file_info(const char *file_path, int size)
{
	int fd = open(file_path, O_RDWR | O_CREAT, 0666);
	void *map = NULL;
	if (fd >= 0) {
		if (size > 0) {
			if (ftruncate(fd, size) == -1) {
				// Handle error - could log or close fd
			}
		}
		else
			size = lseek(fd, 0L, SEEK_END);

		if (size > 0)
			map = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED,
				   fd, 0);
		close(fd);
	} else if (size > 0) {
		map = mmap(0, size, PROT_READ | PROT_WRITE,
			   MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	}
	if (map == MAP_FAILED) {
		map = NULL;
	}
	return map;
}

#if HAS_ATOMIC8
StatItem StatManager::get_stat_iterm(unsigned int id)
{
	//P __a(this);

	StatInfo *i = id_map_[id];
	if (i == NULL || i->is_sample()) {
		StatItem v;
		return v;
	}
	return (StatItem)&at_cur(i->offset());
}

StatItem StatManager::get_interval_10s_stat_iterm(unsigned int id)
{
	StatInfo *i = id_map_[id];
	if (i == NULL || i->is_sample()) {
		StatItem v;
		return v;
	}
	return (StatItem)&at_10s(i->offset());
}
#else

StatCounter StatManager::get_stat_string_counter(unsigned int id)
{
	P __a(this);

	StatInfo *i = id_map_[id];
	if (i == NULL || i->is_sample() || !i->istype(1)) {
		StatCounter v;
		return v;
	}
	i->ltype = 1;
	return (StatCounter)(int32_t *)&at_cur(i->offset());
}

StatCounter StatManager::get_stat_int_counter(unsigned int id)
{
	P __a(this);

	StatInfo *i = id_map_[id];
	if (i == NULL || i->is_sample() || i->istype(2)) {
		StatCounter v;
		return v;
	}
	i->ltype = 2;
	return (StatCounter)(uint32_t *)&at_cur(i->offset());
}

StatItem StatManager::get_stat_iterm(unsigned int id)
{
	P __a(this);

	StatInfo *i = id_map_[id];
	if (i == NULL || i->is_sample() || !i->istype(3)) {
		StatItem v;
		return v;
	}
	i->ltype = 3;

	if (i->vobj == NULL)
		i->vobj = new StatItemObject(&at_cur(i->offset()));
	return StatItem(i->vobj);
}

StatItem StatManager::get_interval_10s_stat_iterm(unsigned int id)
{
	P __a(this);

	StatInfo *i = id_map_[id];
	if (i == NULL || i->is_sample() || !i->istype(3)) {
		StatItem v;
		return v;
	}
	i->ltype = 3;

	if (i->vobj == NULL)
		i->vobj = new StatItemObject(&at_10s(i->offset()));
	return StatItem(i->vobj);
}

#endif
int64_t StatManager::get_interval_10s_stat_value(unsigned int id)
{
	P __a(this);
	int64_t ddwValue = 0;
	StatInfo *i = id_map_[id];
	if (i == NULL || i->is_sample()) {
		return ddwValue;
	}
	ddwValue = at_10s(i->offset());
	return ddwValue;
}

StatSample StatManager::get_sample(unsigned int id)
{
	P __a(this);
	StatInfo *i = id_map_[id];
	if (i == NULL || !i->is_sample()) {
		StatSample v;
		return v;
	}

	if (i->stat_sample_object == NULL)
		i->stat_sample_object = new StatSampleObject(
			i->stat_info, &at_cur(i->offset()));

	StatSample v(i->stat_sample_object);
	return v;
}

int StatManager::set_count_base(unsigned int id, const int64_t *v, int c)
{
	if (c < 0)
		c = 0;
	else if (c > 16)
		c = 16;

	P __a(this);
	StatInfo *i = id_map_[id];
	if (i == NULL || !i->is_sample())
		return -1;

	mprotect(header_, index_size_, PROT_READ | PROT_WRITE);
	if (c > 0)
		memcpy(i->stat_info->vptr, v, sizeof(int64_t) * c);
	i->stat_info->before_count = c;
	mprotect(header_, index_size_, PROT_READ);
	return c;
}

int StatManager::get_count_base(unsigned int id, int64_t *v)
{
	P __a(this);
	StatInfo *i = id_map_[id];
	if (i == NULL || !i->is_sample())
		return -1;

	if (i->stat_info->before_count > 0)
		memcpy(v, i->stat_info->vptr,
		       sizeof(int64_t) * i->stat_info->before_count);

	return i->stat_info->before_count;
}

int64_t call_imm(StatManager::StatExpression *info, const char *map)
{
	return info->value;
}

int64_t call_id(StatManager::StatExpression *info, const char *map)
{
	return *(const int64_t *)(map + info->offset_zero);
}

int64_t call_negative(StatManager::StatExpression *info, const char *map)
{
	return -*(const int64_t *)(map + info->offset_zero);
}

int64_t call_shift_1(StatManager::StatExpression *info, const char *map)
{
	return *(const int64_t *)(map + info->offset_zero) << 1;
}

int64_t call_shift_2(StatManager::StatExpression *info, const char *map)
{
	return *(const int64_t *)(map + info->offset_zero) << 2;
}

int64_t call_shift_3(StatManager::StatExpression *info, const char *map)
{
	return *(const int64_t *)(map + info->offset_zero) << 3;
}

int64_t call_shift_4(StatManager::StatExpression *info, const char *map)
{
	return *(const int64_t *)(map + info->offset_zero) << 4;
}

int64_t call_id_multi(StatManager::StatExpression *info, const char *map)
{
	return *(const int64_t *)(map + info->offset_zero) * info->value;
}

int64_t call_id_multi2(StatManager::StatExpression *info, const char *map)
{
	return *(const int64_t *)(map + info->offset_zero) *
	       *(const int64_t *)(map + info->off_one);
}

StatManager::StatExpression *
StatManager::init_stat_expression(unsigned int count, int64_t *arg)
{
	StatInfo *info;
	StatExpression *expr = new StatExpression[count];
	int val, id;
	int sub;
	for (unsigned int i = 0; i < count; i++) {
		val = arg[i] >> 32;
		id = arg[i] & 0xFFFFFFFF;
		if (id == 0) {
			expr[i].value = val;
			expr[i].offset_zero = 0;
			expr[i].off_one = 0;

			expr[i].call = &call_imm;
		} else if ((id & 0x80000000) == 0) {
			expr[i].value = val;

			sub = id % 20;
			id /= 20;
			info = id_map_[id];
			if (info == NULL || (sub > 0 && !info->is_sample())) {
				goto bad;
			}
			// not yet  initialized
			if (info->is_expr() && info->stat_expression == NULL)
				goto bad;
			expr[i].offset_zero =
				info->offset() + sub * sizeof(int64_t);
			expr[i].off_one = 0;

			switch (val) {
			case -1:
				expr[i].call = &call_negative;
				break;
			case 0:
				expr[i].call = &call_imm;
				break;
			case 1:
				expr[i].call = &call_id;
				break;
			case 2:
				expr[i].call = &call_shift_1;
				break;
			case 4:
				expr[i].call = &call_shift_2;
				break;
			case 8:
				expr[i].call = &call_shift_3;
				break;
			case 16:
				expr[i].call = &call_shift_4;
				break;
			default:
				expr[i].call = &call_id_multi;
				break;
			}
		} else {
			expr[i].value = 0;

			id &= 0x7FFFFFFF;
			sub = id % 20;
			id /= 20;
			info = id_map_[id];
			if (info == NULL || (sub > 0 && !info->is_sample())) {
				goto bad;
			}
			// not yet  initialized
			if (info->is_expr() && info->stat_expression == NULL)
				goto bad;
			expr[i].offset_zero =
				info->offset() + sub * sizeof(int64_t);

			id = val & 0x7FFFFFFF;
			sub = id % 20;
			id /= 20;
			info = id_map_[id];
			if (info == NULL || (sub > 0 && !info->is_sample())) {
				goto bad;
			}
			// not yet  initialized
			if (info->is_expr() && info->stat_expression == NULL)
				goto bad;
			expr[i].off_one =
				info->offset() + sub * sizeof(int64_t);

			expr[i].call = &call_id_multi2;
		}
	}
	return expr;
bad:
	delete[] expr;
	return NULL;
}

int64_t StatManager::calculate_stat_expression(const char *map,
					       unsigned int count,
					       StatExpression *stat_expression)
{
	int64_t v = 0;

	for (unsigned int i = 0; i < count; i++)
		v += stat_expression[i].call(&stat_expression[i], map);
	return v;
}

static inline void trend(int64_t &m, int64_t s)
{
	m = (m * 63 + (s << 10)) >> 6;
}
static inline void ltrend(int64_t &m, int64_t s)
{
	m = (m * 255 + s) >> 8;
}

void StatManager::run_job_once(void)
{
	atomic_t *a0 = (atomic_t *)&at_cur(0);
	int a0v = atomic_add_return(1, a0);
	at_cur(4 * 8) = time(NULL); // checkpoint time

	for (unsigned i = 0; i < stat_num_info_; i++) {
		unsigned offset = stat_info_[i].offset();
		switch (stat_info_[i].type()) {
		case SA_SAMPLE:
			if (stat_info_[i].stat_sample_object) {
				stat_info_[i].stat_sample_object->output(
					&at_10s(offset));
			} else {
				memcpy(&at_10s(offset), &at_cur(offset),
				       18 * sizeof(int64_t));
				memset(&at_cur(offset), 0,
				       18 * sizeof(int64_t));
			}

			for (unsigned n = 0; n < 18; n++) {
				// count all
				at_all(offset, n) += at_10s(offset, n);
				// 10m
				trend(at_10m(offset, n), at_10s(offset, n));
			}
			break;
		case SA_COUNT:
#if !HAS_ATOMIC8
			if (stat_info_[i].ltype == 1) {
				StatCounter vi((int32_t *)&at_cur(offset));
				at_10s(offset) = vi.clear();
			} else
#else
		{
			StatItem vi(&at_cur(offset));
			at_10s(offset) = vi.clear();
		}
#endif
				// count all
				at_all(offset) += at_10s(offset);

			// 10m
			trend(at_10m(offset), at_10s(offset));
			break;
		case SA_VALUE:
#if !HAS_ATOMIC8
			if (stat_info_[i].ltype == 1) {
				StatCounter vi((int32_t *)&at_cur(offset));
				at_10s(offset) = vi.get();
			} else
#else
		{
			StatItem vi(&at_cur(offset));
			at_10s(offset) = vi.get();
		}
#endif
				// 10m
				trend(at_10m(offset), at_10s(offset));
			ltrend(at_all(offset), at_10m(offset));
			break;
		case SA_CONST:
			at_10s(offset) = at_cur(offset);
			at_10m(offset) = at_cur(offset);
			at_all(offset) = at_cur(offset);
			break;
		}
	}

	// calculate expression
	for (unsigned i = 0; i < stat_info_expression_.size(); i++) {
		StatInfo &e = *(stat_info_expression_[i]);
		unsigned offset = e.offset();
		int64_t sum, div;

		if (e.stat_info->after_count == 0) {
			at_10s(offset) = calculate_stat_expression(
				map_[SC_10S], e.stat_info->before_count,
				e.stat_expression);
			at_all(offset) = calculate_stat_expression(
				map_[SC_ALL], e.stat_info->before_count,
				e.stat_expression);
		} else {
			div = calculate_stat_expression(
				map_[SC_10S], e.stat_info->after_count,
				e.stat_expression + e.stat_info->before_count);
			if (div != 0) {
				sum = calculate_stat_expression(
					map_[SC_10S], e.stat_info->before_count,
					e.stat_expression);
				at_10s(offset) = sum / div;
			}
			div = calculate_stat_expression(
				map_[SC_ALL], e.stat_info->after_count,
				e.stat_expression + e.stat_info->before_count);
			if (div != 0) {
				sum = calculate_stat_expression(
					map_[SC_ALL], e.stat_info->before_count,
					e.stat_expression);
				at_all(offset) = sum / div;
			}
		}
		trend(at_10m(offset), at_10s(offset));
	}
	atomic_set(a0 + 1, a0v);
}

int StatManager::get_socket_lock_fd(const char *type)
{
	if (socket_lock_fd_ >= 0)
		return 0;

	socket_lock_fd_ = unix_socket_lock("tlock-stat-%s-%llu-%llu", type,
					   (long long)socket_lock_device_,
					   (long long)socket_lock_ino_);
	return socket_lock_fd_ >= 0 ? 0 : -1;
}

void StatManager::clear(void)
{
	for (unsigned i = 0; i < stat_num_info_; i++) {
		unsigned offset = stat_info_[i].offset();
		switch (stat_info_[i].type()) {
		case SA_SAMPLE:
			break;
		case SA_COUNT:
			at_cur(offset) = 0;
			at_10s(offset) = 0;
			at_10m(offset) = 0;
			//			at_all(off) =0;
			break;
		case SA_VALUE:
			break;
		case SA_CONST:
			break;
		}
	}
}
