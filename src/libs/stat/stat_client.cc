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
#include "stat_client.h"

StatClient::StatClient(void)
{
	last_serial_number_ = 0;
}

StatClient::~StatClient(void)
{
}

int64_t StatClient::read_counter_value(Iterator_ stat_info, unsigned int cat)
{
	if (cat > get_map_size())
		return 0;
	int64_t *ptr = (int64_t *)(map_[cat] + stat_info->offset());
	if (cat == SC_10M || cat == SCC_10M)
		return ptr[0] >> 10;
	return ptr[0];
}

int64_t StatClient::read_sample_average(Iterator_ stat_info, unsigned int cat)
{
	if (cat > get_map_size() || stat_info->is_sample() == 0)
		return 0;
	int64_t *ptr = (int64_t *)(map_[cat] + stat_info->offset());

	return ptr[1] ? ptr[0] / ptr[1] : 0;
}

int64_t StatClient::read_sample_counter(Iterator_ stat_info, unsigned int cat,
					unsigned int count)
{
	if (cat > get_map_size() || stat_info->is_sample() == 0 ||
	    count > stat_info->count())
		return 0;
	int64_t *ptr = (int64_t *)(map_[cat] + stat_info->offset());
	if (cat == SC_10M || cat == SCC_10M)
		return ptr[count + 1] >> 10;
	return ptr[count + 1];
}

int StatClient::init_stat_info(const char *name, const char *index_file)
{
	int ret = StatManager::init_stat_info(name, index_file, 1);
	if (ret < 0)
		return ret;
	mprotect(map_[SC_CUR], header_->data_size, PROT_READ);
	mprotect(map_[SC_10S], header_->data_size, PROT_READ);
	mprotect(map_[SC_10M], header_->data_size, PROT_READ);
	mprotect(map_[SC_ALL], header_->data_size, PROT_READ);
	map_[SCC_10S] =
		(char *)mmap(0, header_->data_size, PROT_READ | PROT_WRITE,
			     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	map_[SCC_10M] =
		(char *)mmap(0, header_->data_size, PROT_READ | PROT_WRITE,
			     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	map_[SCC_ALL] =
		(char *)mmap(0, header_->data_size, PROT_READ | PROT_WRITE,
			     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	return ret;
}

int StatClient::check_point(void)
{
	memcpy(map_[SCC_10S], map_[SC_10S], header_->data_size);
	memcpy(map_[SCC_10M], map_[SC_10M], header_->data_size);
	memcpy(map_[SCC_ALL], map_[SC_ALL], header_->data_size);
	return 0;
}
