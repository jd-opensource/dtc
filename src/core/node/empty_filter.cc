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
*/

#include <limits.h>
#include <string.h>
#include <stdio.h>
#include "pt_malloc.h"
#include "empty_filter.h"
#include "algorithm/bitsop.h"

EmptyNodeFilter::EmptyNodeFilter() : _enf(0)
{
	memset(errmsg_, 0x0, sizeof(errmsg_));
}

EmptyNodeFilter::~EmptyNodeFilter()
{
}

int EmptyNodeFilter::ISSET(uint32_t key)
{
	uint32_t bitoff = get_offset(key);
	uint32_t tableid = get_index(key);

	if (_enf->enf_tables[tableid].t_size < bitoff / CHAR_BIT + 1)
		return 0;

	return ISSET_B(bitoff,
		       M_POINTER(void, _enf->enf_tables[tableid].t_handle));
}

void EmptyNodeFilter::SET(uint32_t key)
{
	uint32_t bitoff = get_offset(key);
	uint32_t tableid = get_index(key);

	if (_enf->enf_tables[tableid].t_size < bitoff / CHAR_BIT + 1) {
		/* Increase table by integer multiples of step*/
		int incbyte = bitoff / CHAR_BIT + 1 -
			      _enf->enf_tables[tableid].t_size;
		int how = (incbyte + _enf->enf_step - 1) / _enf->enf_step;
		size_t size =
			_enf->enf_tables[tableid].t_size + how * _enf->enf_step;

		_enf->enf_tables[tableid].t_handle =
			M_REALLOC(_enf->enf_tables[tableid].t_handle, size);
		if (_enf->enf_tables[tableid].t_handle == INVALID_HANDLE) {
			/* After realloc fails, will not retry*/
			return;
		}

		_enf->enf_tables[tableid].t_size = size;
	}

	return SET_B(bitoff,
		     M_POINTER(void, _enf->enf_tables[tableid].t_handle));
}

void EmptyNodeFilter::CLR(uint32_t key)
{
	uint32_t bitoff = get_offset(key);
	uint32_t tableid = get_index(key);

	if (_enf->enf_tables[tableid].t_size < bitoff / CHAR_BIT + 1)
		/* Beyond table range, return*/
		return;

	return CLR_B(bitoff,
		     M_POINTER(void, _enf->enf_tables[tableid].t_handle));
}

int EmptyNodeFilter::do_init(uint32_t total, uint32_t step, uint32_t mod)
{
	mod = mod ? mod : DF_ENF_MOD;
	step = step ? step : DF_ENF_STEP;
	total = total ? total : DF_ENF_TOTAL;

	/* allocate header */
	uint32_t size = sizeof(ENF_T);
	size += sizeof(ENF_TABLE_T) * mod;

	MEM_HANDLE_T v = M_CALLOC(size);
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "calloc %u bytes mem failed, %s", size, M_ERROR());
		return -1;
	}

	_enf = M_POINTER(ENF_T, v);

	_enf->enf_total = total;
	_enf->enf_step = step;
	_enf->enf_mod = mod;

	return 0;
}

int EmptyNodeFilter::do_attach(MEM_HANDLE_T v)
{
	if (INVALID_HANDLE == v) {
		snprintf(errmsg_, sizeof(errmsg_),
			 "attach Empty-Node Filter failed, memory handle = 0");
		return -1;
	}

	_enf = M_POINTER(ENF_T, v);
	return 0;
}

int EmptyNodeFilter::do_detach(void)
{
	_enf = 0;
	errmsg_[0] = 0;

	return 0;
}
