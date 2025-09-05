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
#include <new>
#include <stdio.h>
#include <time.h>
#include "field.h"
#include "dtc_error_code.h"
#include "algorithm/bitsop.h"
#include "../log/log.h"

#define TP(x, y, z) (((x) << 16) + ((y) << 8) + (z))
#define TC(x, y, z) ((DField::x << 16) + (DField::y << 8) + DField::z)

static void TruncateIntValue(DTCValue &v1, int type, int size)
{
	switch (size * 2 + type) {
	default:
		break;
	case 1 * 2 + DField::Signed:
		if (v1.s64 < -0x80)
			v1.s64 = -0x80;
		else if (v1.s64 > 0x7F)
			v1.s64 = 0x7F;
		break;
	case 1 * 2 + DField::Unsigned:
		if (v1.s64 < 0)
			v1.s64 = 0;
		else if (v1.s64 > 0xFF)
			v1.s64 = 0xFF;
		break;
	case 2 * 2 + DField::Signed:
		if (v1.s64 < -0x8000)
			v1.s64 = -0x8000;
		else if (v1.s64 > 0x7FFF)
			v1.s64 = 0x7FFF;
		break;
	case 2 * 2 + DField::Unsigned:
		if (v1.s64 < 0)
			v1.s64 = 0;
		else if (v1.s64 > 0xFFFF)
			v1.s64 = 0xFFFF;
		break;
	case 3 * 2 + DField::Signed:
		if (v1.s64 < -0x800000)
			v1.s64 = -0x800000;
		else if (v1.s64 > 0x7FFFFF)
			v1.s64 = 0x7FFFFF;
		break;
	case 3 * 2 + DField::Unsigned:
		if (v1.s64 < 0)
			v1.s64 = 0;
		else if (v1.s64 > 0xFFFFFF)
			v1.s64 = 0xFFFFFF;
		break;
	case 4 * 2 + DField::Signed:
		if (v1.s64 < -0x80000000LL)
			v1.s64 = -0x80000000LL;
		else if (v1.s64 > 0x7FFFFFFF)
			v1.s64 = 0x7FFFFFFF;
		break;
	case 4 * 2 + DField::Unsigned:
		if (v1.s64 < 0)
			v1.s64 = 0;
		else if (v1.s64 > 0xFFFFFFFFLL)
			v1.s64 = 0xFFFFFFFFLL;
		break;
	case 5 * 2 + DField::Signed:
		if (v1.s64 < -0x8000000000LL)
			v1.s64 = -0x8000000000LL;
		else if (v1.s64 > 0x7FFFFFFFFFLL)
			v1.s64 = 0x7FFFFFFFFFLL;
		break;
	case 5 * 2 + DField::Unsigned:
		if (v1.s64 < 0)
			v1.s64 = 0;
		else if (v1.s64 > 0xFFFFFFFFFFLL)
			v1.s64 = 0xFFFFFFFFFFLL;
		break;
	case 6 * 2 + DField::Signed:
		if (v1.s64 < -0x800000000000LL)
			v1.s64 = -0x800000000000LL;
		else if (v1.s64 > 0x7FFFFFFFFFFFLL)
			v1.s64 = 0x7FFFFFFFFFFFLL;
		break;
	case 6 * 2 + DField::Unsigned:
		if (v1.s64 < 0)
			v1.s64 = 0;
		else if (v1.s64 > 0xFFFFFFFFFFFFLL)
			v1.s64 = 0xFFFFFFFFFFFFLL;
		break;
	case 7 * 2 + DField::Signed:
		if (v1.s64 < -0x80000000000000LL)
			v1.s64 = -0x80000000000000LL;
		else if (v1.s64 > 0x7FFFFFFFFFFFFFLL)
			v1.s64 = 0x7FFFFFFFFFFFFFLL;
		break;
	case 7 * 2 + DField::Unsigned:
		if (v1.s64 < 0)
			v1.s64 = 0;
		else if (v1.s64 > 0xFFFFFFFFFFFFFFLL)
			v1.s64 = 0xFFFFFFFFFFFFFFLL;
		break;
	}
}
/*
 * Update RowValue by Operation:this:
 * 	left side: RowValue
 * 	right side: this
 * valid right side type: Signed, Float, String.
 * 	Unsigned encoded as Signed, Binary encoded as String
 */
int DTCFieldValue::Update(RowValue &r)
{
	for (int i = 0; i < num_fields(); i++) {
		const int id = fieldValue[i].id;
		DTCValue &v1 = r[id];
		const DTCValue &v2 = fieldValue[i].val;

		switch (TP(r.field_type(id), fieldValue[i].type,
			   fieldValue[i].oper)) {
		default:
			log4cplus_info("id:%d TC(%d,%d,%d) not surport", id,
				       r.field_type(id), fieldValue[i].type,
				       fieldValue[i].oper);
			break;
		case TC(Signed, Signed, Set):
		case TC(Unsigned, Signed, Set):
		case TC(Unsigned, Unsigned, Set):
			v1.s64 = v2.s64;
			TruncateIntValue(v1, r.field_type(id),
					 r.field_size(id));
			break;

		case TC(Signed, Signed, Add):
		case TC(Unsigned, Signed, Add):
			if (v1.s64 > 0) {
				v1.s64 += v2.s64;
				if (v2.s64 > 0 && v1.s64 < 0) {
					/* overflow */
					v1.s64 = 0x7FFFFFFFFFFFFFFFLL;
				}
			} else {
				v1.s64 += v2.s64;
				if (v2.s64 < 0 && v1.s64 > 0) {
					/* overflow */
					v1.s64 = 0x8000000000000000LL;
				}
			}
			TruncateIntValue(v1, r.field_type(id),
					 r.field_size(id));
			break;

		case TC(Signed, Signed, OR):
		case TC(Unsigned, Signed, OR):
			v1.s64 |= v2.s64;
			TruncateIntValue(v1, r.field_type(id),
					 r.field_size(id));
			break;

		case TC(Float, Signed, Set):
			v1.flt = v2.s64;
			break;
		case TC(Float, Unsigned, Set):
			v1.flt = v2.u64;
			break;
		case TC(Float, Float, Set):
			v1.flt = v2.flt;
			break;

		case TC(Float, Signed, Add):
			v1.flt += v2.s64;
			break;
		case TC(Float, Unsigned, Add):
			v1.flt += v2.u64;
			break;
		case TC(Float, Float, Add):
			v1.flt += v2.flt;
			break;

		case TC(String, String, Set):
			v1.str = v2.str;
			if (v1.str.len > r.field_size(id))
				v1.str.len = r.field_size(id);
			break;
		case TC(Binary, String, Set):
			v1.bin = v2.bin;
			if (v1.str.len > r.field_size(id))
				v1.str.len = r.field_size(id);
			break;
		case TC(Binary, Binary, Set):
			v1.bin = v2.bin;
			if (v1.str.len > r.field_size(id))
				v1.str.len = r.field_size(id);
			break;

		//setbits operation
		case TC(Signed, Signed, SetBits):
		case TC(Signed, Unsigned, SetBits):
		case TC(Unsigned, Signed, SetBits):
		case TC(Unsigned, Unsigned, SetBits): {
			const int len = 8;
			unsigned int off = v2.u64 >> 32;
			unsigned int size = off >> 24;
			off &= 0xFFFFFF;
			unsigned int value = v2.u64 & 0xFFFFFFFF;

			if (off >= 8 * len || size == 0)
				break;
			if (size > 32)
				size = 32;
			if (size > 8 * len - off)
				size = 8 * len - off;

			log4cplus_debug(
				"SetMultBits, off:%d, size:%d, value:%d", off,
				size, value);

			uint64_t mask = ((1ULL << size) - 1) << off;
			v1.u64 &= ~mask;
			v1.u64 |= ((uint64_t)value << off) & mask;
		} break;
		case TC(Binary, Signed, SetBits):
		case TC(Binary, Unsigned, SetBits):
		case TC(String, Signed, SetBits):
		case TC(String, Unsigned, SetBits): {
			const int len = v1.bin.len;
			int off = v2.u64 >> 32;
			int size = off >> 24;
			off &= 0xFFFFFF;
			unsigned int value = v2.u64 & 0xFFFFFFFF;
			if (off >= 8 * len || size == 0)
				break;
			if (size > 32)
				size = 32;
			if (size > 8 * len - off)
				size = 8 * len - off;

			for (size += off; off < size; off++, value >>= 1) {
				if ((value & 1) == 0)
					CLR_B(off, v1.bin.ptr);
				else
					SET_B(off, v1.bin.ptr);
			}
		} break;
		}
	}
	return 0;
}

int DTCFieldValue::Compare(const RowValue &r, int iCmpFirstNRows)
{
	for (int i = 0; i < num_fields(); i++) {
		const int id = fieldValue[i].id;
		if (id < r.table_definition()->key_fields() ||
		    id > iCmpFirstNRows - 1)
			continue;
		const DTCValue &v1 = r[id];
		const DTCValue &v2 = fieldValue[i].val;

		switch (TP(r.field_type(id), fieldValue[i].type,
			   fieldValue[i].oper)) {
		default:
			return 0;
		case TC(Signed, Signed, EQ):
		case TC(Unsigned, Signed, EQ):
			if (v1.s64 == v2.s64)
				break;
			else
				return 0;

		case TC(Signed, Signed, NE):
		case TC(Unsigned, Signed, NE):
			if (v1.s64 != v2.s64)
				break;
			else
				return 0;

		case TC(Signed, Signed, LT):
		case TC(Unsigned, Signed, LT):
			if (v1.s64 < v2.s64)
				break;
			else
				return 0;

		case TC(Signed, Signed, LE):
		case TC(Unsigned, Signed, LE):
			if (v1.s64 <= v2.s64)
				break;
			else
				return 0;

		case TC(Signed, Signed, GT):
		case TC(Unsigned, Signed, GT):
			if (v1.s64 > v2.s64)
				break;
			else
				return 0;

		case TC(Signed, Signed, GE):
		case TC(Unsigned, Signed, GE):
			if (v1.s64 >= v2.s64)
				break;
			else
				return 0;

		/* case insensitive for string comparison */
		case TC(String, String, EQ):
			if (string_equal(v1, v2))
				break;
			else
				return 0;
		case TC(String, String, NE):
			if (!string_equal(v1, v2))
				break;
			else
				return 0;
		case TC(String, String, GT):
			if (string_greater(v1, v2))
				break;
			else
				return 0;
		case TC(String, String, GE):
			if (string_greater_equal(v1, v2))
				break;
			else
				return 0;					
		case TC(String, String, LT):
			if (string_less(v1, v2))
				break;
			else
				return 0;
		case TC(String, String, LE):
			if (string_less_equal(v1, v2))
				break;
			else
				return 0;								

		/* case sensitive for binary comparison */
		case TC(Binary, Binary, EQ):
		case TC(String, Binary, EQ):
		case TC(Binary, String, EQ):
			if (binary_equal(v1, v2))
				break;
			else
				return 0;
		case TC(Binary, Binary, NE):
		case TC(String, Binary, NE):
		case TC(Binary, String, NE):
			if (!binary_equal(v1, v2))
				break;
			else
				return 0;
		}
	}
	return 1;
}

int RowValue::Compare(const RowValue &rv, uint8_t *fieldIDList, uint8_t num) const
{
	for (int i = 0; i < num; ++i) {
		switch (field_type(fieldIDList[i])) {
		case DField::Signed:
		case DField::Unsigned:
			if (value[fieldIDList[i]].u64 !=
			    rv.field_value(fieldIDList[i])->u64)
				return -1;
			break;
		case DField::Float: //floating point number
			//floating point numbers not allowed for comparison
			return -2;
			break;
		case DField::String: //string
			if (!string_equal(value[fieldIDList[i]],
					  *(rv.field_value(fieldIDList[i]))))
				return -1;
			break;
		case DField::Binary: //binary data
			if (!binary_equal(value[fieldIDList[i]],
					  *(rv.field_value(fieldIDList[i]))))
				return -1;
			break;
		default:
			return -3;
			break;
		}
	}
	return 0;
};

int RowValue::Copy(const RowValue *r)
{
	// left value: target, right value: source
	int id = -1;
	for (int i = 0; i < num_fields(); ++i) {
		if ((id = r->field_id(field_name(i + 1))) == -1)
			continue;
		memcpy(value + i + 1, r->value + id, sizeof(DTCValue));
	}
	return 0;
}

void RowValue::update_timestamp(uint32_t now, int updateall)
{
	const DTCTableDefinition *tdef = table_definition();
	int id;

	id = tdef->lastacc_field_id();
	if (id > 0) {
		value[id].u64 = now;
	}

	id = tdef->lastmod_field_id();
	if (id > 0) {
		value[id].u64 = now;
	}

	id = tdef->lastcmod_field_id();
	if (id > 0 && updateall) {
		value[id].u64 = now;
	}
}

void RowValue::update_expire_time()
{
	const DTCTableDefinition *tdef = table_definition();
	int id = tdef->expire_time_field_id();
	if (id > 0) {
		if (value[id].s64 > 0) {
			value[id].s64 = value[id].s64 - time(NULL);
		}
	}
}
