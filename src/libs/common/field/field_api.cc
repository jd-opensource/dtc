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
#include "field.h"
#include "field_api.h"
#include "dtc_error_code.h"
#include "algorithm/bitsop.h"
#include "../log/log.h"

int check_int_value(const DTCValue &v1, int type, int size)
{
	int res = 0;
	switch (size * 2 + type) {
	default:
		break;
	case 1 * 2 + DField::Signed:
		if (v1.s64 < -0x80 || v1.s64 > 0x7F)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 1 * 2 + DField::Unsigned:
		if (v1.s64 < 0 || v1.s64 > 0xFF)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 2 * 2 + DField::Signed:
		if (v1.s64 < -0x8000 || v1.s64 > 0x7FFF)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 2 * 2 + DField::Unsigned:
		if (v1.s64 < 0 || v1.s64 > 0xFFFF)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 3 * 2 + DField::Signed:
		if (v1.s64 < -0x800000 || v1.s64 > 0x7FFFFF)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 3 * 2 + DField::Unsigned:
		if (v1.s64 < 0 || v1.s64 > 0xFFFFFF)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 4 * 2 + DField::Signed:
		if (v1.s64 < -0x80000000LL || v1.s64 > 0x7FFFFFFF)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 4 * 2 + DField::Unsigned:
		if (v1.s64 < 0 || v1.s64 > 0xFFFFFFFFLL)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 5 * 2 + DField::Signed:
		if (v1.s64 < -0x8000000000LL || v1.s64 > 0x7FFFFFFFFFLL)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 5 * 2 + DField::Unsigned:
		if (v1.s64 < 0 || v1.s64 > 0xFFFFFFFFFFLL)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 6 * 2 + DField::Signed:
		if (v1.s64 < -0x800000000000LL || v1.s64 > 0x7FFFFFFFFFFFLL)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 6 * 2 + DField::Unsigned:
		if (v1.s64 < 0 || v1.s64 > 0xFFFFFFFFFFFFLL)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 7 * 2 + DField::Signed:
		if (v1.s64 < -0x80000000000000LL || v1.s64 > 0x7FFFFFFFFFFFFFLL)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	case 7 * 2 + DField::Unsigned:
		if (v1.s64 < 0 || v1.s64 > 0xFFFFFFFFFFFFFFLL)
			res = -EC_BAD_FIELD_SIZE_ON_RESOLVE;
		break;
	}
	return res;
}

/*
 * resolve fieldname at client side
 * 	solved: all field has resolved
 */
int FieldValueByName::Resolve(const DTCTableDefinition *tdef, int force)
{
	if (force)
		solved = 0;
	else if (solved)
		return 0;
	else if (numFields == 0)
		return 0;

	if (tdef == NULL)
		return -EINVAL;

	for (int i = 0; i < numFields; i++) {
		if (fieldValue[i].name == NULL)
			continue;
		const int fid =
			tdef->field_id(fieldValue[i].name, fieldValue[i].nlen);
		if (fid < 0)
			return -EC_BAD_FIELD_NAME;

		if (tdef->field_type(fid) == 1 || tdef->field_type(fid) == 2) {
			int res = check_int_value(fieldValue[i].val,
						  tdef->field_type(fid),
						  tdef->field_size(fid));
			if (res < 0) {
				return res;
			}
		} else if (tdef->field_type(fid) == 4 ||
			   tdef->field_type(fid) == 5) {
			if (tdef->field_size(fid) < fieldValue[i].val.str.len) {
				return -EC_BAD_FIELD_SIZE_ON_RESOLVE;
			}
		} else if (tdef->field_type(fid) == 3) {
			continue;
		} else {
			return -EC_EXCEPTION_ON_RESOLVE;
		}
		fieldValue[i].fid = fid;
	}
	solved = 1;
	return 0;
}

/*
 * resolve fieldname at client side
 * 	solved: all field has resolved
 */
void FieldValueByName::Unresolve(void)
{
	for (int i = 0; i < numFields; i++) {
		if (fieldValue[i].name != NULL) {
			fieldValue[i].fid = INVALID_FIELD_ID;
			solved = 0;
		}
	}
}

/*
 * resolve fieldname at client side
 * 	solved: all field has resolved
 */
int FieldSetByName::Resolve(const DTCTableDefinition *tdef, int force)
{
	if (force)
		solved = 0;
	else if (solved)
		return 0;
	else if (numFields == 0)
		return 0;
	if (tdef == NULL)
		return -EINVAL;
	for (int i = 0; i < numFields; i++) {
		if (fieldValue[i].name == NULL)
			continue;
		const int fid =
			tdef->field_id(fieldValue[i].name, fieldValue[i].nlen);
		//	    if(fid<=0)
		if (fid < 0)
			return -EC_BAD_FIELD_NAME;
		fieldValue[i].fid = fid;
	}
	solved = 1;
	return 0;
}

/*
 * resolve fieldname at client side
 * 	solved: all field has resolved
 */
void FieldSetByName::Unresolve(void)
{
	for (int i = 0; i < numFields; i++) {
		if (fieldValue[i].name != NULL) {
			fieldValue[i].fid = INVALID_FIELD_ID;
			solved = 0;
		}
	}
}

const uint8_t *FieldSetByName::virtual_map(void) const
{
	if (maxvid == 0)
		return NULL;
	if (solved == 0)
		return NULL;
	uint8_t *m = (uint8_t *)calloc(1, maxvid);
	if (m == NULL)
		throw std::bad_alloc();
	for (int i = 0; i < numFields; i++) {
		if (fieldValue[i].vid)
			m[fieldValue[i].vid - 1] = fieldValue[i].fid;
	}
	return m;
}

int FieldSetByName::add_field(const char *name, int vid)
{
	int nlen = strlen(name);
	if (nlen >= 1024) {
		return -EC_BAD_FIELD_NAME;
	}
	if (vid < 0 || vid >= 256) {
		vid = 0;
		return -EINVAL;
	} else if (vid) {
		for (int i = 0; i < numFields; i++) {
			if (fieldValue[i].vid == vid)
				return -EINVAL;
		}
	}
	if (numFields == maxFields) {
		if (maxFields == 255)
			return -E2BIG;
		int n = maxFields + 8;
		if (n > 255)
			n = 255;
		typeof(fieldValue) p;
		if (fieldValue == NULL) {
			p = (typeof(fieldValue))MALLOC(n * sizeof(*fieldValue));
		} else {
			p = (typeof(fieldValue))REALLOC(
				fieldValue, n * sizeof(*fieldValue));
		}
		if (p == NULL)
			throw std::bad_alloc();
		fieldValue = p;
		maxFields = n;
	}

	char *str = (char *)MALLOC(nlen + 1);
	if (str == NULL)
		throw std::bad_alloc();
	memcpy(str, name, nlen + 1);
	fieldValue[numFields].name = str;
	fieldValue[numFields].nlen = nlen;
	fieldValue[numFields].fid = INVALID_FIELD_ID; // allow select key-field
	fieldValue[numFields].vid = vid;
	if (vid > maxvid)
		maxvid = vid;
	solved = 0;
	numFields++;
	return 0;
}

int FieldValueByName::add_value(const char *name, uint8_t op, uint8_t type,
				const DTCValue &val)
{
	int nlen = strlen(name);
	if (nlen >= 1024) {
		return -EC_BAD_FIELD_NAME;
	}
	if (numFields == maxFields) {
		if (maxFields == 255)
			return -E2BIG;
		int n = maxFields + 8;
		if (n > 255)
			n = 255;
		typeof(fieldValue) p;
		if (fieldValue == NULL) {
			p = (typeof(fieldValue))MALLOC(n * sizeof(*fieldValue));
		} else {
			p = (typeof(fieldValue))REALLOC(
				fieldValue, n * sizeof(*fieldValue));
		}
		if (p == NULL)
			throw std::bad_alloc();
		fieldValue = p;
		maxFields = n;
	}

	char *str = (char *)MALLOC(nlen + 1);
	if (str == NULL)
		throw std::bad_alloc();
	memcpy(str, name, nlen + 1);
	fieldValue[numFields].name = str;
	fieldValue[numFields].nlen = nlen;
	fieldValue[numFields].type = type;
	fieldValue[numFields].fid = INVALID_FIELD_ID;
	fieldValue[numFields].oper = op;
	fieldValue[numFields].val = val;
	if (type == DField::String || type == DField::Binary) {
		if (val.bin.ptr != NULL) {
			char *p = (char *)MALLOC(val.bin.len + 1);
			if (p == NULL)
				throw std::bad_alloc();
			memcpy(p, val.bin.ptr, val.bin.len);
			p[val.bin.len] = '\0';
			fieldValue[numFields].val.bin.ptr = p;
		}
	}
	solved = 0;
	numFields++;
	return 0;
}
