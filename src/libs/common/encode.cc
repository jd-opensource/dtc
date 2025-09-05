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
#include <stdint.h>
#include <endian.h>
#include <byteswap.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>

#include "packet/packet.h"
#include "table/table_def.h"
#include "decode/decode.h"
#include "task/task_base.h"

/* encoding DataValue by type */
char *encode_data_value(char *p, const DTCValue *v, int type)
{
	char *t;
	int n;

	switch (type) {
	case DField::None:
		*p++ = 0;
		break;

	case DField::Signed:
	case DField::Unsigned:
		if (v->s64 >= 0) {
			if (v->s64 < 0x80LL)
				n = 1;
			else if (v->s64 < 0x8000LL)
				n = 2;
			else if (v->s64 < 0x800000LL)
				n = 3;
			else if (v->s64 < 0x80000000LL)
				n = 4;
			else if (v->s64 < 0x8000000000LL)
				n = 5;
			else if (v->s64 < 0x800000000000LL)
				n = 6;
			else if (v->s64 < 0x80000000000000LL)
				n = 7;
			else
				n = 8;
		} else {
			if (v->s64 >= -0x80LL)
				n = 1;
			else if (v->s64 >= -0x8000LL)
				n = 2;
			else if (v->s64 >= -0x800000LL)
				n = 3;
			else if (v->s64 >= -0x80000000LL)
				n = 4;
			else if (v->s64 >= -0x8000000000LL)
				n = 5;
			else if (v->s64 >= -0x800000000000LL)
				n = 6;
			else if (v->s64 >= -0x80000000000000LL)
				n = 7;
			else
				n = 8;
		}
		t = (char *)&v->s64;
		*p++ = n;
#if __BYTE_ORDER == __BIG_ENDIAN
		for (int i = 0; i < n; i++)
			p[i] = t[7 - n + i];
#else
		for (int i = 0; i < n; i++)
			p[i] = t[n - 1 - i];
#endif
		p += n;
		break;

	case DField::Float:
		char buf[sizeof(long double) * 2 + 8];
		if (snprintf(buf, sizeof(buf), __FLTFMT__,
			     (long double)v->flt) >= (int)sizeof(buf))
			memcpy(buf, "NAN", 4);
		n = strlen(buf) + 1;
		*p++ = n;
		memcpy(p, buf, n);
		p += n;
		break;

	case DField::String:
	case DField::Binary:
		if (v->str.ptr == NULL) {
			*p++ = 0;
		} else {
			p = encode_length(p, v->str.len + 1);
			if (v->bin.len)
				memcpy(p, v->bin.ptr, v->str.len);
			p += v->str.len;
			*p++ = '\0';
		}
		break;
	}
	return p;
}

int encoded_bytes_data_value(const DTCValue *v, int type)
{
	switch (type) {
	case DField::None:
		return 1;
	case DField::Signed:
	case DField::Unsigned:
		if (v->s64 >= 0) {
			if (v->s64 < 0x80LL)
				return 2;
			if (v->s64 < 0x8000LL)
				return 3;
			if (v->s64 < 0x800000LL)
				return 4;
			if (v->s64 < 0x80000000LL)
				return 5;
			if (v->s64 < 0x8000000000LL)
				return 6;
			if (v->s64 < 0x800000000000LL)
				return 7;
			if (v->s64 < 0x80000000000000LL)
				return 8;
		} else {
			if (v->s64 >= -0x80LL)
				return 2;
			if (v->s64 >= -0x8000LL)
				return 3;
			if (v->s64 >= -0x800000LL)
				return 4;
			if (v->s64 >= -0x80000000LL)
				return 5;
			if (v->s64 >= -0x8000000000LL)
				return 6;
			if (v->s64 >= -0x800000000000LL)
				return 7;
			if (v->s64 >= -0x80000000000000LL)
				return 8;
		}
		return 9;
	case DField::Float:
		char b[sizeof(long double) * 2 + 8];
		if (snprintf(b, sizeof(b), __FLTFMT__, (long double)v->flt) >=
		    (int)sizeof(b))
			return 5;
		return 2 + strlen(b);
	case DField::String:
	case DField::Binary:
		if (v->str.ptr == NULL)
			return 1;
		return v->str.len + 1 + encoded_bytes_length(v->str.len + 1);
	}
	return 0;
}

/*
 * Encoding simple section
 * <ID> <LEN> <VAL> <ID> <LEN> <VAL>...
 */
int encoded_bytes_simple_section(const SimpleSection &sct, uint8_t kt)
{
	int len = 0;
	for (int i = 0; i <= sct.max_tags(); i++) {
		if (sct.tag_present(i) == 0)
			continue;
		const int t = i == 0 ? kt : sct.tag_type(i);
		len += 1 + encoded_bytes_data_value(sct.get_tag(i), t);
	}
	return len;
}

char *encode_simple_section(char *p, const SimpleSection &sct, uint8_t kt)
{
	for (int i = 0; i <= sct.max_tags(); i++) {
		if (sct.tag_present(i) == 0)
			continue;
		const int t = i == 0 ? kt : sct.tag_type(i);
		*p++ = i;
		p = encode_data_value(p, sct.get_tag(i), t);
	}
	return p;
}

/*
 * FieldSet format:
 * 	<NUM> <ID>...
 * 		NUM: 1 byte, total fields
 * 		ID: 1 byte per fieldid
 */
int encoded_bytes_field_set(const DTCFieldSet &fs)
{
	if (fs.num_fields() == 0)
		return 0;
	if (fs.field_present(0)) {
		return fs.num_fields() + 2;
	}
	return fs.num_fields() + 1;
}

char *encode_field_set(char *p, const DTCFieldSet &fs)
{
	if (fs.num_fields() == 0)
		return p;
	*p++ = fs.num_fields();
	for (int i = 0; i < fs.num_fields(); i++) {
		*p++ = fs.field_id(i);
		if (fs.field_id(i) == 0)
			*p++ = 0;
	}
	return p;
}

/*
 * field_value format:
 * 	<NUM> <OPER:TYPE> <ID> <LEN> <VALUE>...
 * 		NUM: 1 byte, total fields
 * 		ID: 1 byte
 */
int encoded_bytes_field_value(const DTCFieldValue &fv)
{
	if (fv.num_fields() == 0)
		return 0;
	int len = 1;
	for (int i = 0; i < fv.num_fields(); i++) {
		//for migrate
		if (fv.field_id(i) == 0)
			len++;
		len += 2 + encoded_bytes_data_value(fv.field_value(i),
						    fv.field_type(i));
	}
	return len;
}

char *encode_field_value(char *p, const DTCFieldValue &fv)
{
	if (fv.num_fields() == 0)
		return p;
	*p++ = fv.num_fields();
	for (int i = 0; i < fv.num_fields(); i++) {
		const int n = fv.field_id(i);
		const int t = fv.field_type(i);
		*p++ = (fv.field_operation(i) << 4) + t;
		*p++ = n;
		//for migrate
		if (n == 0)
			*p++ = 0;

		p = encode_data_value(p, fv.field_value(i), t);
	}
	return p;
}

int encoded_bytes_multi_key(const DTCValue *v, const DTCTableDefinition *tdef)
{
	if (tdef->key_fields() <= 1)
		return 0;
	int len = 1;
	for (int i = 1; i < tdef->key_fields(); i++)
		len += 2 + encoded_bytes_data_value(v + i, tdef->field_type(i));
	return len;
}

char *encode_multi_key(char *p, const DTCValue *v,
		       const DTCTableDefinition *tdef)
{
	if (tdef->key_fields() <= 1)
		return p;
	*p++ = tdef->key_fields() - 1;
	for (int i = 1; i < tdef->key_fields(); i++) {
		const int t = tdef->field_type(i);
		*p++ = ((DField::Set) << 4) + t;
		*p++ = i;
		p = encode_data_value(p, v + i, t);
	}
	return p;
}

/*
 * FieldSet format:
 * 	<NUM> <FIELD> <FIELD>...
 * 		NUM: 1 byte, total fields
 * 		FIELD: encoded field ID/name
 */
int encoded_bytes_field_set(const FieldSetByName &fs)
{
	if (fs.num_fields() == 0)
		return 0;
	int len = 1;
	for (int i = 0; i < fs.num_fields(); i++) {
		switch (fs.field_id(i)) {
		case 255:
			len += 2 + fs.field_name_length(i);
			break;
		case 0:
			len += 2;
			break;
		default:
			len += 1;
		}
	}
	return len;
}

char *encode_field_set(char *p, const FieldSetByName &fs)
{
	if (fs.num_fields() == 0)
		return p;
	*p++ = fs.num_fields();
	for (int i = 0; i < fs.num_fields(); i++) {
		switch (fs.field_id(i)) {
		case 0:
			*p++ = 0;
			*p++ = 0;
			break;

		default:
			*p++ = fs.field_id(i);
			break;

		case 255:
			*p++ = 0;
			const int n = fs.field_name_length(i);
			*p++ = n;
			memcpy(p, fs.field_name(i), n);
			p += n;
			break;
		}
	}
	return p;
}

/*
 * field_value format:
 * 	<NUM> <OPER:TYPE> <FIELD> <LEN> <VALUE>
 * 		NUM: 1 byte, total fields
 * 		FIELD: encoded field ID/name
 */
int encoded_bytes_field_value(const FieldValueByName &fv)
{
	if (fv.num_fields() == 0)
		return 0;
	int len = 1;
	for (int i = 0; i < fv.num_fields(); i++) {
		switch (fv.field_id(i)) {
		case 255:
			len += 3 + fv.field_name_length(i);
			break;
		case 0:
			len += 3;
			break;
		default:
			len += 2;
		}
		len += encoded_bytes_data_value(fv.field_value(i),
						fv.field_type(i));
	}
	return len;
}

char *encode_field_value(char *p, const FieldValueByName &fv)
{
	if (fv.num_fields() == 0)
		return p;
	*p++ = fv.num_fields();
	for (int i = 0; i < fv.num_fields(); i++) {
		int n = fv.field_name_length(i);
		int t = fv.field_type(i);
		if (t == DField::Unsigned)
			t = DField::Signed;
		*p++ = (fv.field_operation(i) << 4) + t;
		switch (fv.field_id(i)) {
		case 0:
			*p++ = 0;
			*p++ = 0;
			break;
		case 255:
			*p++ = 0;
			*p++ = n;
			memcpy(p, fv.field_name(i), n);
			p += n;
			break;
		default:
			*p++ = fv.field_id(i);
		}

		p = encode_data_value(p, fv.field_value(i), t);
	}
	return p;
}
