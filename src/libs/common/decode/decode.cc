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
#include <unistd.h>
#include <errno.h>
#include <endian.h>
#include <byteswap.h>
#include <time.h>
#include <stdio.h>

#include "decode.h"
#include "dtc_error_code.h"
#include "../log/log.h"
#include "../table/../table/table_def.h"
#define CONVERT_NULL_TO_EMPTY_STRING 1

// BYTE0           BYTE1   BYTE2   BYTE3   BYTE4
// 0-1110111						<224
// 11110000-1100   8					<3328
// 11111101        8       8				<64K
// 11111110        8       8       8			<16M
// 11111111        8       8       8       8		<4G
int decode_length(DTCBinary &bin, uint32_t &len)
{
	if (!bin)
		return -EC_BAD_SECTION_LENGTH;
	len = *bin++;
	if (len < 240) {
	} else if (len <= 252) {
		if (!bin)
			return -EC_BAD_SECTION_LENGTH;
		len = ((len & 0xF) << 8) + *bin++;
	} else if (len == 253) {
		if (bin < 2)
			return -EC_BAD_SECTION_LENGTH;
		len = (bin[0] << 8) + bin[1];
		bin += 2;
	} else if (len == 254) {
		if (bin < 3)
			return -EC_BAD_SECTION_LENGTH;
		len = (bin[0] << 16) + (bin[1] << 8) + bin[2];
		bin += 3;
	} else {
		if (bin < 4)
			return -EC_BAD_SECTION_LENGTH;
		len = (bin[0] << 24) + (bin[1] << 16) + (bin[2] << 8) + bin[3];
		bin += 4;
		if (len > MAXPACKETSIZE)
			return -EC_BAD_VALUE_LENGTH;
	}
	return 0;
}

char *encode_length(char *p, uint32_t len)
{
	if (len < 240) {
		p[0] = len;
		return p + 1;
	} else if (len < (13 << 8)) {
		p[0] = 0xF0 + (len >> 8);
		p[1] = len & 0xFF;
		return p + 2;
	} else if (len < (1 << 16)) {
		p[0] = 253;
		p[1] = len >> 8;
		p[2] = len & 0xFF;
		return p + 3;
	} else if (len < (1 << 24)) {
		p[0] = 254;
		p[1] = len >> 16;
		p[2] = len >> 8;
		p[3] = len & 0xFF;
		return p + 4;
	} else {
		p[0] = 255;
		p[1] = len >> 24;
		p[2] = len >> 16;
		p[3] = len >> 8;
		p[4] = len & 0xFF;
		return p + 5;
	}
	return 0;
}

int encoded_bytes_length(uint32_t n)
{
	if (n < 240)
		return 1;
	if (n < (13 << 8))
		return 2;
	if (n < (1 << 16))
		return 3;
	if (n < (1 << 24))
		return 4;
	return 5;
}

/*
 * decode value by type
 * datavalue format:
 * 	<LEN> <VAL>
 */
int decode_data_value(DTCBinary &bin, DTCValue &val, int type)
{
	uint8_t *p;
	uint32_t len;
	int rv;

	rv = decode_length(bin, len);
	if (rv) {
		return rv;
	}

	if (bin.len < (int64_t)len)
		return -EC_BAD_SECTION_LENGTH;

	switch (type) {
	case DField::None:
		break;

	case DField::Signed:
	case DField::Unsigned:
		/* integer always encoded as signed value */
		if (len == 0 || len > 8)
			return -EC_BAD_VALUE_LENGTH;
		p = (uint8_t *)bin.ptr + 1;
		int64_t s64;
		s64 = *(int8_t *)bin.ptr;
		switch (len) {
		case 8:
			s64 = (s64 << 8) | *p++;
		case 7:
			s64 = (s64 << 8) | *p++;
		case 6:
			s64 = (s64 << 8) | *p++;
		case 5:
			s64 = (s64 << 8) | *p++;
		case 4:
			s64 = (s64 << 8) | *p++;
		case 3:
			s64 = (s64 << 8) | *p++;
		case 2:
			s64 = (s64 << 8) | *p++;
		}
		val.Set(s64);
		break;

	case DField::Float:
		/* float value encoded as %A string */
		if (len < 3)
			return -EC_BAD_VALUE_LENGTH;
		if (bin[len - 1] != '\0')
			return -EC_BAD_FLOAT_VALUE;
		if (!strcmp(bin.ptr, "NAN"))
			val.flt = NAN;
		else if (!strcmp(bin.ptr, "INF"))
			val.flt = INFINITY;
		else if (!strcmp(bin.ptr, "-INF"))
			val.flt = -INFINITY;
		else {
			long double ldf;
			if (sscanf(bin.ptr, __FLTFMT__, &ldf) != 1)
				return -EC_BAD_FLOAT_VALUE;
			val.flt = ldf;
		}
		break;

	case DField::String:
		/* NULL encoded as zero length, others padded '\0' */
		if (len == 0) {
#if CONVERT_NULL_TO_EMPTY_STRING
			val.Set(bin.ptr - 1, 0);
#else
			val.Set(NULL, 0);
#endif
		} else {
			if (bin[len - 1] != '\0')
				return -EC_BAD_STRING_VALUE;
			val.Set(bin.ptr, len - 1);
		}
		break;

	case DField::Binary:
		/* NULL encoded as zero length, others padded '\0' */
		if (len == 0) {
#if CONVERT_NULL_TO_EMPTY_STRING
			val.Set(bin.ptr - 1, 0);
#else
			val.Set(NULL, 0);
#endif
		} else {
			if (bin[len - 1] != '\0')
				return -EC_BAD_STRING_VALUE;
			val.Set(bin.ptr, len - 1);
		}
		break;
	}
	bin += len;
	return 0;
}

/*
 * two form of field Id
 * <ID>			ID>0, by ID
 * 0 <LEN> <NAME>	byname, LEN is NAME length, no '\0'
 */
int decode_field_id(DTCBinary &bin, uint8_t &id, const DTCTableDefinition *tdef,
		    int &needDefinition)
{
	if (!bin)
		return -EC_BAD_SECTION_LENGTH;
	uint8_t n = *bin++;
	if (n) {
		id = n;
	} else {
		if (!bin)
			return -EC_BAD_SECTION_LENGTH;
		n = *bin++;
		if (n == 0) {
			id = n;
		} else {
			if (bin < n)
				return -EC_BAD_SECTION_LENGTH;
			int fid;
			//		    if(n <= 0 || (fid = tdef->field_id(bin.ptr, n)) <= 0){
			if (n <= 0 || (fid = tdef->field_id(bin.ptr, n)) <
					      0) { // allow select key-field
				log4cplus_debug("bad field name: %s", bin.ptr);
				return -EC_BAD_FIELD_NAME;
			}
			id = fid;
			bin += n;
			needDefinition = 1;
		}
	}
	return 0;
}

/*
 * get_tag format:
 * <ID> <LEN> <VALUE>
 * 	ID: 1 bytes
 * 	LEN: Length encoding
 * 	VALUE: DataValue encoding, predefined type
 * Simpel Section format:
 * 	<TAG> <TAG> ...
 */
int decode_simple_section(DTCBinary &bin, SimpleSection &ss, uint8_t kt)
{
	uint8_t mask[32];
	FIELD_ZERO(mask);
	while (!!bin) {
		int id = *bin++;

		if (FIELD_ISSET(id, mask))
			return -EC_DUPLICATE_TAG;
		int type = id == 0 ? kt : ss.tag_type(id);
		/* avoid one more copy of tag DTCValue */
		/* int(len, value):  buf -> local; buf -> local -> tag */
		/* str(len, value):  buf -> local -> tag; buf -> tag */
		int rv = decode_data_value(bin, *ss.get_this_tag(id), type);
		if (rv) {
			log4cplus_debug("decode tag[id:%d] error: %d", id, rv);
			return rv;
		}
		if (type != DField::None) {
			//ss.set_tag(id, val);
			/* no need of check if none type section and check if duplicate tag */
			ss.SetTagMask(id);
			FIELD_SET(id, mask);
		}
	}
	return 0;
}

int decode_simple_section(char *p, int l, SimpleSection &ss, uint8_t kt)
{
	DTCBinary bin = { l, p };
	return decode_simple_section(bin, ss, kt);
}
