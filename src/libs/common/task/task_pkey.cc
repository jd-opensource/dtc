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

#include "value.h"
#include "../table/table_def.h"
#include "task_pkey.h"

void TaskPackedKey::unpack_key(const DTCTableDefinition *tdef,
			       const char *ptrKey, DTCValue *val)
{
	if (tdef->key_format() == 0) {
		val->bin.ptr = (char *)ptrKey + 1;
		val->bin.len = *(unsigned char *)ptrKey;
	} else {
		for (int i = 0; i < tdef->key_fields();
		     val++, ptrKey += tdef->field_size(i), i++) {
			const int size = tdef->field_size(i);
#if __BYTE_ORDER == __LITTLE_ENDIAN
			val->s64 = tdef->field_type(i) == DField::Unsigned ?
					   0 :
					   ptrKey[size - 1] & 0x80 ? -1 : 0;
			memcpy((char *)&val->s64, ptrKey, size);
#elif __BYTE_ORDER == __BIG_ENDIAN
			val->s64 = tdef->field_type(i) == DField::Unsigned ?
					   0 :
					   ptrKey[0] & 0x80 ? -1 : 0;
			memcpy((char *)&val->s64 + (sizeof(uint64_t) - size),
			       ptrKey, size);
#else
#error unkown endian
#endif
		}
	}
}

int TaskPackedKey::build_packed_key(const DTCTableDefinition *tdef,
				    const DTCValue *pstKeyValues,
				    unsigned int uiBufSize, char packKey[])
{
	if (tdef->key_format() == 0) { // single string key
		if (pstKeyValues->str.len > (int)tdef->field_size(0))
			return -1;
		if ((int)uiBufSize < pstKeyValues->str.len + 1)
			return -2;
		if (tdef->field_type(0) == DField::String) {
			packKey[0] = pstKeyValues->str.len;
			for (int i = 0; i < pstKeyValues->str.len; i++)
				packKey[i + 1] = INTERNAL_TO_LOWER(
					pstKeyValues->str.ptr[i]);
		} else {
			packKey[0] = pstKeyValues->str.len;
			memcpy(packKey + 1, pstKeyValues->str.ptr,
			       pstKeyValues->str.len);
		}
	} else if (tdef->key_fields() == 1) { // single int key
		if (uiBufSize < (unsigned int)(tdef->key_format()))
			return -3;

		if (store_value(packKey, pstKeyValues->u64, tdef->field_type(0),
				tdef->field_size(0)) < 0) {
			return -4;
		}
	} else { // multi-int key
		if ((int)uiBufSize < tdef->key_format())
			return -5;

		for (int i = 0; i < tdef->key_fields(); i++) {
			if (store_value(packKey + tdef->field_offset(i),
					(pstKeyValues + i)->u64,
					tdef->field_type(i),
					tdef->field_size(i)) < 0) {
				return -6;
			}
		}
	}

	return 0;
}

unsigned long
TaskPackedKey::calculate_barrier_key(const DTCTableDefinition *tdef,
				     const char *pkey)
{
	unsigned const char *p = (unsigned const char *)pkey;
	int len = tdef->key_format();
	if (len == 0)
		len = *p++;

	unsigned long h = 0;
	unsigned char c;
	while (len-- > 0) {
		c = *p++;
		h = h * 11111 + (c << 4) + (c >> 4);
	}
	return h;
}
