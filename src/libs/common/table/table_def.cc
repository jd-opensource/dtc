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
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <endian.h>
#include <byteswap.h>

// skip scope test here, we need full decleration
#undef CLIENTAPI
#include "../table/table_def.h"
#include "algorithm/md5.h"
#include "../decode/decode.h"
#include "dtc_error_code.h"
#include "mem_check.h"

#define HASHSTOP 0xFFFF

#if MAX_STATIC_SECTION >= 1 && MAX_STATIC_SECTION < 4
#error MAX_STATIC_SECTION must >= 4
#endif
const SectionDefinition tableAttributeDefinition = {
	DRequest::Section::table_definition,
	4,
	{
		DField::None,
		DField::Unsigned, // flags
		DField::Unsigned, // masks
		DField::Unsigned, // key field count
	}
};

DTCTableDefinition::DTCTableDefinition(int m)
	: maxFields(m), usedFields(0), numFields(0), keyFields(0)
{
	// memset 0xFF, == all value 0xFFFF (HASHSTOP)
	memset(nameHash, 0xFF, sizeof(nameHash));
	tableName.Set(NULL, 0);
	// hash didn't, it will be inited by build_info_cache(server) or Unpack(client)
	// max_key_size always calculated by set_key_fields(server), unused in client side

	if (maxFields > 0) {
		// server side code
		// used in early initialize, assume always has enough memory
		fieldList = (FieldDefinition *)calloc(maxFields,
						      sizeof(FieldDefinition));
		defaultValue = (DTCValue *)calloc(maxFields, sizeof(DTCValue));
		uniqFields = (uint8_t *)calloc(maxFields, sizeof(uint8_t));
		rawFields = (uint8_t *)calloc(maxFields, sizeof(uint8_t));
		packedTableDefinition.Set(NULL, 0);
		packedFullFieldSet.Set(NULL, 0);
		packedFieldSet.Set(NULL, 0);
		uniqFieldCnt = 0;
		keysAsUniqField = 0;
		hasAutoInc = -1;
		hasLastacc = -1;
		hasLastmod = -1;
		hasLastcmod = -1;
		hasCompress = -1;
		hasExpireTime = -1;
		keyFormat = 0;
		m_row_size = 0;
		hasDiscard = 0;
		indexFields = 0; // by TREE_DATA
		maxKeySize = 0;
	} else {
		// client side code
		fieldList = NULL;
		// default Value used as flag determine client/server side object
		defaultValue = NULL;
		// don't init following members:
		//	 packedTableDefinition
		//	 packedFullFieldSet
		//	 packedFieldSet
		//	 uniqFields
		//	 uniqFieldCnt
		//	 keysAsUniqField
		//	 hasAutoInc
		//	 keyFormat
		//	 m_row_size
		//	 indexFields
		// because client side don't use it, and save a lot of CPU cycle
	}
}

DTCTableDefinition::~DTCTableDefinition(void)
{
	FREE_IF(tableName.ptr);
	if (fieldList != NULL) {
		for (int i = 0; i <= numFields; i++) {
			FREE_IF(fieldList[i].fieldName);
		}
		FREE_IF(fieldList);
	}
	if (defaultValue != NULL) {
		// client side didn't init this members
		FREE_IF(packedTableDefinition.ptr);
		FREE_IF(packedFullFieldSet.ptr);
		FREE_IF(packedFieldSet.ptr);
		FREE_IF(uniqFields);
		FREE_IF(rawFields);
		FREE(defaultValue);
	}
}

// used for bitmap svr
int DTCTableDefinition::add_field(int id, const char *name, uint8_t type,
				  int size, int bsize, int boffset)
{
	// permit duplicate add to overwrite the old one
	if (id < 0 || id > MAXFIELDS_PER_TABLE || id >= maxFields)
		return -EC_BAD_FIELD_ID;
	if (size <= 0)
		return -EC_BAD_FIELD_SIZE;
	switch (type) {
	case DField::Signed:
	case DField::Unsigned:
		if (size > 8)
			return -EC_BAD_FIELD_SIZE;
		if (defaultValue != NULL)
			defaultValue[id].Set(0);
		break;
	case DField::Float:
		if (size != 8 && size != 4)
			return -EC_BAD_FIELD_SIZE;
		if (defaultValue != NULL)
			defaultValue[id].Set(0.0);
		break;
	case DField::String:
	case DField::Binary:
		if (size > MAXPACKETSIZE)
			return -EC_BAD_FIELD_SIZE;
		if (defaultValue != NULL)
			defaultValue[id].Set(NULL, 0);
		break;
	default:
		return -EC_BAD_FIELD_TYPE;
	}
	int n = strlen(name);
	if (n == 0 || n > 255)
		return -EC_BAD_FIELD_NAME;

	char *lname = STRDUP(name);
	if (lname == NULL)
		throw std::bad_alloc();

	unsigned h = 0;
	for (int i = 0; i < n; i++)
		h = h * 11 + (name[i] << 4) + (name[i] >> 4);
	h &= 0xFF;

	fieldList[id].fieldName = lname;
	fieldList[id].field_type = type;
	fieldList[id].fieldSize = size; //Bytes
	fieldList[id].bsize = bsize; //bits
	fieldList[id].boffset = boffset;
	fieldList[id].nameLen = n;
	fieldList[id].flags = id == 0 ? FieldDefinition::FF_READONLY : 0;
	fieldList[id].next = nameHash[h];
	nameHash[h] = id;
	if (id > numFields)
		numFields = id;
	if (id)
		usedFields++;
	return 0;
}

int DTCTableDefinition::add_field(int id, const char *name, uint8_t type,
				  int size)
{
	if (id < 0 || id > MAXFIELDS_PER_TABLE || id >= maxFields ||
	    fieldList[id].fieldName)
		return -EC_BAD_FIELD_ID;
	if (size <= 0)
		return -EC_BAD_FIELD_SIZE;
	switch (type) {
	case DField::Signed:
	case DField::Unsigned:
		if (size > 8)
			return -EC_BAD_FIELD_SIZE;
		if (defaultValue != NULL)
			defaultValue[id].Set(0);
		break;
	case DField::Float:
		if (size != 8 && size != 4)
			return -EC_BAD_FIELD_SIZE;
		if (defaultValue != NULL)
			defaultValue[id].Set(0.0);
		break;
	case DField::String:
	case DField::Binary:
		if (size > MAXPACKETSIZE)
			return -EC_BAD_FIELD_SIZE;
		if (defaultValue != NULL)
			defaultValue[id].Set(NULL, 0);
		break;
	default:
		return -EC_BAD_FIELD_TYPE;
	}
	int n = strlen(name);
	if (n == 0 || n > 255)
		return -EC_BAD_FIELD_NAME;

	char *lname = STRDUP(name);
	if (lname == NULL)
		throw std::bad_alloc();

	unsigned h = 0;
	for (int i = 0; i < n; i++)
		h = h * 11 + (name[i] << 4) + (name[i] >> 4);
	h &= 0xFF;

	fieldList[id].fieldName = lname;
	fieldList[id].field_type = type;
	fieldList[id].fieldSize = size;
	fieldList[id].nameLen = n;
	fieldList[id].flags = id == 0 ? FieldDefinition::FF_READONLY : 0;
	fieldList[id].next = nameHash[h];
	nameHash[h] = id;
	if (id > numFields)
		numFields = id;
	if (id)
		usedFields++;
	if (id != 0) {
		rawFields[id] = id;
	}
	return 0;
}

void DTCTableDefinition::set_default_value(int id, const DTCValue *dval)
{
	if (id <= 0 || id > 255 || id >= maxFields || defaultValue == NULL)
		return;
	defaultValue[id] = *dval;
	/* string value cross reference to dbconfig  */
}

void DTCTableDefinition::get_default_row(DTCValue *row) const
{
	if (defaultValue)
		memcpy(row + 1, defaultValue + 1, sizeof(DTCValue) * numFields);
	else
		memset(row + 1, 0, sizeof(DTCValue) * numFields);
}

int DTCTableDefinition::field_id(const char *name, int len) const
{
	if (len == 0)
		len = strlen(name);
	unsigned h = 0;
	for (int i = 0; i < len; i++)
		h = h * 11 + (name[i] << 4) + (name[i] >> 4);
	h &= 0xFF;

	for (uint16_t id = nameHash[h]; id != HASHSTOP;
	     id = fieldList[id].next) {
		if (fieldList[id].nameLen != len)
			continue;
		if (memcmp(fieldList[id].fieldName, name, len) == 0)
			return id;
	}
	return -1;
}

int DTCTableDefinition::set_table_name(const char *name)
{
	char *p = STRDUP(name);
	if (p == NULL)
		return -ENOMEM;
	tableName.Set(p);
	return 0;
}

int DTCTableDefinition::set_key_fields(int n)
{
	if (n < 0)
		n = 1;
	if (n <= 0)
		return 0;
	if (n > 32)
		return -1;
	if (n > numFields + 1)
		n = numFields + 1;
	keyFields = n;
	attr.set_key_field_count(n);
	log4cplus_info("key count:%d" , keyFields);
	int j;
	keysAsUniqField = 2; /* SUBSET */
	int i, maxSize = 0, nvar = 0;
	for (i = 0; i < keyFields; i++) {
		mark_as_read_only(i); // key is readonly
		fieldList[i].offset = maxSize;
		switch (field_type(i)) {
		case DField::Signed:
		case DField::Unsigned: //integer
		case DField::Float: //float
			maxSize += field_size(i);
			break;
		case DField::String: //null-terminated string
		case DField::Binary: //binary data
		default:
			nvar++;
			maxSize = 1 + field_size(i); // 1 byte used to store string length
			break;
		}
		if (keysAsUniqField) {
			for (j = 0; j < uniqFieldCnt; j++)
				if (uniqFields[j] == i)
					break;
			if (j >= uniqFieldCnt)
				keysAsUniqField = 0; /* NOT */
		}
	}
	if (uniqFieldCnt == n && keysAsUniqField > 0)
		keysAsUniqField = 1; /* EXACT */
	if (nvar == 0) { // not string , binary
		if (maxKeySize >= 256) 
			return -1;
		keyFormat = maxSize; // total key size
		maxKeySize = maxSize;
	} else {  // key has string , binary type field
		if (maxKeySize >= 256 + 1) 
			return -1;
		if (keyFields != 1)
			return -1;
		keyFormat = 0;
		maxKeySize = maxSize + (nvar > 1);
	}
	return 0;
}

/*
 * table definition packing:
 * 	<HASH>		16 bytes, MD5 of remain data
 * 	<VER>		1 byte, ==1
 * 	<NFIELD>	1 byte, total field# exclude key
 * 	<NAMELEN>	1 byte, length of table name
 * 	<TABLENAME>	<NAMELEN> bytes, table name
 * 	<FIELD> list
 * 	  <FIELDID>	1 byte, field ID
 * 	  <FIELDTYPE>   1 byte, field type
 * 	  <FIELDSIZE>   1 byte, field size
 * 	  <FNAMELEN>    1 byte, length of field name
 * 	  <FNAME>       <FNAMELEN> bytes, field name
 * 	<ATTR>		table attribute, Section encoding format
 */
int DTCTableDefinition::build_info_cache(void)
{
	if (numFields <= 0)
		return 0;

	packedFullFieldSet.ptr = (char *)MALLOC(1 + usedFields + 2);
	char *p = packedFullFieldSet.ptr + 1;
	*p++ = 0; // field-id 0 encode: 00
	*p++ = 0;
	for (int i = 1; i <= numFields; i++) {
		if (fieldList[i].fieldName == NULL)
			continue;
		*p++ = i;
	}
	packedFullFieldSet.len = p - packedFullFieldSet.ptr;
	packedFullFieldSet.ptr[0] =
		packedFullFieldSet.len - 1 - 1; // real num fields

	packedFieldSet.ptr = (char *)MALLOC(1 + usedFields);
	p = packedFieldSet.ptr + 1;
	for (int i = 1; i <= numFields; i++) {
		if (fieldList[i].fieldName == NULL)
			continue;
		*p++ = i;
	}
	packedFieldSet.len = p - packedFieldSet.ptr;
	packedFieldSet.ptr[0] = packedFieldSet.len - 1; // real num fields

	int n = 16 /*hash*/ + 1 /*ver*/ + 1 + /*nf*/ +1 /*nl*/ + tableName.len;
	int nf = 0;
	for (int i = 0; i <= numFields; i++) {
		if (fieldList[i].fieldName == NULL)
			continue;
		nf++;
		n += 1 /*id*/ + 1 /*type*/ + 5 /*size*/ + 1 /*nl*/ +
		     fieldList[i].nameLen;
	}
	n += encoded_bytes_simple_section(attr, DField::None);

	packedTableDefinition.ptr = (char *)MALLOC(n);
	p = packedTableDefinition.ptr + 16;
	*p++ = 1; /*ver*/

	*p++ = tableName.len;
	memcpy(p, tableName.ptr, tableName.len);
	p += tableName.len;

	*p++ = nf;
	for (int i = 0; i <= numFields; i++) {
		if (fieldList[i].fieldName == NULL)
			continue;
		*p++ = i;
		*p++ = fieldList[i].field_type;
		p = encode_length(p, fieldList[i].fieldSize);
		*p++ = fieldList[i].nameLen;
		memcpy(p, fieldList[i].fieldName, fieldList[i].nameLen);
		p += fieldList[i].nameLen;
	}

	p = encode_simple_section(p, attr, DField::None);

	packedTableDefinition.len = p - packedTableDefinition.ptr;

	MD5Context md5;
	MD5Init(&md5);
	MD5Update(&md5, (unsigned char *)packedTableDefinition.ptr + 16,
		  packedTableDefinition.len - 16);
	MD5Final((unsigned char *)packedTableDefinition.ptr, &md5);

	memcpy(hash, packedTableDefinition.ptr, 16);

	return 0;
}

int DTCTableDefinition::Unpack(const char *ptr, int len)
{
	if (numFields != 0 || maxFields != 0)
		return -EC_TABLE_REDEFINED;
	DTCBinary b = { len, (char *)ptr };

	if (b < 24)
		return -EC_BAD_SECTION_LENGTH;

	{ // verify MD5
		MD5Context md5;
		unsigned char h[16];
		MD5Init(&md5);
		MD5Update(&md5, (unsigned char *)ptr + 16, len - 16);
		MD5Final(h, &md5);
		if (memcmp(h, ptr, 16) != 0)
			return -EC_VERSION_MISMATCH; //EC_HASH_MISMATCH;
		memcpy(hash, ptr, 16);
	}

	b += 16;
	uint8_t n;
	n = *b++;
	if (n != 1)
		return -EC_VERSION_MISMATCH;
	n = *b++;

	if (n == 0 || b < n + 1)
		return -EC_BAD_SECTION_LENGTH;
	tableName.ptr = (char *)MALLOC(n + 1);
	if (tableName.ptr == NULL)
		throw std::bad_alloc();
	memcpy(tableName.ptr, b.ptr, n);
	tableName.ptr[n] = '\0';
	tableName.len = n;
	b += n;

	int nf = *b++;
	if (nf == 0)
		return -EC_BAD_SECTION_LENGTH;

	maxFields = nf;
	fieldList = maxFields == 0 ?
			    NULL :
			    (FieldDefinition *)calloc(maxFields,
						      sizeof(FieldDefinition));
	// don't allocate defaultValue and uniqFields buffer, it's useless at client side
	for (int i = 0; i < nf; i++) {
		if (b < 5)
			return -EC_BAD_SECTION_LENGTH;
		uint8_t id = *b++;
		uint8_t type = *b++;
		uint32_t size;
		int rv;
		char name[256];
		rv = decode_length(b, size);
		if (rv)
			return rv;
		if (b < 2)
			return -EC_BAD_SECTION_LENGTH;
		n = *b++;
		if (n == 0 || b < n)
			return -EC_BAD_SECTION_LENGTH;
		memcpy(name, b.ptr, n);
		b += n;
		name[n] = '\0';
		rv = add_field(id, name, type, size);
		if (rv)
			return rv;
	}

	int rv = decode_simple_section(b, attr, DField::None);
	if (rv)
		return rv;

	return 0;
}
