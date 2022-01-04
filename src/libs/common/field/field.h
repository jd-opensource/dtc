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
#ifndef __CH_FIELD_H__
#define __CH_FIELD_H__

#include <string.h>
#include <errno.h>

#include "value.h"
#include "../table/table_def.h"
#include "protocol.h"
#include "mem_check.h"

class DTCFieldValue;
class FieldSetByName;
class FieldValueByName;

class DTCFieldSet {
    private:
	uint8_t *fieldId;
	uint8_t fieldMask[32];

    public:
	DTCFieldSet(const DTCFieldSet &fs)
	{
		int n = fs.fieldId[-2];
		fieldId = (uint8_t *)MALLOC(n + 2);
		if (fieldId == NULL)
			throw std::bad_alloc();
		memcpy(fieldId, fs.fieldId - 2, n + 2);
		memcpy(fieldMask, fs.fieldMask, sizeof(fieldMask));
		fieldId += 2;
	}
	DTCFieldSet(int n)
	{
		if (n > 255)
			n = 255;
		fieldId = (uint8_t *)MALLOC(n + 2);
		if (fieldId == NULL)
			throw std::bad_alloc();
		fieldId += 2;
		fieldId[-2] = n;
		memset(fieldId - 1, 0, n + 1);
		memset(fieldMask, 0, sizeof(fieldMask));
	}
	DTCFieldSet(const uint8_t *idtab, int n)
	{
		if (n > 255)
			n = 255;
		fieldId = (uint8_t *)MALLOC(n + 2);
		if (fieldId == NULL)
			throw std::bad_alloc();
		*fieldId++ = n;
		*fieldId++ = n;
		memcpy(fieldId, idtab, n);
		build_field_mask(fieldMask);
	}

	/* allocate max field cnt, real field num in first and second byte */
	DTCFieldSet(const uint8_t *idtab, int n, int total)
	{
		if (n > 255)
			n = 255;
		if (total > 255)
			total = 255;
		fieldId = (uint8_t *)MALLOC(total + 2);
		if (fieldId == NULL)
			throw std::bad_alloc();
		*fieldId++ = total;
		*fieldId++ = n;
		memcpy(fieldId, idtab, n);
		memset(fieldMask, 0, sizeof(fieldMask));
		build_field_mask(fieldMask);
	}

	inline void Clean()
	{
		fieldId[-1] = 0;
		memset(fieldMask, 0, sizeof(fieldMask));
	}

	/* clean before set */
	inline int Set(const uint8_t *idtab, int n)
	{
		if (n > 255)
			n = 255;
		if (fieldId == NULL)
			return -1;
		fieldId[-1] = n;
		memcpy(fieldId, idtab, n);
		build_field_mask(fieldMask);
		return 0;
	}

	inline int max_fields()
	{
		return fieldId[-2];
	}
	inline void Realloc(int total)
	{
		fieldId -= 2;
		fieldId = (uint8_t *)REALLOC(fieldId, total + 2);
		*fieldId = total;
		fieldId += 2;
	}

	int Copy(const FieldSetByName &);
	~DTCFieldSet()
	{
		if (fieldId)
			FREE(fieldId - 2);
	}

	int num_fields(void) const
	{
		return fieldId[-1];
	}
	int field_id(int n) const
	{
		return n >= 0 && n < num_fields() ? fieldId[n] : 0;
	}
	void add_field(int id)
	{
		const int n = fieldId[-1];
		if (n >= fieldId[-2])
			return;
		fieldId[n] = id;
		fieldId[-1]++;
		FIELD_SET(id, fieldMask);
	}
	int field_present(int id) const
	{
		if (id >= 0 && id < 255)
			return FIELD_ISSET(id, fieldMask);
		return 0;
	}
	void build_field_mask(uint8_t *mask) const
	{
		for (int i = 0; i < num_fields(); i++)
			FIELD_SET(fieldId[i], mask);
	}
};

class RowValue : public TableReference {
    private:
	DTCValue *value;

    public:
	RowValue(DTCTableDefinition *t) : TableReference(t)
	{
		value = (DTCValue *)calloc(num_fields() + 1, sizeof(DTCValue));
		if (value == NULL)
			throw std::bad_alloc();
	};

	RowValue(const RowValue &r) : TableReference(r.table_definition())
	{
		value = (DTCValue *)calloc(num_fields() + 1, sizeof(DTCValue));
		if (value == NULL)
			throw std::bad_alloc();
		memcpy(value, r.value, sizeof(DTCValue) * (num_fields() + 1));
	}

	virtual ~RowValue()
	{
		FREE_IF(value);
	};

	inline void Clean()
	{
		memset(value, 0, sizeof(DTCValue) * (num_fields() + 1));
	}

#if !CLIENTAPI
	// this macro test is scope-test only, didn't affected the class implementation
	void default_value(void)
	{
		get_default_row(value);
	}
#endif
	int is_same_table(const RowValue &rv) const
	{
		return is_same_table(rv.table_definition());
	}
	int is_same_table(const RowValue *rv) const
	{
		return rv ? is_same_table(rv->table_definition()) : 0;
	}

	/*Compare tow RowValue by FieldIDList*/
	int Compare(const RowValue &rv, uint8_t *fieldIDList, uint8_t num);
	DTCValue &operator[](int n)
	{
		return value[n];
	}
	const DTCValue &operator[](int n) const
	{
		return value[n];
	}
	DTCValue *field_value(int id)
	{
		return id >= 0 && id <= num_fields() ? &value[id] : NULL;
	}
	const DTCValue *field_value(int id) const
	{
		return id >= 0 && id <= num_fields() ? &value[id] : NULL;
	}
	DTCValue *field_value(const char *name)
	{
		return field_value(field_id(name));
	}
	const DTCValue *field_value(const char *name) const
	{
		return field_value(field_id(name));
	}
	void copy_value(const DTCValue *v, int id, int n)
	{
		memcpy(&value[id], v, n * sizeof(DTCValue));
	}
	int Copy(const RowValue *r);
	void update_timestamp(
		uint32_t now,
		int updateall /*update all timestamp, include lastcmod*/);
	void update_expire_time();
};

class DTCFieldValue {
    private:
	struct SFieldValue {
		uint8_t id;
		uint8_t oper;
		uint8_t type;
		DTCValue val;
	} * fieldValue;

	//total
	int maxFields;
	//real
	int numFields;
	FieldDefinition::fieldflag_t typeMask[2];

    public:
	DTCFieldValue(int total)
	{
		fieldValue = NULL;
		maxFields = numFields = 0;
		if (total <= 0)
			return;
#if ROCKSDB_COMPILER
		fieldValue = (decltype(fieldValue))MALLOC(total *
							  sizeof(*fieldValue));
#else
		fieldValue =
			(typeof(fieldValue))MALLOC(total * sizeof(*fieldValue));
#endif
		if (fieldValue == NULL)
			throw(-ENOMEM);
		maxFields = total;
		typeMask[0] = 0;
		typeMask[1] = 0;
	}
	DTCFieldValue(const DTCFieldValue &fv, int sparse = 0)
	{
		numFields = fv.numFields;
		typeMask[0] = fv.typeMask[0];
		typeMask[1] = fv.typeMask[1];
		if (sparse < 0)
			sparse = 0;
		sparse += fv.numFields;
		maxFields = sparse;
		if (fv.fieldValue != NULL) {
#if ROCKSDB_COMPILER
			fieldValue = (decltype(fieldValue))MALLOC(
				sparse * sizeof(*fieldValue));
#else
			fieldValue = (typeof(fieldValue))MALLOC(
				sparse * sizeof(*fieldValue));
#endif
			if (fieldValue == NULL)
				throw(-ENOMEM);
			memcpy(fieldValue, fv.fieldValue,
			       fv.numFields * sizeof(*fieldValue));
		} else {
			fieldValue = NULL;
		}
	}
	int Copy(const FieldValueByName &, int mode,
		 const DTCTableDefinition *);
	~DTCFieldValue()
	{
		FREE_IF(fieldValue);
	}

	/* should be inited as just constructed */
	inline void Clean()
	{
		numFields = 0;
		memset(typeMask, 0, 2 * sizeof(FieldDefinition::fieldflag_t));
	}

	inline void Realloc(int total)
	{
		maxFields = total;
		fieldValue = (struct SFieldValue *)REALLOC(
			fieldValue, sizeof(struct SFieldValue) * total);
	}

	int max_fields(void) const
	{
		return maxFields;
	}
	int num_fields(void) const
	{
		return numFields;
	}

	int field_id(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].id : 0;
	}

	int field_type(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].type :
						 DField::None;
	}

	int field_operation(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].oper : 0;
	}

	DTCValue *field_value(int n) const
	{
		return n >= 0 && n < numFields ? &fieldValue[n].val : NULL;
	}

	void add_value(uint8_t id, uint8_t op, uint8_t t, const DTCValue &val)
	{
		if (numFields == maxFields)
			return;
		fieldValue[numFields].id = id;
		fieldValue[numFields].oper = op;
		fieldValue[numFields].type = t;
		fieldValue[numFields].val = val;
		numFields++;
	}
	DTCValue *next_field_value()
	{
		if (numFields == maxFields)
			return NULL;
		return &fieldValue[numFields].val;
	}
	void add_value_no_val(uint8_t id, uint8_t op, uint8_t t)
	{
		if (numFields == maxFields)
			return;
		fieldValue[numFields].id = id;
		fieldValue[numFields].oper = op;
		fieldValue[numFields].type = t;
		numFields++;
	}
	void update_type_mask(unsigned int flag)
	{
		typeMask[0] |= flag;
		typeMask[1] |= ~flag;
	}
	int has_type_ro(void) const
	{
		return typeMask[0] & FieldDefinition::FF_READONLY;
	}
	int has_type_rw(void) const
	{
		return typeMask[1] & FieldDefinition::FF_READONLY;
	}
	int has_type_sync(void) const
	{
		return 1;
	}
	int has_type_async(void) const
	{
		return 0;
	}
	int has_type_volatile(void) const
	{
		return typeMask[0] & FieldDefinition::FF_VOLATILE;
	}
	int has_type_commit(void) const
	{
		return typeMask[1] & FieldDefinition::FF_VOLATILE;
	}
	int has_type_timestamp(void) const
	{
		return typeMask[0] & FieldDefinition::FF_TIMESTAMP;
	}

	void build_field_mask(uint8_t *mask) const
	{
		for (int i = 0; i < num_fields(); i++)
			FIELD_SET(fieldValue[i].id, mask);
	}

	int Update(RowValue &);
	int Compare(const RowValue &, int iCmpFirstNRows = 256);
};

#endif
