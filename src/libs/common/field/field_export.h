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
#ifndef __CH_FIELD_H__
#define __CH_FIELD_H__

/*

 */
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <new>

#include "value.h"
#include "protocol.h"

class DTCTableDefinition;
class DTCFieldValue;

class DTCFieldSet {
    private:
	uint8_t *fieldId;
	uint8_t fieldMask[32];

    public:
	DTCFieldSet(const DTCFieldSet &fs)
	{
		int n = fs.fieldId[-2];
		fieldId = (uint8_t *)malloc(n + 2);
		if (fieldId == NULL)
			throw std::bad_alloc();
		memcpy(fieldId, fs.fieldId - 2, n + 2);
		fieldId += 2;
	}
	DTCFieldSet(int n)
	{
		if (n > 255)
			n = 255;
		fieldId = (uint8_t *)malloc(n + 2);
		if (fieldId == NULL)
			throw std::bad_alloc();
		fieldId += 2;
		fieldId[-2] = n;
		memset(fieldId - 1, 0, n + 1);
		memset(fieldMask, 0, sizeof(fieldMask));
	}
	DTCFieldSet(uint8_t *idtab, int n)
	{
		if (n > 255)
			n = 255;
		fieldId = (uint8_t *)malloc(n + 2);
		if (fieldId == NULL)
			throw std::bad_alloc();
		*fieldId++ = n;
		*fieldId++ = n;
		memcpy(fieldId, idtab, n);
		build_field_mask(fieldMask);
	}
	~DTCFieldSet()
	{
		if (fieldId)
			free(fieldId - 2);
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
	void build_field_mask(uint8_t *mask) const;
};

class TableReference {
    public:
	TableReference(void *t) : ref(t)
	{
	}
	void *table_definition(void) const
	{
		return ref;
	}
	void *ref;
	int num_fields() const
	{
		return 255;
	}
};

class RowValue : public TableReference {
    private:
	DTCValue *value;

    public:
	RowValue(DTCTableDefinition *t) : TableReference(t)
	{
		assert(table_definition() != NULL);
		assert(num_fields() > 0);
		value = (DTCValue *)calloc(num_fields() + 1, sizeof(DTCValue));
		if (value == NULL)
			throw std::bad_alloc();
	};

	RowValue(const RowValue &r) : TableReference(r.table_definition())
	{
		value = (DTCValue *)calloc(num_fields() + 1, sizeof(DTCValue));
		if (value == NULL)
			throw std::bad_alloc();
		memcpy(value, r.value, sizeof(DTCValue) * num_fields() + 1);
	}

	~RowValue()
	{
		if (value)
			free(value);
	};

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
	void copy_value(const DTCValue *v, int id, int n)
	{
		memcpy(&value[id], v, n * sizeof(DTCValue));
	}
};

class DTCFieldValue {
    private:
	struct {
		uint8_t id;
		uint8_t oper;
		uint8_t type;
		DTCValue val;
	} * fieldValue;

	int maxFields;
	int numFields;
	int typeMask;

    public:
	DTCFieldValue(int total)
	{
		fieldValue = NULL;
		maxFields = numFields = 0;
		if (total <= 0)
			return;
		fieldValue =
			(typeof(fieldValue))malloc(total * sizeof(*fieldValue));
		if (fieldValue == NULL)
			throw(-ENOMEM);
		maxFields = total;
		typeMask = 0;
	}
	DTCFieldValue(const DTCFieldValue &fv, int sparse = 0)
	{
		numFields = fv.numFields;
		typeMask = fv.typeMask;
		if (sparse < 0)
			sparse = 0;
		sparse += fv.numFields;
		maxFields = sparse;
		if (fv.fieldValue != NULL) {
			fieldValue = (typeof(fieldValue))malloc(
				sparse * sizeof(*fieldValue));
			if (fieldValue == NULL)
				throw(-ENOMEM);
			memcpy(fieldValue, fv.fieldValue,
			       fv.numFields * sizeof(*fieldValue));
		} else {
			fieldValue = NULL;
		}
	}
	~DTCFieldValue()
	{
		if (fieldValue)
			free(fieldValue);
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
	void update_type_mask_ro(int ro)
	{
		typeMask |= (ro ? 1 : 2);
	};
	void update_type_mask_sync(int sy)
	{
		typeMask |= (sy ? 4 : 8);
	};
	int has_type_ro(void) const
	{
		return typeMask & 1;
	}
	int has_type_rw(void) const
	{
		return typeMask & 2;
	}
	int has_type_sync(void) const
	{
		return typeMask & 4;
	}
	int has_type_async(void) const
	{
		return typeMask & 8;
	}

	void build_field_mask(uint8_t *mask) const;
	int Update(RowValue &);
	int Compare(const RowValue &, int iCmpFirstNRows = 256);
};

#endif
