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
#ifndef __CH_FIELD_API_H__
#define __CH_FIELD_API_H__

#define INVALID_FIELD_ID 255
class FieldSetByName {
    private:
	uint16_t maxFields;
	uint8_t numFields;
	uint8_t solved;
	uint8_t maxvid;
	struct {
		uint8_t fid;
		uint8_t vid;
		uint8_t nlen;
		char *name;
	} * fieldValue;

	FieldSetByName(const FieldSetByName &); // NOT IMPLEMENTED
    public:
	FieldSetByName(void)
		: maxFields(0), numFields(0), solved(1), maxvid(0),
		  fieldValue(NULL)
	{
	}
	~FieldSetByName()
	{
		if (fieldValue) {
			for (int i = 0; i < numFields; i++)
				FREE(fieldValue[i].name);
			FREE(fieldValue);
		}
	}

	int Solved(void) const
	{
		return solved;
	}
	int Resolve(const DTCTableDefinition *, int);
	void Unresolve(void);

	int max_fields(void) const
	{
		return maxFields;
	}
	int num_fields(void) const
	{
		return numFields;
	}

	int field_present(const char *name) const
	{
		for (int i = 0; i < numFields; i++)
			if (strncasecmp(fieldValue[i].name, name, 256) == 0)
				return 1;
		return 0;
	}

	const char *field_name(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].name : NULL;
	}

	int field_name_length(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].nlen : 255;
	}

	int field_id(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].fid : 255;
	}

	int virtual_id(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].vid : 0;
	}

	int max_virtual_id(void) const
	{
		return maxvid;
	}

	int add_field(const char *name, int vid);
	int field_v_id(const char *name) const
	{
		for (int i = 0; i < numFields; i++)
			if (strncasecmp(fieldValue[i].name, name, 256) == 0)
				return fieldValue[i].vid;
		return -1;
	}

	const uint8_t *virtual_map(void) const;
};

extern int check_int_value(const DTCValue &v1, int type, int size);

class FieldValueByName {
    private:
	uint16_t maxFields;
	uint8_t numFields;
	uint8_t solved;

	struct {
		uint8_t type;
		uint8_t oper;
		uint8_t fid;
		uint8_t nlen;
		char *name;
		DTCValue val;
	} * fieldValue;

	FieldValueByName(const FieldValueByName &); // NOT IMPLEMENTED
    public:
	FieldValueByName(void)
		: maxFields(0), numFields(0), solved(1), fieldValue(NULL)
	{
	}

	~FieldValueByName()
	{
		if (fieldValue) {
			for (int i = 0; i < numFields; i++) {
				FREE_IF(fieldValue[i].name);
				if (fieldValue[i].type == DField::String ||
				    fieldValue[i].type == DField::Binary)
					FREE_IF(fieldValue[i].val.bin.ptr);
			}
			FREE(fieldValue);
		}
	}

	int Solved(void) const
	{
		return solved;
	}
	int Resolve(const DTCTableDefinition *, int);
	void Unresolve(void);

	int max_fields(void) const
	{
		return maxFields;
	}
	int num_fields(void) const
	{
		return numFields;
	}

	const char *field_name(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].name : NULL;
	}

	int field_name_length(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].nlen : 0;
	}

	int field_id(int n) const
	{
		return n >= 0 && n < numFields ? fieldValue[n].fid : 255;
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

	const DTCValue *field_value(int n) const
	{
		return n >= 0 && n < numFields ? &fieldValue[n].val : NULL;
	}

	int add_value(const char *n, uint8_t op, uint8_t t, const DTCValue &v);
};

#endif
