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
#ifndef __CH_DECODE_H__
#define __CH_DECODE_H__

/*
 * Base Decode/encode routines:
 *	p = encode...(p, ...);	// encode object and advance pointer
 *	EncodedBytes...(...);	// calculate encoded object size
 *	Decode...(...);		// Decode objects
 */
#include "value.h"
#include "../table/../table/table_def.h"
#include "section.h"
#include "../field/field.h"
#include "../field/field_api.h"

#define __FLTFMT__ "%LA"
static inline char *encode_data_type(char *p, uint8_t type)
{
	*p++ = type == DField::Unsigned ? DField::Signed : type;
	return p;
}

extern int decode_length(DTCBinary &bin, uint32_t &len);
extern char *encode_length(char *p, uint32_t len);
extern int encoded_bytes_length(uint32_t n);

extern int decode_data_value(DTCBinary &bin, DTCValue &val, int type);
extern char *encode_data_value(char *p, const DTCValue *v, int type);
extern int encoded_bytes_data_value(const DTCValue *v, int type);

extern int decode_field_id(DTCBinary &, uint8_t &id,
			   const DTCTableDefinition *tdef, int &needDefinition);
extern int decode_simple_section(DTCBinary &, SimpleSection &, uint8_t);
extern int decode_simple_section(char *, int, SimpleSection &, uint8_t);

extern int encoded_bytes_simple_section(const SimpleSection &, uint8_t);
extern char *encode_simple_section(char *p, const SimpleSection &, uint8_t);
extern int encoded_bytes_field_set(const DTCFieldSet &);
extern char *encode_field_set(char *p, const DTCFieldSet &);
extern int encoded_bytes_field_value(const DTCFieldValue &);
extern char *encode_field_value(char *, const DTCFieldValue &);

extern int encoded_bytes_multi_key(const DTCValue *v,
				   const DTCTableDefinition *tdef);
extern char *encode_multi_key(char *, const DTCValue *v,
			      const DTCTableDefinition *tdef);
class FieldSetByName;
extern int encoded_bytes_field_set(const FieldSetByName &);
extern char *encode_field_set(char *p, const FieldSetByName &);
extern int encoded_bytes_field_value(const FieldValueByName &);
extern char *encode_field_value(char *, const FieldValueByName &);

#endif
