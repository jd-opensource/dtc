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
#ifndef __CH_RESUL_H__
#define __CH_RESUL_H__
#include "field/field.h"

class ResultSet : public DTCFieldSet {
    private:
	int err;
	DTCBinary init;
	DTCBinary curr;
	int numRows;
	int rowno;
	RowValue row;

    public:
	/*fieldset at largest size*/
	ResultSet(const uint8_t *idtab, int num, int total,
		  DTCTableDefinition *t)
		: DTCFieldSet(idtab, num, total), err(0),
		  init(DTCBinary::Make(NULL, 0)),
		  curr(DTCBinary::Make(NULL, 0)), numRows(0), rowno(0), row(t)
	{
	}

	ResultSet(const DTCFieldSet &f, DTCTableDefinition *t)
		: DTCFieldSet(f), err(0), init(DTCBinary::Make(NULL, 0)),
		  curr(DTCBinary::Make(NULL, 0)), numRows(0), rowno(0), row(t)
	{
	}

	~ResultSet(void)
	{
	}

	inline void Clean()
	{
		DTCFieldSet::Clean();
		err = 0;
		init = DTCBinary::Make(NULL, 0);
		curr = DTCBinary::Make(NULL, 0);
		numRows = 0;
		rowno = 0;
		row.Clean();
	}

	inline int field_set_max_fields()
	{
		return DTCFieldSet::max_fields();
	}

	inline void realloc_field_set(int total)
	{
		DTCFieldSet::Realloc(total);
	}

	/* clean before set*/
	inline void Set(const uint8_t *idtab, int num)
	{
		DTCFieldSet::Set(idtab, num);
		//row.set_table_definition(t);
	}

	inline void set_value_data(int nr, DTCBinary b)
	{
		rowno = 0;
		numRows = nr;
		init = b;
		curr = b;
	}

	int total_rows(void) const
	{
		return numRows;
	}

	const DTCValue &operator[](int id) const
	{
		return row[id];
	}
	const DTCValue *field_value(int id) const
	{
		return row.field_value(id);
	}
	const DTCValue *field_value(const char *id) const
	{
		return row.field_value(id);
	}

	int decode_row(void);

	const RowValue *row_value(void) const
	{
		if (err)
			return NULL;
		return &row;
	}

	const RowValue *fetch_row(void)
	{
		if (decode_row())
			return NULL;
		return &row;
	}

	RowValue *_fetch_row(void)
	{
		if (decode_row())
			return NULL;
		return &row;
	}

	int error_num(void) const
	{
		return err;
	}

	int rewind(void)
	{
		curr = init;
		err = 0;
		rowno = 0;
		return 0;
	}

	int data_len(void) const
	{
		return init.len;
	}
	char* data(void) const
	{
		return init.ptr;
	}
};

class ResultWriter {
	ResultWriter(const ResultWriter &); // NOT IMPLEMENTED
    public:
	const DTCFieldSet *fieldSet;
	unsigned int limitStart, limitNext;
	unsigned int totalRows;
	unsigned int numRows;

	ResultWriter(const DTCFieldSet *, unsigned int, unsigned int);
	virtual ~ResultWriter(void)
	{
	}

	inline virtual void Clean()
	{
		totalRows = 0;
		numRows = 0;
	}
	virtual void detach_result() = 0;
	inline virtual int Set(const DTCFieldSet *fs, unsigned int st,
			       unsigned int cnt)
	{
		if (cnt == 0)
			st = 0;
		limitStart = st;
		limitNext = st + cnt;
		const int nf = fs == NULL ? 0 : fs->num_fields();
		fieldSet = nf ? fs : NULL;

		return 0;
	}
	virtual int append_row(const RowValue &) = 0;
	virtual int merge_no_limit(const ResultWriter *rp) = 0;

	int set_rows(unsigned int rows);
	void set_total_rows(unsigned int totalrows)
	{
		totalRows = totalrows;
	}
	void add_total_rows(int n)
	{
		totalRows += n;
	}
	int is_full(void) const
	{
		return limitNext > 0 && totalRows >= limitNext;
	}
	int in_range(unsigned int total, unsigned int begin = 0) const
	{
		if (limitNext == 0)
			return (1);
		if (total <= limitStart || begin >= limitNext)
			return (0);
		return (1);
	}
};

class ResultPacket : public ResultWriter {
	ResultPacket(const ResultPacket &); // NOT IMPLEMENTED
    public:
	/* acctually just one buff */
	BufferChain *bc;
	unsigned int rowDataBegin;

	ResultPacket(const DTCFieldSet *, unsigned int, unsigned int);
	virtual ~ResultPacket(void);

	void Clean();
	inline virtual void detach_result()
	{
		bc = NULL;
	}
	virtual int Set(const DTCFieldSet *fieldList, unsigned int st,
			unsigned int ct);
	virtual int append_row(const RowValue &);
	virtual int merge_no_limit(const ResultWriter *rp);
};

class ResultBuffer : public ResultWriter {
	ResultBuffer(const ResultBuffer &); // NOT IMPLEMENTED
    public:
	ResultBuffer(const DTCFieldSet *, unsigned int, unsigned int);
	virtual ~ResultBuffer(void);

	virtual int append_row(const RowValue &);
	virtual int merge_no_limit(const ResultWriter *rp);
};

#endif
