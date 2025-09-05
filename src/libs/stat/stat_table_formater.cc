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
* 
*/
#include <stdarg.h>
#include "stat_table_formater.h"

StateTableFormater::StateTableFormater()
{
}

StateTableFormater::~StateTableFormater()
{
}

void StateTableFormater::new_row()
{
	table_.resize(table_.size() + 1);
}

void StateTableFormater::clear_row()
{
	table_.resize(table_.size() - 1);
}

void StateTableFormater::cell(const char *format, ...)
{
	cell_t cell;
	if (format && format[0]) {
		va_list ap;
		cell.off = buf_.size();
		va_start(ap, format);
		cell.len = buf_.vbprintf(format, ap);
		va_end(ap);
	}

	unsigned int c = table_.back().size();
	if (c >= width_.size())
		width_.push_back(cell.len);
	if (width_[c] < cell.len)
		width_[c] = cell.len;

	table_.back().push_back(cell);
}
void StateTableFormater::cell_v(const char *format, ...)
{
	cell_t cell;
	if (format && format[0]) {
		va_list ap;
		cell.off = buf_.size();
		va_start(ap, format);
		cell.len = buf_.vbprintf(format, ap);
		va_end(ap);
	}

	unsigned int c = table_.back().size();
	if (c >= width_.size())
		width_.push_back(0);

	table_.back().push_back(cell);
}

void StateTableFormater::dump(FILE *fp, int fmt)
{
	unsigned i, j;
	char del = '\t';

	switch (fmt) {
	case FORMAT_ALIGNED:
		for (i = 0; i < table_.size(); i++) {
			row_ &r = table_[i];
			for (j = 0; j < r.size(); j++) {
				if (width_[j] == 0)
					continue;
				cell_t &c = r[j];

				if (j != 0)
					fputc(' ', fp);
				if (c.len < width_[j])
					fprintf(fp, "%*s", width_[j] - c.len,
						"");
				fprintf(fp, "%.*s", (int)(c.len),
					buf_.c_str() + c.off);
			}
			fputc('\n', fp);
		}
		break;

	case FORMAT_TABBED:
		del = '\t';
		goto out;
	case FORMAT_COMMA:
		del = ',';
		goto out;
	out:
		for (i = 0; i < table_.size(); i++) {
			row_ &r = table_[i];
			for (j = 0; j < r.size(); j++) {
				cell_t &c = r[j];
				if (j != 0)
					fputc(del, fp);
				fprintf(fp, "%.*s", (int)(c.len),
					buf_.c_str() + c.off);
			}
			for (; j < width_.size(); j++)
				if (j != 0)
					fputc(del, fp);
			fputc('\n', fp);
		}
		break;
	}
}
