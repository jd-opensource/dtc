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
#include <stdio.h>
#include <vector>

#include "buffer.h"

class StateTableFormater {
    public:
	enum { FORMAT_ALIGNED,
	       FORMAT_TABBED,
	       FORMAT_COMMA,
	};
	StateTableFormater();
	~StateTableFormater();
	void new_row(void);
	void clear_row(void);
	void cell(const char *fmt, ...)
		__attribute__((__format__(printf, 2, 3)));
	void cell_v(const char *fmt, ...)
		__attribute__((__format__(printf, 2, 3)));
	void dump(FILE *, int fmt = FORMAT_ALIGNED);

    private:
	struct cell_t {
		unsigned int off;
		unsigned int len;
		cell_t() : off(0), len(0)
		{
		}
		~cell_t()
		{
		}
	};
	typedef std::vector<cell_t> row_;

	class buffer buf_;
	std::vector<row_> table_;
	std::vector<unsigned int> width_;
};
