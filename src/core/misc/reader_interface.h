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

#ifndef __READER_INTERFACE_H
#define __READER_INTERFACE_H

#include "field/field.h"

class ReaderInterface {
    public:
	ReaderInterface()
	{
	}
	virtual ~ReaderInterface()
	{
	}

	virtual const char *err_msg() = 0;
	virtual int begin_read()
	{
		return 0;
	}
	virtual int read_row(RowValue &row) = 0;
	virtual int end() = 0;
};

#endif
