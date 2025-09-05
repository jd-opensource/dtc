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
#include <unistd.h>
#include <stdlib.h>
#include "section.h"
#include "field/field.h"
#include "dtc_error_code.h"

#if MAX_STATIC_SECTION >= 1 && MAX_STATIC_SECTION < 18
#error MAX_STATIC_SECTION must >= 18
#endif
const SectionDefinition versionInfoDefinition = {
	DRequest::Section::VersionInfo,
	20,
	{
		// version info:
		DField::None,
		DField::String, // 1 -- table name
		DField::Binary, // 2 -- data table hash [for hb register use, unless always send 0x00000...]
		DField::Unsigned, // 3 -- serial#
		DField::Binary, // 4 -- real table hash
		DField::String, // 5 -- client version
		DField::String, // 6 -- ctlib version
		DField::String, // 7 -- helper version [unuse]
		DField::Unsigned, // 8 -- keepalive timeout
		DField::Unsigned, // 9 -- key type
		DField::Unsigned, // 10 -- key field count
		DField::Unsigned, // 11 -- key value count
		DField::Binary, // 12 -- key type list
		DField::Binary, // 13 -- key name list
		DField::Unsigned, // 14 -- hot backup ID
		DField::Signed, // 15 -- hot backup master timestamp
		DField::Signed, // 16 -- hot backup slave timestamp
		DField::Unsigned, // 17 -- agent client id
		DField::String, // 18 -- accessKey
		DField::Unsigned // 19 -- reconnet flag
	},
};

const SectionDefinition requestInfoDefinition = {
	DRequest::Section::VersionInfo,
	8,
	{
#if MAX_STATIC_SECTION >= 1 && MAX_STATIC_SECTION < 8
#error MAX_STATIC_SECTION must >= 8
#endif
		// request info:
		DField::Binary, // 0 -- key
		DField::Unsigned, // 1 -- timeout
		DField::Unsigned, // Limit start
		DField::Unsigned, // Limit count
		DField::String, // 4 -- trace msg
		DField::Unsigned, // 5 -- Cache ID -- OBSOLETED
		DField::String, // 6 -- raw config string
		DField::Unsigned, // 7 -- admin cmd code
	}
};

const SectionDefinition resultInfoDefinition = {
	DRequest::Section::ResultInfo,
	10,
	{
#if MAX_STATIC_SECTION >= 1 && MAX_STATIC_SECTION < 9
#error MAX_STATIC_SECTION must >= 9
#endif
		// result info:
		DField::Binary, // 0 -- key, original key or INSERT_ID
		DField::Signed, // 1 -- code
		DField::String, // 2 -- trace msg
		DField::String, // 3 -- from
		DField::Signed, // 4 -- affected rows, by delete/update
		DField::Unsigned, // 5 -- total rows, not used. for SELECT SQL_CALC_FOUND_ROWS...
		DField::Unsigned, // 6 -- insert_id, not used. for SELECT SQL_CALC_FOUND_ROWS...
		DField::Binary, // 7 -- query server info: version, binlogid,....
		DField::Unsigned, // 8 -- server timestamp
		DField::Unsigned, // 9 -- Hit Flag
	}
};
