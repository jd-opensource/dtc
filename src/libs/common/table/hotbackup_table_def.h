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
#ifndef __ADMIN_TDEF_H__
#define __ADMIN_TDEF_H__

class DTCTableDefinition;
class DTCHotBackup {
    public:
	//type
	enum { SYNC_LRU = 1,
	       SYNC_INSERT = 2,
	       SYNC_UPDATE = 4,
	       SYNC_PURGE = 8,
	       SYNC_DELETE = 16,
	       SYNC_CLEAR = 32,
	       SYNC_COLEXPAND = 64,
	       SYNC_COLEXPAND_CMD = 128,
		   SYNC_NONE = 256 };

	//flag
	enum { NON_VALUE = 1,
	       HAS_VALUE = 2,
	       EMPTY_NODE = 4,
	       KEY_NOEXIST = 8,
	};
};

class DTCMigrate {
    public:
	//type
	enum { FROM_CLIENT = 1, FROM_SERVER = 2 };

	//flag
	enum { NON_VALUE = 1,
	       HAS_VALUE = 2,
	       EMPTY_NODE = 4,
	       KEY_NOEXIST = 8,
	};
};
extern DTCTableDefinition *build_hot_backup_table(void);

#define _DTC_HB_COL_EXPAND_ "_dtc_hb_col_expand_"
#define _DTC_HB_COL_EXPAND_DONE_ "_dtc_hb_col_expand_done_"

#endif
