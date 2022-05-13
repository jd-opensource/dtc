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
#ifndef __CH_PROTOCOL_H_
#define __CH_PROTOCOL_H_
#include <stdint.h>

#define MAXFIELDS_PER_TABLE 255
#define MAXPACKETSIZE (64 << 20)

class DField {
    public:
	enum { None = 0, // undefined
	       Signed = 1, // Signed Integer
	       Unsigned = 2, // Unsigned Integer
	       Float = 3, // FloatPoint
	       String = 4, // String, case insensitive, null ended
	       Binary = 5, // opaque binary data
	       TotalType };

	enum { Set = 0, Add = 1, SetBits = 2, OR = 3, TotalOperation };

	enum { EQ = 0,
	       NE = 1,
	       LT = 2,
	       LE = 3,
	       GT = 4,
	       GE = 5,
	       TotalComparison };
};

class DRequest {
    public:
	enum { TYPE_PASS = 0,
	       result_code = 1,
	       DTCResultSet = 2,
	       TYPE_SYSTEM_COMMAND = 3,
	       Get = 4,
	       Purge = 5,
	       Insert = 6,
	       Update = 7,
	       Delete = 8,
	       Replace = 12,
	       Flush = 13,
	       // OBSOLETED
	       Invalidate = 14,
	       Monitor = 15,
	       // work helper 重新载入配置文件
	       ReloadConfig = 16,
	       // master-slave backup
	       Replicate = 17,
	       // for cluster scales
	       LocalMigrate = 18,
	};

	class Flag {
	    public:
		enum { KeepAlive = 1,
		       NeedTableDefinition = 2,
		       no_cache = 4,
		       NoResult = 8,
		       no_next_server = 16,
		       MultiKeyValue = 32,
		       admin_table = 64,
		};
	};

	class Section {
	    public:
		enum { VersionInfo = 0,
		       table_definition = 1,
		       RequestInfo = 2,
		       ResultInfo = 3,
		       UpdateInfo = 4,
		       ConditionInfo = 5,
		       FieldSet = 6,
		       DTCResultSet = 7,
		       Total };
	};

	class SystemCommand {
	    public:
		enum CMD {
			RegisterHB = 1,
			LogoutHB = 2,
			GetKeyList = 3,
			GetUpdateKey = 4,
			GetRawData = 5,
			ReplaceRawData = 6,
			AdjustLRU = 7,
			VerifyHBT = 8,
			GetHBTime = 9,
			SET_READONLY = 10,
			SET_READWRITE = 11,
			QueryServerInfo = 12,
			kNodeHandleChange = 13,
			Migrate = 14,
			ReloadClusterNodeList = 15,
			SetClusterNodeState = 16,
			change_node_address = 17,
			GetClusterState = 18,
			PurgeForHit = 19,
			QUERY_MEM_INFO = 20,
			ClearCache = 21,
			MigrateDB = 22,
			MigrateDBSwitch = 23,
			ColExpandStatus = 24,
			col_expand = 25,
			ColExpandDone = 26,
			ColExpandKey = 27,
			Cascade = 28,
		};
	};
};

struct DTC_HEADER_V1 {
	uint8_t version;
	uint8_t scts;
	uint8_t flags;
	uint8_t cmd;
	uint32_t len[DRequest::Section::Total];
};

struct DTC_HEADER_V2 {
	uint8_t version;
	uint8_t admin;
	uint8_t reserved[2];
	uint32_t packet_len;
	uint64_t id;
};

struct DTCServerInfo {
	uint8_t version;
	uint8_t reserved[3];
	uint32_t binlog_id;
	uint32_t binlog_off;
	uint64_t memsize;
	uint64_t datasize;
};

struct DTCTimeInfo {
	uint64_t time;
};

#endif
