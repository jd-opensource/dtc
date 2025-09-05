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
#ifndef __CH_CACHE_ERRR_H__
#define __CH_CACHE_ERRR_H__

enum { EC_ERROR_BASE = 2000,
       EC_BAD_COMMAND, // unsupported command
       EC_MISSING_SECTION, // missing mandatory section
       EC_EXTRA_SECTION, // incompatible section present
       EC_DUPLICATE_TAG, // same tag appear twice

       EC_DUPLICATE_FIELD, //5: same field appear twice in .Need()
       EC_BAD_SECTION_LENGTH, // section length too short
       EC_BAD_VALUE_LENGTH, // value length not allow
       EC_BAD_STRING_VALUE, // string value w/o NULL
       EC_BAD_FLOAT_VALUE, // invalid float format

       EC_BAD_FIELD_NUM, //10: invalid total field#
       EC_EXTRA_SECTION_DATA, // section length too large
       EC_BAD_VALUE_TYPE, // incompatible value type
       EC_BAD_OPERATOR, // incompatible operator/comparison
       EC_BAD_FIELD_ID, // invalid field ID

       EC_BAD_FIELD_NAME, //15: invalid field name
       EC_BAD_FIELD_TYPE, // invalid field type
       EC_BAD_FIELD_SIZE, // invalid field size
       EC_TABLE_REDEFINED, // table defined twice
       EC_TABLE_MISMATCH, // request table != server table

       EC_VERSION_MISMATCH, //20: unsupported protocol version
       EC_CHECKSUM_MISMATCH, // table hash not equal
       EC_NO_MORE_DATA, // End of Result
       EC_NEED_FULL_FIELDSET, // only full field set accepted by helper
       EC_BAD_KEY_TYPE, // key type incompatible

       EC_BAD_KEY_SIZE, // 25: key size incompatible
       EC_SERVER_BUSY, //server error
       EC_BAD_SOCKET, // network failed
       EC_NOT_INITIALIZED, // object didn't initialized
       EC_BAD_HOST_STRING,

       EC_BAD_TABLE_NAME, // 30
       EC_TASK_NEED_DELETE,
       EC_KEY_NEEDED,
       EC_SERVER_ERROR,
       EC_UPSTREAM_ERROR,

       EC_KEY_OVERFLOW, // 35
       EC_BAD_MULTIKEY,
       EC_READONLY_FIELD,
       EC_BAD_ASYNC_CMD,
       EC_OUT_OF_KEY_RANGE,

       EC_REQUEST_ABORTED, // 40
       EC_PARALLEL_MODE,
       EC_KEY_NOTEXIST,
       EC_SERVER_READONLY,
       EC_BAD_INVALID_FIELD,

       EC_DUPLICATE_KEY, // 45
       EC_TOO_MANY_KEY_VALUE,
       EC_BAD_KEY_NAME,
       EC_BAD_RAW_DATA,
       EC_BAD_HOTBACKUP_JID,

       EC_FULL_SYNC_COMPLETE, //50
       EC_FULL_SYNC_STAGE,
       EC_INC_SYNC_STAGE,
       EC_ERR_SYNC_STAGE,
       EC_NOT_ALLOWED_INSERT,

       EC_COMPRESS_ERROR, //55
       EC_UNCOMPRESS_ERROR,
       EC_TASKPOOL,
       EC_STATE_ERROR,
       EC_DATA_NEEDED,

       EC_BAD_ACCESS_KEY, //60
       EC_TASK_TIMEOUT,
       EC_BUSINESS_WITHOUT_EXPIRETIME, //62
       EC_EMPTY_TBDEF, //63
       EC_INVALID_KEY_VALUE, //64

       EC_INVALID_EXPIRETIME, //65
       EC_GET_EXPIRETIME_END_OF_RESULT, //66
       EC_ERR_MIGRATEDB_ILLEGAL,
       EC_ERR_MIGRATEDB_DUPLICATE,
       EC_ERR_MIGRATEDB_HELPER,

       EC_ERR_MIGRATEDB_MIGRATING, // 70
       EC_ERR_MIGRATEDB_NOT_START,
       EC_ERR_MIGRATEDB_DISTINCT,
       EC_ERR_HOTBACK_WRITELRU,
       EC_ERR_HOTBACK_WRITEUPDATE,

       EC_ERR_COL_EXPANDING, // 75
       EC_ERR_COL_EXPAND_DUPLICATE,
       EC_ERR_COL_EXPAND_DONE_DUPLICATE,
       EC_ERR_COL_EXPAND_DONE_DISTINCT,
       EC_ERR_COL_EXPAND_NO_MEM,

       EC_ERR_COL_EXPAND_KEY_ERROR, // 80
       EC_ERR_COL_NOT_EXPANDING,
       EC_ERR_COL_NO_KEY,
       EC_GET_EXPIRETIME_RESULT_NULL,
       EC_GET_EXPIRETIME_FIELD_EXPIRETIME_NULL,

       EC_BAD_FIELD_SIZE_ON_RESOLVE, // 85
       EC_EXCEPTION_ON_RESOLVE,
       EC_BAD_FIELD_SIZE_ON_CHECKKEY,
       EC_EXCEPTION_ON_CHECKKEY,
       EC_NOT_HAVE_AVAILABLE_HELPERGROUP,

       EC_NOT_HAVE_AVAILABLE_HELPERCLIENT, // 90
       EC_RELOAD_CONFIG_FAILED,
};

#endif
