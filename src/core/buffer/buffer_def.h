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

#ifndef __CACHE_DEF_H
#define __CACHE_DEF_H

#define E_OK 0 //success
#define E_FAIL -1 //fail
#define KEY_LEN_LEN sizeof(char) //"key长"字段长度
#define MAX_KEY_LEN 256 //key最大长度,由"key长"字段长度所能表示的最大数字决定
#define ERR_MSG_LEN 1024
#define MAX_PURGE_NUM 1000 //每次purge的节点数上限
#define CACHE_SVC "dtc" //cache服务名
//#define VERSION			"1.0.3"			//版本信息

#define STRNCPY(dest, src, len)                                                \
	{                                                                      \
		memset(dest, 0x00, len);                                       \
		strncpy(dest, src, len - 1);                                   \
	}
#define SNPRINTF(dest, len, fmt, args...)                                      \
	{                                                                      \
		memset(dest, 0x00, len);                                       \
		snprintf(dest, len - 1, fmt, ##args);                          \
	}
#define MSGNCPY(dest, len, fmt, args...)                                       \
	{                                                                      \
		memset(dest, 0x00, len);                                       \
		snprintf(dest, len - 1, "[%s][%d]%s: " fmt "\n", __FILE__,     \
			 __LINE__, __FUNCTION__, ##args);                      \
	}

#endif
