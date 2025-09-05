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
#include "hotback_task.h"
#include "mem_check.h"
#include <stdlib.h>
#include <string.h>
HotBackTask::HotBackTask()
	: m_Type(0), m_Flag(0), m_pPackedKey(NULL), m_PackedKeyLen(0),
	  m_pValue(NULL), m_ValueLen(0)
{
}

HotBackTask::~HotBackTask()
{
	FREE_IF(m_pPackedKey);
	FREE_IF(m_pValue);
}

void HotBackTask::set_packed_key(char *pPackedKey, int keyLen)
{
	if ((NULL == pPackedKey) || (0 == keyLen)) {
		return;
	}

	m_pPackedKey = (char *)MALLOC(keyLen);
	if (NULL == m_pPackedKey) {
		return;
	}
	m_PackedKeyLen = keyLen;
	memcpy(m_pPackedKey, pPackedKey, m_PackedKeyLen);
}

void HotBackTask::set_value(char *pValue, int valueLen)
{
	if ((NULL == pValue) || (0 == valueLen)) {
		return;
	}

	m_pValue = (char *)MALLOC(valueLen);
	if (NULL == m_pPackedKey) {
		return;
	}
	m_ValueLen = valueLen;
	memcpy(m_pValue, pValue, m_ValueLen);
}
