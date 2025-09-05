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
#ifndef __HOTBACK_TASK_H__
#define __HOTBACK_TASK_H__

struct HotBackTask {
    private:
	/*do nothing, only forbiden use */
	HotBackTask(const HotBackTask &other);
	HotBackTask &operator=(const HotBackTask &other);

    public:
	HotBackTask();
	~HotBackTask();

	void set_packed_key(char *pPackedKey, int keyLen);
	void set_value(char *pValue, int valueLen);
	void set_type(int type)
	{
		m_Type = type;
	}
	void set_flag(int flag)
	{
		m_Flag = flag;
	}
	char *get_packed_key()
	{
		return m_pPackedKey;
	}
	char *get_value()
	{
		return m_pValue;
	}
	int get_type()
	{
		return m_Type;
	}
	int get_flag()
	{
		return m_Flag;
	}
	int get_packed_key_len()
	{
		return m_PackedKeyLen;
	}
	int get_value_len()
	{
		return m_ValueLen;
	}

    private:
	int m_Type;
	int m_Flag;
	char *m_pPackedKey;
	int m_PackedKeyLen;
	char *m_pValue;
	int m_ValueLen;
};
#endif