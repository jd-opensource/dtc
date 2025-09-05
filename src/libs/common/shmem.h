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
#define IPC_PERM 0644

class SharedMemory {
    private:
	int m_key;
	int m_id;
	unsigned long m_size;
	void *m_ptr;
	int lockfd;

    public:
	SharedMemory();
	~SharedMemory();

	unsigned long mem_open(int key);
	unsigned long mem_create(int key, unsigned long size);
	unsigned long mem_size(void) const
	{
		return m_size;
	}
	void *mem_ptr(void) const
	{
		return m_ptr;
	}
	void *mem_attach(int ro = 0);
	void mem_detach(void);
	int mem_lock(void);
	void mem_unlock(void);

	/* Delete shared memory */
	int mem_delete(void);
};
