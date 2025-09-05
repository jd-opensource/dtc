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
#include <stddef.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>

#include "shmem.h"
#include "lock/system_lock.h"

SharedMemory::SharedMemory()
	: m_key(0), m_id(0), m_size(0), m_ptr(NULL), lockfd(-1)
{
}

SharedMemory::~SharedMemory()
{
	mem_detach();
	mem_unlock();
}

unsigned long SharedMemory::mem_open(int key)
{
	if (m_ptr)
		mem_detach();

	if (key == 0)
		return 0;

	m_key = key;
	if ((m_id = shmget(m_key, 0, 0)) == -1)
		return 0;

	struct shmid_ds ds;

	if (shmctl(m_id, IPC_STAT, &ds) < 0)
		return 0;

	return m_size = ds.shm_segsz;
}

unsigned long SharedMemory::mem_create(int key, unsigned long size)
{
	if (m_ptr)
		mem_detach();

	m_key = key;

	if (m_key == 0) {
		m_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
			     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (m_ptr != MAP_FAILED)
			m_size = size;
		else
			m_ptr = NULL;
	} else {
		if ((m_id = shmget(m_key, size,
				   IPC_CREAT | IPC_EXCL | IPC_PERM)) == -1)
			return 0;

		struct shmid_ds ds;

		if (shmctl(m_id, IPC_STAT, &ds) < 0)
			return 0;

		m_size = ds.shm_segsz;
	}
	return m_size;
}

void *SharedMemory::mem_attach(int ro)
{
	if (m_ptr)
		return m_ptr;

	m_ptr = shmat(m_id, NULL, ro ? SHM_RDONLY : 0);
	if (m_ptr == MAP_FAILED)
		m_ptr = NULL;
	return m_ptr;
}

void SharedMemory::mem_detach(void)
{
	if (m_ptr) {
		if (m_key == 0)
			munmap(m_ptr, m_size);
		else
			shmdt(m_ptr);
	}
	m_ptr = NULL;
}

int SharedMemory::mem_lock(void)
{
	if (lockfd > 0 || m_key == 0)
		return 0;
	lockfd = unix_socket_lock("tlock-shm-%u", m_key);
	if (lockfd < 0)
		return -1;
	return 0;
}

void SharedMemory::mem_unlock(void)
{
	if (lockfd > 0) {
		close(lockfd);
		lockfd = -1;
	}
}

int SharedMemory::mem_delete(void)
{
	mem_detach();

	if (shmctl(m_id, IPC_RMID, NULL) < 0)
		return -1;
	return 0;
}
