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
* 
*/
#include <errno.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>

#include "app_shm_manager.h"

char *get_shm(uint32_t key, uint32_t len, int flag, int *shm_id, bool create,
	      int *exist)
{
	if (!shm_id || !exist)
		return NULL;
	//	printf("Key:0x%x, Len:%d, Flag:%o\n", key, len, flag);
	void *shm_ptr = NULL; //(void*) -1;

	//Get shared memory
	(*shm_id) = shmget(key, len, flag);
	if ((*shm_id) < 0) {
		if (ENOENT != errno) {
			printf("shmget failed, ShmId:%d, key:%d, len:%d, flag:%d, errno:%d, strerror:%s\n",
			       *shm_id, key, len, flag, errno, strerror(errno));
			return NULL;
		}
		*exist = 0;
	}
	//Create if doesn't exist
	if (!(*exist)) {
		if (!create) {
			return NULL;
		}
		(*shm_id) = shmget(key, len, flag | IPC_CREAT);
		if ((*shm_id) < 0) {
			printf("shmget failed, ShmId:%d, errno:%d, strerror:%s\n",
			       *shm_id, errno, strerror(errno));
			return NULL;
		}
	}
	//Bind to shared memory
	int access_flag = 0;
	if ((shm_ptr = shmat((*shm_id), NULL, access_flag)) == (void *)-1) {
		printf("shmat failed, ShmId:%d, errno:%d, strerror:%s\n",
		       *shm_id, errno, strerror(errno));
		return NULL;
	}

	return (char *)(shm_ptr);
}

int detach_shm(void *shmaddr)
{
	if (!shmaddr) {
		return -NULL_PTR;
	}

	if (shmdt(shmaddr) == -1) {
		return -errno;
	}

	return 0;
}
