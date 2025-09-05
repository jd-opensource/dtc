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
#ifndef APP_CLIENT_SET_H_INCLUDED_
#define APP_CLIENT_SET_H_INCLUDED_

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <memory.h>

#define FORWARD_SHM_KEY 213576
#define MASTER_SHM_KEY 214578
#define SLAVE_SHM_KEY 215579

#define DEFAULT_BID_NUM 1000

enum { BEGIN = 10000,

       NULL_PTR,
       GET_SHM_ERR,
       DETACH_SHM_ERR,
       NOT_FIND_BID,
       OFFSET_ERR,
       IP_NUM_ERR,
       PARAM_ERR,

       END,
};

typedef struct node_header {
	int bid;
	int size;
	int offset;
} NODE_HEADER;

typedef struct forward_item {
	int lock;
	int shm_key;
	int bid_size;
	int node_size;
	int forward_shm_id;
	int master_shm_id;
	int slave_shm_id;
	unsigned int shm_size;
	uint64_t time_stamp;
	NODE_HEADER headers[DEFAULT_BID_NUM];
} FORWARD_ITEM;

#endif
