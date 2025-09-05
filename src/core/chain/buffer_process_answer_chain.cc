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

#include <stdlib.h>
#include <stdio.h>
#include <endian.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "packet/packet.h"
#include "log/log.h"
#include "buffer_process_answer_chain.h"
#include "buffer_flush.h"
#include "mysql_error.h"
#include "sys_malloc.h"
#include "data_chunk.h"
#include "raw_data_process.h"
#include "key/key_route_ask_chain.h"
#include "buffer_remoteLog.h"
#include "hotback_task.h"
#include "tree_data_process.h"
DTC_USING_NAMESPACE;

void BufferProcessAnswerChain::job_answer_procedure(
	DTCJobOperation *job_operation)
{
	buffer_reply_notify_owner_->job_answer_procedure(job_operation);
}
