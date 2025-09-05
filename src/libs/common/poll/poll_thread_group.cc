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
#include "poll_thread_group.h"

PollThreadGroup::PollThreadGroup(const std::string groupName) : threadIndex(0), pollThreads(NULL)
{
	this->groupName = groupName;
}

PollThreadGroup::PollThreadGroup(const std::string groupName, int numThreads, int mp) : threadIndex(0), pollThreads(NULL)
{
	this->groupName = groupName;
	Start(numThreads, mp);
}

PollThreadGroup::~PollThreadGroup()
{
	if (pollThreads != NULL)
	{
		for (int i = 0; i < this->numThreads; i++)
		{
			if (pollThreads[i])
			{
				pollThreads[i]->interrupt();
				delete pollThreads[i];
			}
		}

		delete pollThreads;

		pollThreads = NULL;
	}
}

PollerBase *PollThreadGroup::get_poll_thread()
{
	if (pollThreads != NULL)
		return pollThreads[threadIndex++ % numThreads];

	return NULL;
}

PollerBase *PollThreadGroup::get_poll_thread(int threadIdx)
{
	if (threadIdx > numThreads)
	{
		return NULL;
	}
	else
	{
		return pollThreads[threadIdx];
	}
}

void PollThreadGroup::Start(int numThreads, int mp)
{
	char threadName[256];
	this->numThreads = numThreads;

	pollThreads = new PollerBase *[this->numThreads];

	for (int i = 0; i < this->numThreads; i++)
	{
		snprintf(threadName, sizeof(threadName), "%s@%d", groupName.c_str(), i);

		pollThreads[i] = new PollerBase(threadName);
		// set_max_pollers must be called before InitializeThread, otherwise it won't take effect
		pollThreads[i]->set_max_pollers(mp);
		pollThreads[i]->initialize_thread();
	}
}

void PollThreadGroup::running_threads()
{
	if (pollThreads == NULL)
		return;

	for (int i = 0; i < this->numThreads; i++)
	{
		pollThreads[i]->running_thread();
	}
}

int PollThreadGroup::get_poll_threadIndex(PollerBase *thread)
{
	for (int i = 0; i < this->numThreads; i++)
	{
		if (thread == pollThreads[i])
			return i;
	}
	return -1;
}

int PollThreadGroup::get_poll_threadSize()
{
	return numThreads;
}
