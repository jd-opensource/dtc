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
#ifndef CLIENT_RESOURCE_POOL_H
#define CLIENT_RESOURCE_POOL_H

#include <stdint.h>

class DTCJobOperation;
class Packet;
class ClientResourceSlot {
    public:
	ClientResourceSlot()
		: freenext(0), freeprev(0), job(NULL), packet(NULL), seq(0)
	{
	}
	~ClientResourceSlot();
	int freenext;
	/*only used if prev slot free too*/
	int freeprev;
	DTCJobOperation *job;
	Packet *packet;
	uint32_t seq;
};

/* automaticaly change size according to usage status */
/* resource from pool need */
class ClientResourcePool {
    public:
	ClientResourcePool()
		: total(0), used(0), freehead(0), freetail(0), taskslot(NULL)
	{
	}
	~ClientResourcePool();
	int do_init();
	inline ClientResourceSlot *Slot(unsigned int id)
	{
		return &taskslot[id];
	}
	/* clean resource allocated */
	int Alloc(unsigned int &id, uint32_t &seq);
	int Fill(unsigned int id);
	void Clean(unsigned int id);
	/* free, should clean first */
	void Free(unsigned int id, uint32_t seq);

    private:
	unsigned int total;
	unsigned int used;
	unsigned int freehead;
	unsigned int freetail;
	ClientResourceSlot *taskslot;

	int Enlarge();
	void Shrink();
	inline int half_use()
	{
		return used <= total / 2 ? 1 : 0;
	}
};

#endif
