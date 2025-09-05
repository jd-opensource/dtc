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
#ifndef __H_DTC_DECODER_UNIT_H__
#define __H_DTC_DECODER_UNIT_H__

class PollerBase;
class TimerList;

class DecoderBase {
    public:
	DecoderBase(PollerBase *, int idletimeout);
	virtual ~DecoderBase();

	TimerList *idle_list(void) const
	{
		return m_idle_list;
	}
	int idle_time(void) const
	{
		return idleTime;
	}
	PollerBase *owner_thread(void) const
	{
		return owner;
	}

	virtual int process_stream(int fd, int req, void *, int) = 0;
	virtual int process_dgram(int fd) = 0;

    protected:
	PollerBase *owner;
	int idleTime;
	TimerList *m_idle_list;
};

#endif
