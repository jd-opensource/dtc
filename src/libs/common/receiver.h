/*
* Copyright [2021] JD.com, Inc.
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
#ifndef __RECVEIVER_H__
#define __RECVEIVER_H__

#include <unistd.h>
#include "protocol.h"

class SimpleReceiver {
    private:
	struct DTC_HEADER_V1 _header;
	char *_packet;
	int _netfd;
	int _count;
	int _offset;

    public:
	SimpleReceiver(int fd = -1)
		: _packet(NULL), _netfd(fd), _count(0), _offset(0){};
	~SimpleReceiver(void){};

	void attach(int fd)
	{
		_netfd = fd;
	}
	void erase(void)
	{
		_packet = NULL;
		_count = 0;
		_offset = 0;
	}
	int remain(void) const
	{
		return _count - _offset;
	}
	int discard(void)
	{
		char buf[1024];
		int len;
		const int bsz = sizeof(buf);
		while ((len = remain()) > 0) {
			int rv = read(_netfd, buf, (len > bsz ? bsz : len));
			if (rv <= 0)
				return rv;
			_offset += rv;
		}
		return 1;
	}
	int fill(void)
	{
		int rv = read(_netfd, _packet + _offset, remain());
		if (rv > 0)
			_offset += rv;
		return rv;
	}
	void init(void)
	{
		_packet = (char *)&_header;
		_count = sizeof(_header);
		_offset = 0;
	}
	void set(char *p, int sz)
	{
		_packet = p;
		_count = sz;
		_offset = 0;
	}
	char *c_str(void)
	{
		return _packet;
	}
	const char *c_str(void) const
	{
		return _packet;
	}
	DTC_HEADER_V1 &header(void)
	{
		return _header;
	}
	const DTC_HEADER_V1 &header(void) const
	{
		return _header;
	}
};

#endif
