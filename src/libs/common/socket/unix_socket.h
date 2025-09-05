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
#include <sys/un.h>
#include <sys/socket.h>

static inline int is_unix_socket_path(const char *path)
{
	return path[0] == '/' || path[0] == '@';
}

static inline int init_unix_socket_address(struct sockaddr_un *addr,
					   const char *path)
{
	bzero(addr, sizeof(struct sockaddr_un));
	addr->sun_family = AF_LOCAL;
	strncpy(addr->sun_path, path, sizeof(addr->sun_path) - 1);
	socklen_t addrlen = SUN_LEN(addr);
	if (path[0] == '@')
		addr->sun_path[0] = '\0';
	return addrlen;
}
