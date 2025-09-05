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
#include <stdio.h>
#include <alloca.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>

unsigned int get_local_ip(void)
{
	volatile static unsigned int cached_localip = INADDR_ANY;
	int fd;
	struct ifconf ifc;
	struct ifreq *ifr = NULL;
	int n = 0;
	int i;

	if (cached_localip != 0)
		return cached_localip;

	fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (fd < 0)
		return INADDR_ANY;

	ifc.ifc_len = 0;
	ifc.ifc_req = NULL;
	if (ioctl(fd, SIOCGIFCONF, &ifc) == 0)
	{
		ifr = (struct ifreq *)alloca(ifc.ifc_len > 128 ? ifc.ifc_len : 128);
		ifc.ifc_req = ifr;
		if (ioctl(fd, SIOCGIFCONF, &ifc) == 0)
			n = ifc.ifc_len / sizeof(struct ifreq);
	}
	close(fd);

	for (i = 0; i < n; i++)
	{
		if (ifr[i].ifr_addr.sa_family == AF_INET)
		{
			unsigned char *p = (unsigned char *)&((struct sockaddr_in *)&ifr[i].ifr_addr)->sin_addr.s_addr;
			if (p[0] == 10 || (p[0] == 172 && (p[1] >= 16 && p[1] <= 31)) ||
				(p[0] == 192 && p[1] == 168))

				return cached_localip = *(unsigned int *)p;
		}
	}

	return INADDR_ANY;
}
