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

#include <ctype.h>

static inline int stricmp(const char *p, const char *q)
{
	while (toupper(*(unsigned char *)p) == toupper(*(unsigned char *)q)) {
		if (*p == '\0') {
			return 0;
		}
		p += 1;
		q += 1;
	}
	return toupper(*(unsigned char *)p) - toupper(*(unsigned char *)q);
}

static inline int strincmp(const char *p, const char *q, size_t n)
{
	while (n > 0) {
		int diff = toupper(*(unsigned char *)p) -
			   toupper(*(unsigned char *)q);
		if (diff != 0) {
			return diff;
		} else if (*p == '\0') {
			return 0;
		}
		p += 1;
		q += 1;
		n -= 1;
	}
	return 0;
}

static inline int stricoll(const char *p, const char *q)
{
	char p_buf[256];
	char q_buf[256];
	size_t p_len = strlen(p);
	size_t q_len = strlen(q);
	char *p_dst = p_buf;
	char *q_dst = q_buf;
	int i;
	if (p_len >= sizeof(p_buf)) {
		p_dst = new char[p_len + 1];
	}
	if (q_len >= sizeof(q_buf)) {
		q_dst = new char[q_len + 1];
	}
	for (i = 0; p[i] != '\0'; i++) {
		p_dst[i] = toupper(p[i] & 0xFF);
	}
	p_dst[i] = '\0';

	for (i = 0; q[i] != '\0'; i++) {
		q_dst[i] = toupper(q[i] & 0xFF);
	}
	q_dst[i] = '\0';

	int diff = strcoll(p_dst, q_dst);
	if (p_dst != p_buf) {
		delete[] p_dst;
	}
	if (q_dst != q_buf) {
		delete[] q_dst;
	}
	return diff;
}
