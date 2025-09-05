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
#include <stdarg.h>
#include <stdio.h>

#include "buffer.h"

void CreateBuff(int inlen, int &len, char **buff)
{
	if (*buff == NULL) {
		*buff = (char *)CALLOC(inlen, sizeof(char));
		if (*buff == NULL)
			len = 0;
		else
			len = inlen;
	} else {
		if (len < inlen) {
			FREE_IF(*buff);
			*buff = (char *)CALLOC(inlen, sizeof(char));
			if (*buff == NULL)
				len = 0;
			else
				len = inlen;
		} else {
			memset(*buff, 0x0, len);
		}
	}
}

int buffer::bprintf(const char *format, ...)
{
	va_list ap;
	long len;

	va_start(ap, format);
	len = bprintf(format, ap);
	va_end(ap);
	return len;
}

int buffer::vbprintf(const char *format, va_list ap)
{
	long len;
	va_list ap2;

#ifdef __va_copy
	__va_copy(ap2, ap);
#else
	va_copy(ap2, ap);
#endif
	len = vsnprintf(cursor(), remain(), format, ap);
	if (len < 0)
		return len;

	if ((unsigned long)len >= remain()) {
		if (expand(len) < 0)
			return -1;
		len = vsnprintf(cursor(), remain(), format, ap2);
		if (len < 0)
			return len;
	}
	dataSize += len;
	return len;
}
