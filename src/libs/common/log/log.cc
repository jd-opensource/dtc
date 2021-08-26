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
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "compiler.h"
#include "log.h"
#include "daemon/daemon.h"
#include "config/config.h"
#include "../../stat/stat_dtc.h"

void init_log4cplus()
{
	PropertyConfigurator::doConfigure(LOG4CPLUS_TEXT(LOG4CPLUS_CONF_FILE));
}

void write_log(Logger logger, int level, const char *file_name,
	       const char *func_name, int line, const char *fmt, ...)
{
	//eg:[test.cpp] - [main] : 28--
	char str[4096];
	sprintf(str, "%d", line);
	string msg = "[" + (string)file_name + " : " + (string)str + "] - [" +
		     (string)func_name + "]" + +" -- ";
	char buf[4096];
	int i;
	va_list ap;
	va_start(ap, fmt);
	i = vsnprintf(buf, 4096, fmt, ap);
	char *tmp = buf;
	string s = msg + string(tmp);
	va_end(ap);
	switch (level) {
	case 1:
		LOG4CPLUS_TRACE(logger, s);
		break;
	case 2:
		LOG4CPLUS_DEBUG(logger, s);
		break;
	case 3:
		LOG4CPLUS_INFO(logger, s);
		break;
	case 4:
		LOG4CPLUS_WARN(logger, s);
		break;
	case 5:
		LOG4CPLUS_ERROR(logger, s);
		break;
	case 6:
		LOG4CPLUS_FATAL(logger, s);
		break;
	default:
		break;
	}
}
