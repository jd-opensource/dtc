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
#ifndef __DTCLOG_H__
#define __DTCLOG_H__

#include <sys/cdefs.h>
__BEGIN_DECLS


#include <asm/unistd.h>
#include <unistd.h>
#ifndef __NR_gettid
#endif
static inline int _gettid_(void)
{
	return syscall(__NR_gettid);
}

#include <sys/time.h>
static inline unsigned int GET_MSEC(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}
#define INIT_MSEC(v) v = GET_MSEC()
#define CALC_MSEC(v) v = GET_MSEC() - (v)
static inline unsigned int GET_USEC(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000000 + tv.tv_usec;
}
#define INIT_USEC(v) v = GET_USEC()
#define CALC_USEC(v) v = GET_USEC() - (v)

__END_DECLS

#include <string>
#include <stdarg.h>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>
#include <log4cplus/helpers/stringhelper.h>
#include <log4cplus/loggingmacros.h>
#include <log4cplus/asyncappender.h>

using namespace std;
using namespace log4cplus;
using namespace log4cplus::helpers;

static Logger logger = Logger::getRoot();

#define LOG4CPLUS_CONF_FILE "../conf/log4cplus-life.conf"

/*********************************************
**TRACE：调试应用的详细步骤
**DEBUG：算法关键部分的相关信息
**INFO： 应用的内部状态信息
**WARN： 可以避免的内部状态信息
**ERROR：发生了错误，且应用程序知道如何处理它
**FATAL：发生了不可逆转的错误，程序无法继续运行
 **********************************************/
#define log4cplus_trace(fmt, args...)                                          \
	write_log(logger, 1, __FILE__, __FUNCTION__, __LINE__, fmt, ##args)
#define log4cplus_debug(fmt, args...)                                          \
	write_log(logger, 2, __FILE__, __FUNCTION__, __LINE__, fmt, ##args)
#define log4cplus_info(fmt, args...)                                           \
	write_log(logger, 3, __FILE__, __FUNCTION__, __LINE__, fmt, ##args)
#define log4cplus_warning(fmt, args...)                                        \
	write_log(logger, 4, __FILE__, __FUNCTION__, __LINE__, fmt, ##args)
#define log4cplus_error(fmt, args...)                                          \
	write_log(logger, 5, __FILE__, __FUNCTION__, __LINE__, fmt, ##args)
#define log4cplus_fatal(fmt, args...)                                          \
	write_log(logger, 6, __FILE__, __FUNCTION__, __LINE__, fmt, ##args)
#define log4cplus_bare(lvl, fmt, args...)                                      \
	write_log(logger, lvl, NULL, NULL, 0, fmt, ##args)

extern void write_log(Logger, int, const char *, const char *, int,
		      const char *, ...);

extern void init_log4cplus();
#endif
