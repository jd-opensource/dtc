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
#include <sys/socket.h>
#include <arpa/inet.h>

#include "da_log.h"
#include "da_time.h"
#include "da_string.h"

#define LOGSIZE 4096
#define MSGSIZE 4096
#define REMOTELOG_ERR_TYPE 14
#define AGENT_CLIENT_TYPE 1
#define O_LARGEFILE __O_LARGEFILE
int __log_level__ = 6;
int __log_switch__ = 1;
int __business_id__ = 0;
char __own_addr__[32] = "";
int __own_port__ = 0;

static int logfd = -1;
static char appname[32] = "";
static char log_dir[128] = "./log";
static int logday = 0;
static int noconsole = 0;

//remote log area
static struct sockaddr_in remote_log_addr;
static int sockaddr_length = 0;
int da_gettid(void);

void _set_remote_log_config_(const char *addr, int port, const char *own_addr, int own_port, int businessid, int iSwitch)
{
	if(0 == iSwitch)
		return;
	
	if (NULL == addr || NULL == own_addr)
	{
		return;
	}
	if (0 == port || 0 == own_port)
	{
		return;
	}
	strncpy(__own_addr__, own_addr, 31);
	__own_addr__[31] = '\0';
	__own_port__ = own_port;
	__business_id__ = businessid;
	bzero((void*)&remote_log_addr, sizeof(remote_log_addr));
	remote_log_addr.sin_family = AF_INET;
	remote_log_addr.sin_port   = htons(port);
	int ret = inet_pton(AF_INET, addr, &remote_log_addr.sin_addr);
	if (1 != ret)
	{
		
		return;
	}
	sockaddr_length = sizeof(remote_log_addr);
	return;
}

static inline int _build_remote_log_socket_()
{
	int log_fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (log_fd <= 0)
	{
		
		return -1;
	}
	return log_fd;
}

//remote log fd
static __thread int remote_log_fd;
int _set_remote_log_fd_()
{
	int fd = _build_remote_log_socket_();
	if (fd > 0) { remote_log_fd = fd; return 0; }
	else { return -1; }
}

// clean logfd when module unloaded
__attribute__((__destructor__))
static void clean_logfd(void) {
	if(logfd >= 0) {
		close(logfd);
		logfd = -1;
	}
}

void _init_log_(const char *app, const char *dir) {
	ASSERT(app!=NULL);

	strncpy(appname, app, sizeof(appname) - 1);

	if (dir) {
		strncpy(log_dir, dir, sizeof(log_dir) - 1);
	}
	mkdir(log_dir, 0777);
	if (access(log_dir, W_OK | X_OK) < 0) {
		log_error("logdir(%s): Not writable", log_dir);
	}

	logfd = open("/dev/null", O_WRONLY);
	if (logfd < 0)
		logfd = dup(2);
	fcntl(logfd, F_SETFD, FD_CLOEXEC);
}

void _set_log_level_(int l) {

	ASSERT(l>0);
	if (l >= 0)
		__log_level__ = l > 4 ? l : 4;
}

void _set_log_switch_(int iSwitch)
{
	//0 means do not enable local logging
	__log_switch__ = iSwitch;
}

void _write_log_(int level, char *filename, const char *funcname,
		int lineno, const char *format, ...) {
	// save errno
	int savedErrNo = errno;
	int off = 0;
	int msgoff = 0;
	char buf[LOGSIZE];
	char logfile[256];
	if (appname[0] == 0)
		return;
	if (level < 0)
		level = 0;
	else if (level > 7)
		level = 7;
	struct tm tm;
	time_t now = time(NULL);
	localtime_r(&now, &tm);

	if (filename == NULL) {
		off = snprintf(buf, LOGSIZE, "<%d>[%02d:%02d:%02d] pid[%d]: ", level,
				tm.tm_hour, tm.tm_min, tm.tm_sec, getpid());
	} else {
		filename = basename(filename);
		off = snprintf(buf, LOGSIZE,
				"<%d>[%02d:%02d:%02d] pid[%d]: %s(%d)[%s]: ", level, tm.tm_hour,
				tm.tm_min, tm.tm_sec, da_gettid(), filename, lineno, funcname);
	}
	if (off >= LOGSIZE)
		off = LOGSIZE - 1;
	{
		int today = tm.tm_year * 1000 + tm.tm_yday;
		if (logfd >= 0 && today != logday) {
			int fd;

			logday = today;
			snprintf(logfile, sizeof(logfile), "%s/%s.%04d%02d%02d.log",
					log_dir, appname, tm.tm_year + 1900, tm.tm_mon + 1,
					tm.tm_mday);
			fd = open(logfile, O_CREAT | O_LARGEFILE | O_APPEND | O_WRONLY,
					0644);
			if (fd >= 0) {
				dup2(fd, logfd);
				close(fd);
				fcntl(logfd, F_SETFD, FD_CLOEXEC);
			}
		}
	}

	msgoff = off;
	{
		// formatted message
		va_list ap;
		va_start(ap, format);
		// restore errno
		errno = savedErrNo;
		off += vsnprintf(buf + off, LOGSIZE - off, format, ap);
		va_end(ap);
	}

	if (off >= LOGSIZE)
		off = LOGSIZE - 1;
	if (buf[off - 1] != '\n') {
		buf[off++] = '\n';
	}
	int unused;

	if (logfd >= 0 && (0 != __log_switch__)) {
		unused = write(logfd, buf, off);
		char szRemoteFd[512] = {0};
	}

	if(level <= 3)
	{
		//error log opttype is 14
		remote_log(0, NULL, REMOTELOG_ERR_TYPE, 0, buf, now, 0, 0, off);
	}

	if (level <= 7 && !noconsole) {
		// debug send to console/stderr too.
		unused = fwrite(buf + 3, 1, off - 3, stderr);
		if (unused <= 0) {
			// disable console if write error
			noconsole = 1;
		}
	}
}

void write_stderr(const char *fmt, ...) {
	int len, size, errno_save;
	char buf[4 * 256];
	va_list args;
	ssize_t n;

	errno_save = errno;
	len = 0; /* length of output buffer */
	size = 4 * 256; /* size of output buffer */

	va_start(args, fmt);
	len += da_vscnprintf(buf, size, fmt, args);
	va_end(args);

	buf[len++] = '\n';

	n = da_write(STDERR_FILENO, buf, len);
	errno = errno_save;
}

struct CRemoteLog
{
	int cmd;
	int magic;
	int len;
	char body[0];
};

void remote_log(int type, const char *key, int op_type, int op_result,  char *content, long op_time, int cmd, int magic , int contentlen)
{
	if (0 == sockaddr_length)
	{
		return;
	}
	if (REMOTELOG_ERR_TYPE == op_type)
	{
		if ( (contentlen > 0) &&  ( '\n' == content[contentlen-1]) )
		{
			content[contentlen-1] = 0;
		}
	}
	int off = 0;
	char msg[MSGSIZE];	
	memset(msg, 0 , MSGSIZE);	
	struct CRemoteLog* pRemoteLog = (struct CRemoteLog*)msg;
	int body_max_len = MSGSIZE-sizeof(struct CRemoteLog);	
	off = snprintf(pRemoteLog->body, body_max_len, "{\"clienttype\":%d,\"routekey\":%d,\"tableconfig\":\"%s\",\"fieldvalues\":[{\"bid\":%d,\"ip\":\"%s\",\"port\":%d,\"ukey\":\"%s\",\"op_type\":%d,\"content\":\"%s\",\"op_time\":%ld,\"op_result\":%d}]}", 
				   AGENT_CLIENT_TYPE, __business_id__, type?"dtc_business.xml":"dtc_error.xml", __business_id__, __own_addr__, __own_port__, key==NULL?"":key, op_type, content==NULL?"":content, op_time, op_result);
	
	pRemoteLog->cmd = cmd;
	pRemoteLog->magic = magic;
	pRemoteLog->len = off;	
	
	//printf("send fd is %d", remote_log_fd);
	
	if (remote_log_fd  >  0)
	{
		sendto(remote_log_fd, msg, sizeof(struct CRemoteLog)+off, 0, (const struct sockaddr*)&remote_log_addr, sockaddr_length);
	}
}
