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
#include <libgen.h>
#include "compiler.h"
#include "log.h"
#include "config.h"

#define LOGSIZE 4096
#define MSGSIZE 4096
#define REMOTELOG_ERR_TYPE 14
int stat__log_level__ = 6;
int __business_id__ = 0;

static int noconsole = 0;
static int logfd = -1;
static int logday = 0;
static char log_dir[128] = "/export/Logs";
static char appname[32] = "";
static int (*alert_hook)(const char *, int);

//remote log area
static struct sockaddr_in remote_log_addr;
static int sockaddr_length = 0;

void _set_remote_log_config_(const char *addr, int port, int businessid)
{
	if (NULL == addr)
	{
		return;
	}
	if (0 == port)
	{
		return;
	}
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

#if HAS_TLS
//thread name
static __thread const char *threadname __TLS_MODEL;
void stat_set_log_thread_name_(const char *n) { threadname = n; }
//remote log fd
static __thread int remote_log_fd;
int stat_set_remote_log_fd_()
{
	
	int fd = _build_remote_log_socket_();
	if (fd > 0) { remote_log_fd = fd; return 0; }
	else { return -1; }
}
#else
//thread name
static pthread_key_t namekey;
static pthread_once_t nameonce = PTHREAD_ONCE_INIT;
static void _init_namekey_(void) { pthread_key_create(&namekey, NULL); }
void _set_log_thread_name_(const char *n) { pthread_setspecific(namekey, n); }
//remote log fd
static pthread_key_t fdkey;
static pthread_once_t fdonce = PTHREAD_ONCE_INIT;
static void _init_fdkey_(void) { pthread_key_create(&fdkey, NULL); }
int _set_remote_log_fd_()
{
	int fd = _build_remote_log_socket_();
	if (fd > 0) { pthread_setspecific(fdkey, fd); return 0; }
	else { return -1; }
}
#endif

unsigned int __localStatLogCnt[8];

// clean logfd when module unloaded
__attribute__((__destructor__))
static void clean_logfd(void) {
	if(logfd >= 0) {
		close(logfd);
		logfd = -1;
	}
}

void stat_init_log_ (const char *app, const char *dir)
{
#if !HAS_TLS
	pthread_once(&nameonce, _init_namekey_);
#endif
	memset(__localStatLogCnt, 0, sizeof(__localStatLogCnt));

	strncpy(appname, app, sizeof(appname)-1);
	
	if(dir) {
		strncpy (log_dir, dir, sizeof (log_dir) - 1);
	}
	mkdir(log_dir, 0777);
	if(access(log_dir, W_OK|X_OK) < 0)
	{
		log_error("logdir(%s): Not writable", log_dir);
	}

	logfd = open("/dev/null", O_WRONLY);
	if(logfd < 0)
		logfd = dup(2);
	fcntl(logfd, F_SETFD, FD_CLOEXEC);
}

void stat_set_log_level_(int l)
{
	if(l>=0)
		stat__log_level__ = l > 4 ? l : 4;
}

void stat_set_log_alert_hook_(int(*alert_func)(const char*, int))
{
	alert_hook = alert_func;
}

void stat_write_log_(
	int level, 
	const char *filename, 
	const char *funcname,
	int lineno,
	const char *format, ...)
{
	// save errno
	int savedErrNo = errno;
	int off = 0;
	int msgoff = 0;
	char buf[LOGSIZE];
	char logfile[256];
#if !HAS_TLS
	const char *threadname;
#endif

	if(appname[0] == 0)
		return;

	if(level < 0) level = 0;
	else if(level > 7) level = 7;
	
	// construct prefix
	struct tm tm;
	time_t now = time (NULL);
	localtime_r(&now, &tm);
#if HAS_TLS
#else
	pthread_once(&nameonce, _init_namekey_);
	threadname = (const char *)pthread_getspecific(namekey);
#endif
	int temp = strLevelPrint(level, buf);
	if (temp <= 0) return;
	if(filename==NULL) {
		if(threadname) {
			off = snprintf (buf + temp, LOGSIZE,
					"<%d>[%02d:%02d:%02d] %s: ",
					level,
					tm.tm_hour, tm.tm_min, tm.tm_sec,
					threadname
				       );
		} else {
			off = snprintf (buf + temp, LOGSIZE,
					"<%d>[%02d:%02d:%02d] pid[%d]: ",
					level,
					tm.tm_hour, tm.tm_min, tm.tm_sec,
					net_gettid()
				       );
		}
	} else {
		filename = basename((char *)filename);
		if(threadname)
			off = snprintf (buf + temp, LOGSIZE,
					"<%d>[%02d:%02d:%02d]  %s: %s(%d)[%s]: ",
					level,
					tm.tm_hour, tm.tm_min, tm.tm_sec,
					threadname,
					filename, lineno, funcname
				       );
		else
			off = snprintf (buf + temp, LOGSIZE,
					"<%d>[%02d:%02d:%02d] pid[%d]: %s(%d)[%s]: ",
					level,
					tm.tm_hour, tm.tm_min, tm.tm_sec,
					net_gettid(),
					filename, lineno, funcname
				       );
	}
	off += temp;
	if(off >= LOGSIZE)
		off = LOGSIZE - 1;
		
	{
	int today = tm.tm_year*1000 + tm.tm_yday;
	if(logfd >= 0 && today != logday)
	{
		int fd;

		logday = today;
		snprintf (logfile, sizeof(logfile),
				"%s/%s.error%04d%02d%02d.log", log_dir, appname,
				tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
		fd = open (logfile, O_CREAT | O_LARGEFILE | O_APPEND |O_WRONLY, 0644);
		if(fd >= 0)
		{
			dup2(fd, logfd);
			close(fd);
			fcntl(logfd, F_SETFD, FD_CLOEXEC);
		}
	}
	}

	// remember message body start point
	msgoff = off;

	{
		// formatted message
		va_list ap;
		va_start(ap, format);
		// restore errno
		errno = savedErrNo;
		off += vsnprintf(buf+off, LOGSIZE-off, format, ap);
		va_end(ap);
	}

	if(off >= LOGSIZE)
		off = LOGSIZE - 1;
	if(buf[off-1]!='\n'){
		buf[off++] = '\n';
	}

	int unused;

	if(logfd >= 0) {
		unused = write(logfd, buf, off);
	}
	if (level <= 3)
	{
		remote_log(0, NULL,REMOTELOG_ERR_TYPE, 0, buf,now, 0, 0, off); //error optype is 14
	}
	

	if(level <= 6 && !noconsole) {
		// debug don't send to console/stderr
		unused = fwrite(buf, 1, off, stderr);
		if(unused <= 0) {
			// disable console if write error
			noconsole = 1;
		}
	}

	if(alert_hook && level <= 1 /* emerg,alert */) {
		if(alert_hook(buf+msgoff, off-msgoff-1) < 0) {
			// attr report error, log a warning message
			buf[1] = '4'; // 4 is warning level
			// replace message body
			off = msgoff + snprintf(buf+msgoff, LOGSIZE-msgoff, "%s", "report to attr failed\n");
			// log another line
			unused = fwrite(buf, 1, off, stderr);
			if(logfd >= 0) {
				unused = write(logfd, buf, off);
			}
		}
	}
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
#if !HAS_TLS
	int remote_log_fd;
	pthread_once(&fdonce, _init_fdkey_);
	remote_log_fd = *(int*)pthread_getspecific(fdkey);
#endif
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
	CRemoteLog* pRemoteLog = (CRemoteLog*)msg;
	int body_max_len = MSGSIZE-sizeof(CRemoteLog);	
	off = snprintf(pRemoteLog->body, body_max_len, "{\"routekey\":%d,\"tableconfig\":\"%s\",\"fieldvalues\":[{\"bid\":%d,\"ukey\":\"%s\",\"op_type\":%d,\"content\":\"%s\",\"op_time\":%ld,\"op_result\":%d}]}", 
				   __business_id__, type?"dtc_business.xml":"dtc_error.xml", __business_id__, key==NULL?"":key, op_type, content==NULL?"":content, op_time, op_result);
	
	pRemoteLog->cmd = cmd;
	pRemoteLog->magic = magic;
	pRemoteLog->len = off;	
	
	//printf("send fd is %d", remote_log_fd);
	
	if (remote_log_fd  >  0)
	{
		sendto(remote_log_fd, msg, sizeof(CRemoteLog)+off, 0, (const sockaddr*)&remote_log_addr, sockaddr_length);
	}
	
}

int strLevelPrint(const int level, char* buf)
{
  switch (level)
  {
  case 0:
		return snprintf(buf, LOGSIZE, "[%s]", "EMERG");
  case 1:
		return snprintf(buf, LOGSIZE, "[%s]", "ALERT");
  case 2:
		return snprintf(buf, LOGSIZE, "[%s]", "CRIT");
  case 3:
		return snprintf(buf, LOGSIZE, "[%s]", "ERROR");
  case 4:
		return snprintf(buf, LOGSIZE, "[%s]", "WARNING");
  case 5:
		return snprintf(buf, LOGSIZE, "[%s]", "NOTICE");
  case 6:
		return snprintf(buf, LOGSIZE, "[%s]", "INFO");
  case 7:
		return snprintf(buf, LOGSIZE, "[%s]", "DEBUG");
  default:
    break;
  }

  printf("[ERROR]:unsupport log level:%d", level);
  return -1;
}

