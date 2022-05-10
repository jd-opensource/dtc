/*
 * =====================================================================================
 *
 *       Filename:  daemon.h
 *
 *    Description:  daemon class definition.
 *
 *        Version:  1.0
 *        Created:  11/01/2021
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  chenyujie, chenyujie28@jd.com@jd.com
 *        Company:  JD.com, Inc.
 *
 * =====================================================================================
 */

#ifndef __DAEMON_H__
#define __DAEMON_H__

#include <signal.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>

class DaemonBase {
public:
	static int EnableFdLimit(int maxfd);
	static int EnableCoreDump();

	static int DaemonStart(int back = 1);
	static int DaemonWait();

private:
	 DaemonBase() {
	} virtual ~ DaemonBase() {
	}
	DaemonBase(const DaemonBase &);
	const DaemonBase & operator=(const DaemonBase &);

	static void sig_handler(int signal) {
		_stop = 1;
	}

public:
	static volatile int _stop;
};

#endif
