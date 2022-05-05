/*
 * =====================================================================================
 *
 *       Filename:  poll_thread_group.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  10/05/2014 17:40:19 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  linjinming (prudence), prudence@163.com
 *        Company:  JD, China
 *
 * =====================================================================================
 */
#ifndef __POLLTHREADGROUP_H__
#define __POLLTHREADGROUP_H__

#include "poll_thread.h"

class CPollThreadGroup
{
public:
	CPollThreadGroup (const std::string groupName);
	CPollThreadGroup(const std::string groupName, int numThreads, int mp);
	virtual ~CPollThreadGroup ();

	PollerBase *GetPollThread();
	void Start(int numThreads, int mp);
	void RunningThreads();
	int GetPollThreadIndex(PollerBase *thread);
	int GetPollThreadSize();
	PollerBase *GetPollThread(int threadIdx);
protected:
	int         threadIndex;
	int         numThreads;
	std::string groupName;
	PollerBase **pollThreads;
};

#endif
