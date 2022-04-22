/*
*  process_task.h
*
*  Created on: 2018.3.16
*  Author: zhulin
*/
#ifndef __PROCESS_TASK_H__
#define __PROCESS_TASK_H__
//#include "log.h"
//#include "protocal.h"
//#include "comm.h"
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include "task_request.h"
using namespace std;

#define STRCPY(d,s) do{ strncpy(d, s, sizeof(d)-1); d[sizeof(d)-1]=0; }while(0)
#define TIMER_INTERVAL 86400
#define MAX_PACKAGE_SIZE 1024*1024

class ProcessTask
{
public:
	ProcessTask();
	virtual ~ProcessTask();
	virtual int Process(CTaskRequest *request);
};

#endif
