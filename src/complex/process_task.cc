/*
*  process_task.cc
*
*  Created on: 2018.3.16
*  Author: zhulin
*/

#include "process_task.h"
#include "poller.h"
#include <netinet/in.h>
#include <string>
#include <sstream>
using namespace std;


ProcessTask::ProcessTask()
{
}

int ProcessTask::Process(CTaskRequest *request) {
	log_debug("process");
	return 0;
}

ProcessTask::~ProcessTask()
{

}
