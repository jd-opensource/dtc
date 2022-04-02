#include <unistd.h>

#include "hwc.h"

WatchDogHWC::WatchDogHWC(WatchDog *o, int sec)
	: WatchDogDaemon(o, sec)
{
	strncpy(watchdog_object_name_, "hwcserver", sizeof(watchdog_object_name_));
}

WatchDogHWC::~WatchDogHWC(void)
{
}

void WatchDogHWC::exec(void)
{
	char* argv[4];

	argv[0] = watchdog_object_name_;
    argv[1] = "../conf/dtc.yaml";
	argv[2] = "../conf/table.yaml";
	argv[2] = "1";
	execv(argv[0], argv);
}
