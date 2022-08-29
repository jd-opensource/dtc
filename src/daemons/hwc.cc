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
	// ./hwcserver -d ../conf/dtc.yaml
	argv[0] = watchdog_object_name_;
	argv[1] = (char*)"-d";
    argv[2] = "../conf/dtc.yaml";
	argv[3] = NULL;
	execv(argv[0], argv);
}
