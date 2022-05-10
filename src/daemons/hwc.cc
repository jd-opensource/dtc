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
	char* argv[6];
	// ./hwcserver -d /etc/dtc/dtc.yaml -t /etc/dtc/table.yaml
	argv[0] = watchdog_object_name_;
	argv[1] = (char*)"-d";
    argv[2] = "/etc/dtc/dtc.yaml";
	argv[3] = (char*)"-t";
	argv[4] = "/etc/dtc/table.yaml";
	argv[5] = NULL;
	execv(argv[0], argv);
}
