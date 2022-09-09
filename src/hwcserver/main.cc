#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
// local
#include "comm.h"
#include "hwc_state_manager.h"
#include "log.h"

// common
extern "C" {
#include "proc_title.h"
}

int main(int argc, char **argv)
{
	init_proc_title(argc, argv);
	CComm::parse_argv(argc, argv);
	init_log4cplus();
	
	log4cplus_info("Hello hwcserver.");
	HwcStateManager* pStateManager = new HwcStateManager();
	pStateManager->Start();

	return 0;
}
