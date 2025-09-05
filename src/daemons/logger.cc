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
* 
*/
#include <string.h>
#include "daemon/daemon.h"
#include "proc_title.h"
#include "config/config.h"
#include "gdb.h"
#include "log/log.h"
#include "daemons.h"
#include "unit.h"
#include "fault.h"
#include "thread/thread.h"

#define HOOKSO "../bin/faultlogger"

/* Set environment variable LD_PRELOAD */
static void set_ld_preload()
{
	if (access(HOOKSO, R_OK) == 0) {
		char *preload = canonicalize_file_name(HOOKSO);
		char *p = getenv("LD_PRELOAD");
		if (p == NULL) {
			setenv("LD_PRELOAD", preload, 1);
		} else {
			char *concat = NULL;
			int unused = 0;
			if (unused == 0)
				unused = asprintf(&concat, "%s:%s", p, preload);
			setenv("LD_PRELOAD", concat, 1);
		}
	}
}

/* Start default logging thread */
int start_fault_logger(WatchDog *watchdog)
{
	/**
	 * CarshProtect/CrashLog mode:
	 * 0 -- disabled
	 * 1 -- log-only
	 * 2 -- protect
	 * 3 -- screen
	 * 4 -- xterm
	 */
	/* default protect */
	int mode = 2;
	const char *display;

	display = g_dtc_config->get_str_val("cache", "FaultLoggerMode");
	if (display == NULL || !display[0]) {
		/* protect */
		mode = 0;
	} else if (!strcmp(display, "log")) {
		/* log */
		mode = 1;
	} else if (!strcmp(display, "dump")) {
		/* log */
		mode = 1;
	} else if (!strcmp(display, "protect")) {
		/* protect */
		mode = 2;
	} else if (!strcmp(display, "screen")) {
		/* screen */
		mode = 3;
	} else if (!strcmp(display, "xterm")) {
		if (getenv("DISPLAY")) {
			/* xterm */
			mode = 4;
			display = NULL;
		} else {
			log4cplus_warning(
				"FaultLoggerTarget set to \"xterm\", but no DISPLAY found");
			/* screen */
			mode = 2;
		}
	} else if (!strncmp(display, "xterm:", 6)) {
		mode = 4; // xterm
		display += 6;
	} else if (!strcasecmp(display, "disable")) {
		mode = 0;
	} else if (!strcasecmp(display, "disabled")) {
		mode = 0;
	} else {
		log4cplus_warning("unknown FaultLoggerMode \"%s\"", display);
	}
	log4cplus_info("FaultLoggerMode is %s\n",
		       ((const char *[]){ "disable", "log", "protect", "screen",
					  "xterm" })[mode]);
	if (mode == 0)
		return 0;
	int pid = fork();
	if (pid < 0)
		return -1;
	if (pid == 0) {
		set_proc_title("FaultLogger");
		Thread *loggerThread =
			new Thread("faultlogger", Thread::ThreadTypeProcess);
		loggerThread->initialize_thread();
		gdb_server(mode >= 3, display);
		exit(0);
	}
	char buf[20];
	snprintf(buf, sizeof(buf), "%d", pid);
	setenv(ENV_FAULT_LOGGER_PID, buf, 1);
	FaultHandler::initialize(mode >= 2);
	set_ld_preload();
	WatchDogObject *obj = new WatchDogObject(watchdog, "FaultLogger", pid);
	obj->attach_watch_dog();
	return 0;
}
