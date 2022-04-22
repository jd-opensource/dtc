#include "context.h"

Context::Context() {
	stop_flag = false;
	reload_flag = false;
	exiting_flag = false;
	delete_pid = true;

	listen_addr = "0.0.0.0";
	listen_port = -1;
	backlog = 10240;

	pid_file = "dbproxy.pid";
	log_dir = "/export/Logs";
	log_name = "";
	log_level = 6;

	argc = 0;
	argv = NULL;
}
