#ifndef __CONTEXT_H__
#define __CONTEXT_H__

#include <stdint.h>
#include <string>


class Context {
public:
	std::string listen_addr;
	int32_t listen_port;
	int32_t backlog;

	std::string server_name;
	std::string log_dir;
	std::string log_name;
	int32_t log_level;

	bool stop_flag;
	bool reload_flag;
	bool exiting_flag;
	bool delete_pid;

	char **argv;
	int argc;

	Context();
};

#endif // __CONTEXT_H__
