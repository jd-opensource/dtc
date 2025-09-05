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
*/
#ifndef __DTC_DAEMON__H__
#define __DTC_DAEMON__H__

#define MAXLISTENERS 10

class DTCConfig;
struct DbConfig;
class DTCTableDefinition;

extern DTCConfig *g_dtc_config;
extern DbConfig *dbConfig;
extern DTCTableDefinition *g_table_def[];

extern volatile int stop;
extern int background;

extern const char project_name[];
extern const char version[];
extern const char usage_argv[];
//extern char cache_file[256];
extern char table_file[256];
extern void show_version(void);
extern void show_usage(void);
extern int load_entry_parameter(int argc, char **argv);
extern int bmp_daemon_init(int argc, char **argv);
extern int init_daemon(void);
extern void daemon_wait(void);
extern void daemon_cleanup(void);
extern int daemon_get_fd_limit(void);
extern int daemon_set_fd_limit(int maxfd);
extern int init_core_dump(void);
extern unsigned int scan_process_openning_fd(void);
#endif
