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
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <vector>
#include <ctype.h>

#include "stat_attn.h"
#include "stat_client.h"
#include "stat_alarm_reporter.h"
#ifndef LONG_MAX
#define LONG_MAX (long)2147483647
#endif

struct StatAttn
{
	int  attn_id;
	StatClient::Iterator_ info;
	unsigned char hide;
	unsigned char add;
	unsigned char sub_id;
	unsigned char cat;
};

static std::vector<StatAttn> g_attn_info;

int  reload_config_file(StatClient &stat_client, const char *file_name)
{
	/* timestamp checking */
	{
		static time_t time = 0;
		struct stat struct_stat;

		if (stat(file_name, &struct_stat) != 0)
			struct_stat.st_mtime = 0;
		if (struct_stat.st_mtime == time)
			return 0;
		time = struct_stat.st_mtime;
	}

	FILE *fp = fopen(file_name, "r");
	if (fp == NULL)
		return -1;

	// hide all info
	for (unsigned int i = 0; i < g_attn_info.size(); i++)
	{
		if (g_attn_info[i].hide)
			g_attn_info[i].hide = 2;
		else
			g_attn_info[i].hide = 1;
	}

	char buf[128];
	while (fgets(buf, sizeof(buf), fp) != NULL) {

		char *p = buf;
		long id = strtol(p, &p, 10);
		if (id == LONG_MAX)
			continue;

		while (isblank(*p))
			p++;

		int add = 0;
		if (p[0] == '+') {
			add = 1;
			p++;
			if (*p == '=')
				p++;
		} else if (p[0] == '=') {
			p++;
		} else if (p[0] == ':' && p[1] == '=') {
			p += 2;
		}

		long sid = strtol(p, &p, 10);
		if (sid == LONG_MAX)
			continue;

		StatClient::Iterator_ si = stat_client[(unsigned int)sid];
		if (si == NULL)
			continue;

		int sub = 0;
		if (*p == '.') {
			long lsub = strtol(p + 1, &p, 10);
			if (lsub == LONG_MAX)
				continue;
			if (lsub >= 0x10000000)
				continue;
			sub = lsub;
		}
		if (sub < 0)
			continue;
		if (!si->is_sample() && sub > 0)
			continue;
		if (si->is_sample() && (unsigned)sub >= si->count() + 2)
			continue;

		while (isblank(*p))
			p++;
		int cat = SCC_10S;
		if (!strncmp(p, "[job_operation]", 5))
			cat = SC_CUR;
		if (!strncmp(p, "[10s]", 5))
			cat = SCC_10S;
		if (!strncmp(p, "[10m]", 5))
			cat = SCC_10M;
		if (!strncmp(p, "[all]", 5))
			cat = SCC_ALL;

		unsigned int i;
		for (i = 0; i < g_attn_info.size(); i++)
		{
			if (g_attn_info[i]. attn_id == id)
				break;
		}
		if (i == g_attn_info.size()) {
			g_attn_info.resize(i + 1);
			g_attn_info[i].hide = 2;
		}

		if (g_attn_info[i].hide == 2)
			printf("NEW: %d %c %d.%d %d\n", (int)id, add ? '+' : '=', (int)sid, (int)sub, cat);
		g_attn_info[i]. attn_id = id;
		g_attn_info[i].info = si;
		g_attn_info[i].hide = 0;
		g_attn_info[i].sub_id = sub;
		g_attn_info[i].cat = cat;
		g_attn_info[i].add = add;
	}
	fclose(fp);
	for (unsigned int i = 0; i < g_attn_info.size(); i++)
	{
		if (g_attn_info[i].hide == 1)
			printf("DEL: %d %c %d.%d %d\n",
				   (int)g_attn_info[i]. attn_id,
				   g_attn_info[i].add ? '+' : '=',
				   (int)g_attn_info[i].info->id(),
				   (int)g_attn_info[i].sub_id,
				   g_attn_info[i].cat);
	}
	return 0;
}

int report_data(StatClient &stat_client)
{
	return 0;
}

void lock_reporter(StatClient &stat_client)
{
	for (int i = 0; i < 60; i++)
	{
		if (stat_client.get_socket_lock_fd("attn") == 0)
			return;
		if (i == 0)
			fprintf(stderr, "Reporter locked, trying 1 minute.\n");
		sleep(1);
	}
	fprintf(stderr, "Can't lock reporter. Exiting.\n");
	exit(-1);
}

static void term(int signo)
{
	exit(0);
}

int run_reporter(StatClient &stat_client, const char *file_name)
{
	int ppid = getppid();
	signal(SIGTERM, term);
	signal(SIGHUP, SIG_IGN);
	signal(SIGINT, SIG_IGN);

	lock_reporter(stat_client);
	for (; getppid() == ppid; sleep(10))
	{
		ALARM_REPORTER->init_alarm_cfg(std::string(file_name));
		ALARM_REPORTER->report_alarm();
	}
	return 0;
}
