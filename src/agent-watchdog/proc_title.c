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
#include <mem_check.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/prctl.h>

#ifndef PR_SET_NAME
#define PR_SET_NAME 15
#endif

#ifndef PR_GET_NAME
#define PR_GET_NAME 16
#endif

static char *arg_start;
static char *arg_end;
static char *env_start;
extern char **environ;

// Initialize process name
void init_proc_title(int argc, char **argv)
{
	int i;
	arg_start = argv[0]; 			// Program address
	arg_end = argv[argc - 1] + strlen(argv[argc - 1]) + 1;
	env_start = environ[0];
	for (i = 0; i < argc; ++i)
		argv[i] = STRDUP(argv[i]);   // Allow manipulation of argv space
}

void set_proc_title(const char *title)
{
	if(title == NULL)
		return ;
		
	int tlen = strlen(title) + 1;
	int i;
	char *p;

	if (arg_start == NULL)
		return;
	if (arg_end - arg_start < tlen && env_start == arg_end)
	{
		char *env_end = env_start;
		for (i = 0; environ[i]; i++)
		{
			if (env_end == environ[i])
			{
				env_end = environ[i] + strlen(environ[i]) + 1;
				environ[i] = STRDUP(environ[i]);
			}
			else
				break;
		}
		arg_end = env_end;
		env_start = NULL;
	}
	i = arg_end - arg_start;
	if (tlen == i)
	{
		strncpy(arg_start, title, i);
	}
	else if (tlen < i)
	{
		strncpy(arg_start, title, i);
		memset(arg_start + tlen, 0, i - tlen);
	}
	else
	{
		*(char *)mempcpy(arg_start, title, i - 1) = '\0';
	}
	if (env_start)
	{
		p = strchr(arg_start, ' ');
		if (p)
			*p = '\0';
	}
}

void set_proc_name(const char *name)
{
	prctl(PR_SET_NAME, name);
}

void get_proc_name(char *name)
{
	prctl(PR_GET_NAME, name);
}
