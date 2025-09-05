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
#ifndef __AUTO_CONFIG_H__
#define __AUTO_CONFIG_H__

#include <stdio.h>
#include <string.h>
#include <string>
#include <map>
#include "yaml-cpp/yaml.h"

class AutoConfig {
    public:
	AutoConfig(){};
	virtual ~AutoConfig(){};
	virtual int get_int_val(const char *key, const char *inst,
				int def = 0) = 0;
	virtual unsigned long long get_size_val(const char *key,
						const char *inst,
						unsigned long long def = 0,
						char unit = 0) = 0;
	virtual int get_idx_val(const char *, const char *, const char *const *,
				int = 0) = 0;
	virtual const char *get_str_val(const char *key, const char *inst) = 0;
};

#endif
