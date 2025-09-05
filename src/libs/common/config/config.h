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
#ifndef __CONFIG_H__
#define __CONFIG_H__

#include <stdio.h>
#include <string.h>
#include <string>
#include <map>
#include "auto_config.h"
#include "yaml-cpp/yaml.h"

int str2int(const char *strval, int def);

class DTCConfig {
    public:
	DTCConfig(){};
	~DTCConfig(){};

	AutoConfig *get_auto_config_instance(const char *);
	int get_int_val(const char *sec, const char *key, int def = 0);
	unsigned long long get_size_val(const char *sec, const char *key,
					unsigned long long def = 0,
					char unit = 0);
	int get_idx_val(const char *, const char *, const char *const *,
			int = 0);
	const char *get_str_val(const char *sec, const char *key);
	unsigned long long conv_size_val(const char* val, int ndefault, char unit);

	int Dump(const char *fn, bool dec = false);
	int load_yaml_file(const char *f = 0, bool bakconfig = false);
	int load_yaml_buffer(char *buf);

	YAML::Node get_config_node() { return dtc_config;}

    private:
	YAML::Node dtc_config;
};

#endif
