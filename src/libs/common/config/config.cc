/*
* Copyright [2021] JD.com, Inc.
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
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/mman.h>

#include "mem_check.h"
#include "config.h"
#include "log/log.h"

int str2int(const char *strval, int def)
{
	int ret_code = def;
	if (isdigit(strval[0]) || (strval[0] == '-' && isdigit(strval[1])))
		return atoi(strval);

	if (!strcasecmp(strval, "On"))
		ret_code = 1;
	else if (!strcasecmp(strval, "Off"))
		ret_code = 0;
	else if (!strcasecmp(strval, "Yes"))
		ret_code = 1;
	else if (!strcasecmp(strval, "No"))
		ret_code = 0;
	else if (!strcasecmp(strval, "True"))
		ret_code = 1;
	else if (!strcasecmp(strval, "False"))
		ret_code = 0;
	else if (!strcasecmp(strval, "Enable"))
		ret_code = 1;
	else if (!strcasecmp(strval, "Disable"))
		ret_code = 0;
	else if (!strcasecmp(strval, "Enabled"))
		ret_code = 1;
	else if (!strcasecmp(strval, "Disabled"))
		ret_code = 0;

	return ret_code;
}

int DTCConfig::Dump(const char *fn, bool dec)
{
	FILE *fp = fopen(fn, "w");
	if (fp == NULL)
		return -1;
	//fprintf(fp, "##### ORIGINAL FILE %s #####\n", filename_.c_str());
	for (YAML::const_iterator ite = table_config_.begin();
	     ite != table_config_.end(); ++ite) {
		YAML::Node inception_sec = ite->first;
		std::string sec = inception_sec.as<std::string>();
		fprintf(fp, "%s:\n", sec.c_str());
		YAML::Node key_map = ite->second;
		for (YAML::const_iterator ite_map = key_map.begin();
		     ite_map != key_map.end(); ++ite_map) {
			YAML::Node key = ite_map->first;
			YAML::Node value = ite_map->second;
			std::string now_key = key.as<std::string>();
			std::string now_value = value.as<std::string>();
			if (dec == true)
				fprintf(fp, "# %s NOT SET\n", now_key.c_str());
			if (dec == false)
				fprintf(fp, "   %s: %s\n", now_key.c_str(),
					now_value.c_str());
		}
	}
	fclose(fp);
	return 0;
}

int DTCConfig::parse_buffered_config(char *buf, const char *fn,
				     const char *defsec, bool bakconfig)
{
	int ret_code = -1;
	try {
		printf("open config file:%s\n", fn);
		if (defsec && strcmp(defsec, "cache") == 0) {
			cache_config_ = YAML::Load(buf);
		} else {
			table_config_ = YAML::Load(buf);
		}
		ret_code = 0;
	} catch (const YAML::Exception &e) {
		printf("config file error:%s\n", e.what());
		return ret_code;
	}
	return;
}

int DTCConfig::parse_config(const char *fn, const char *defsec, bool bakconfig)
{
	int ret_code = -1;

	try {
		printf("open config file:%s\n", fn);
		if (strcmp(defsec, "cache") == 0) {
			cache_config_ = YAML::LoadFile(fn);
		} else {
			table_config_ = YAML::LoadFile(fn);
		}
		ret_code = 0;
	} catch (const YAML::Exception &e) {
		printf("config file error:%s\n", e.what());
		return ret_code;
	}

	if (bakconfig) {
		char bak_config[1024];
		int err = 0;
		system("mkdir -p /usr/local/dtc/stat/");
		snprintf(bak_config, sizeof(bak_config),
			 "cp %s /usr/local/dtc/stat/", fn);
		if (err == 0)
			err = system(bak_config);
	}
	return ret_code;
}

bool DTCConfig::has_section(const char *sec)
{
	YAML::Node inception = table_config_[sec];
	if (inception && inception.IsMap())
		return true;
	else
		return false;
}

bool DTCConfig::has_key(const char *sec, const char *key)
{
	if (!table_config_[sec])
		return false;

	YAML::Node inception = table_config_[sec][key];
	if (inception && inception.IsScalar())
		return true;
	else
		return false;
}

/********************************************
 * eg：get_str_val("cache", "RemoteLogAddr")
 * 读取 conf/cache.conf中RemoteLogAdd的参数
 * ******************************************/
const char *DTCConfig::get_str_val(const char *sec, const char *key)
{
	const char *val = NULL;

	if (strcmp(sec, "cache") == 0) {
		if (cache_config_[sec]) {
			if (cache_config_[sec][key]) {
				std::string result = cache_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	} else {
		if (table_config_[sec]) {
			if (table_config_[sec][key]) {
				std::string result = table_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	}

	if (val == NULL)
		return NULL;

	return val;
}

int DTCConfig::get_int_val(const char *sec, const char *key, int def)
{
	const char *val = NULL;
	if (strcmp(sec, "cache") == 0) {
		if (cache_config_[sec]) {
			if (cache_config_[sec][key]) {
				std::string result = cache_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	} else {
		if (table_config_[sec]) {
			if (table_config_[sec][key]) {
				std::string result = table_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	}

	if (val == NULL)
		return def;

	return str2int(val, def);
}

unsigned long long DTCConfig::get_size_val(const char *sec, const char *key,
					   unsigned long long def, char unit)
{
	const char *val = NULL;
	if (strcmp(sec, "cache") == 0) {
		if (cache_config_[sec]) {
			if (cache_config_[sec][key]) {
				std::string result = cache_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	} else {
		if (table_config_[sec]) {
			if (table_config_[sec][key]) {
				std::string result = table_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	}

	if (val == NULL || !isdigit(val[0]))
		return def;

	const char *p;
	double a = strtod(val, (char **)&p);
	if (*p)
		unit = *p;
	switch (unit) {
	case 'b':
		break;
	case 'B':
		break;
	case 'k':
		a *= 1000;
		break;
	case 'K':
		a *= 1 << 10;
		break;
	case 'm':
		a *= 1000000;
		break;
	case 'M':
		a *= 1 << 20;
		break;
	case 'g':
		a *= 1000000000;
		break;
	case 'G':
		a *= 1 << 30;
		break;
	case 't':
		a *= 1000000000000LL;
		break;
	case 'T':
		a *= 1ULL << 40;
		break;
	}

	return (unsigned long long)a;
}

int DTCConfig::get_idx_val(const char *sec, const char *key,
			   const char *const *array, int nDefault)
{
	const char *val = NULL;
	if (strcmp(sec, "cache") == 0) {
		if (cache_config_[sec]) {
			if (cache_config_[sec][key]) {
				std::string result = cache_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	} else {
		if (table_config_[sec]) {
			if (table_config_[sec][key]) {
				std::string result = table_config_[sec][key]
							     .as<std::string>();
				if (result.length() > 0) {
					val = result.c_str();
				}
			}
		}
	}
	if (val == NULL)
		return nDefault;

	if (isdigit(val[0])) {
		char *p;
		int n = strtol(val, &p, 0);
		if (*p == '\0') {
			for (int i = 0; array[i]; ++i) {
				if (n == i)
					return i;
			}
		}
	}

	for (int i = 0; array[i]; ++i) {
		if (!strcasecmp(val, array[i]))
			return i;
	}
	return -1;
}

class AutoConfigSection : public AutoConfig {
    private:
	static pthread_mutex_t glock;
	void GlobalLock(void)
	{
		pthread_mutex_lock(&glock);
	}
	void GlobalUnlock(void)
	{
		pthread_mutex_unlock(&glock);
	}

    private:
	DTCConfig *parent;
	char *section;
	// buf must have enough room place composed key name
	char buf[256];
	char *last;

    public:
	AutoConfigSection(DTCConfig *p, const char *sec);
	~AutoConfigSection();

	virtual int get_int_val(const char *key, const char *inst, int def = 0);
	virtual unsigned long long get_size_val(const char *key,
						const char *inst,
						unsigned long long def = 0,
						char unit = 0);
	virtual int get_idx_val(const char *, const char *, const char *const *,
				int = 0);
	virtual const char *get_str_val(const char *key, const char *inst);

    private:
	// return composed key, or vanilla key, always non-null
	const char *findkey(const char *key, const char *inst);
	// strip suffix digits
	int stripnumber(void);
	// strip suffix alphaphetic
	int stripalpha(void);
	// strip suffix punct
	int strippunct(void);
};

pthread_mutex_t AutoConfigSection::glock = PTHREAD_MUTEX_INITIALIZER;

AutoConfigSection::AutoConfigSection(DTCConfig *p, const char *sec)
{
	this->parent = p;
	this->section = STRDUP(sec);
}

AutoConfigSection::~AutoConfigSection()
{
	FREE_CLEAR(section);
}

int AutoConfigSection::stripnumber(void)
{
	int n = 0;
	while (last >= buf && isdigit(*last)) {
		last--;
		n++;
	}
	last[1] = 0;
	strippunct();
	return n;
}

int AutoConfigSection::stripalpha(void)
{
	int n = 0;
	while (last >= buf && isalpha(*last)) {
		last--;
		n++;
	}
	last[1] = 0;
	strippunct();
	return n;
}

int AutoConfigSection::strippunct(void)
{
	int n = 0;
	while (last >= buf && *last != '@' && !isalnum(*last)) {
		last--;
		n++;
	}
	last[1] = 0;
	return n;
}

const char *AutoConfigSection::findkey(const char *key, const char *inst)
{
	snprintf(buf, sizeof(buf), "%s@%s", key, inst);
	last = buf + strlen(buf) - 1;
	strippunct();

	do {
		if (parent->get_str_val(section, buf) != NULL) {
			return buf;
		}
	} while (isdigit(*last) ? stripnumber() : stripalpha());

	return key;
}

int AutoConfigSection::get_int_val(const char *key, const char *inst, int def)
{
	int ret;
	GlobalLock();
	ret = parent->get_int_val(section, findkey(key, inst), def);
	GlobalUnlock();
	return ret;
}
unsigned long long AutoConfigSection::get_size_val(const char *key,
						   const char *inst,
						   unsigned long long def,
						   char unit)
{
	unsigned long long ret;
	GlobalLock();
	ret = parent->get_size_val(section, findkey(key, inst), def, unit);
	GlobalUnlock();
	return ret;
}
int AutoConfigSection::get_idx_val(const char *key, const char *inst,
				   const char *const *idxval, int def)
{
	int ret;
	GlobalLock();
	ret = parent->get_idx_val(section, findkey(key, inst), idxval, def);
	GlobalUnlock();
	return ret;
}
const char *AutoConfigSection::get_str_val(const char *key, const char *inst)
{
	const char *ret;
	GlobalLock();
	ret = parent->get_str_val(section, findkey(key, inst));
	GlobalUnlock();
	return ret;
}

AutoConfig *DTCConfig::get_auto_config_instance(const char *section)
{
	AutoConfigSection *inst;
	NEW(AutoConfigSection(this, section), inst);
	return inst;
}
