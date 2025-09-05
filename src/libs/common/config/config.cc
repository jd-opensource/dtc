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
    for (YAML::const_iterator ite = dtc_config.begin();
         ite != dtc_config.end(); ++ite) {
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

int DTCConfig::load_yaml_buffer(char *buf)
{
    int ret_code = -1;

    if(!buf)
    {
        log4cplus_error("yaml content don't allow null.");
        return ret_code;
    }
    log4cplus_debug("yaml buf:%s", buf);
    try {
        dtc_config = YAML::Load(buf);
        ret_code = 0;
    } catch (const YAML::Exception &e) {
        printf("load yaml buf error:%s\n", e.what());
        log4cplus_debug("load yaml buf error:%s\n", e.what());
        return ret_code;
    }
    return ret_code;
}

int DTCConfig::load_yaml_file(const char *fn, bool bakconfig)
{
    int ret_code = -1;

    try {
        printf("open config file:%s\n", fn);
        dtc_config = YAML::LoadFile(fn);
        ret_code = 0;
    } catch (const YAML::Exception &e) {
        printf("config file error:%s\n", e.what());
        return ret_code;
    }

    if (bakconfig) {
        char bak_config[1024];
        int err = 0;
        if (system("mkdir -p ../stat/") == -1) {
            // Handle mkdir error if needed
        }
        snprintf(bak_config, sizeof(bak_config),
             "cp %s ../stat/", fn);
        if (err == 0)
            err = system(bak_config);
    }
    return ret_code;
}

const char *DTCConfig::get_str_val(const char *sec, const char *key)
{
    return NULL;
}

int DTCConfig::get_int_val(const char *sec, const char *key, int def)
{
    return def;
}

unsigned long long DTCConfig::conv_size_val(const char* val, int ndefault, char unit)
{
    if (val == NULL)
    {
        log4cplus_error("val is null");
        return ndefault;
    }
        

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

unsigned long long DTCConfig::get_size_val(const char *sec, const char *key,
                       unsigned long long def, char unit)
{
    return def;
}

int DTCConfig::get_idx_val(const char *sec, const char *key,
               const char *const *array, int nDefault)
{
    return nDefault;
}
