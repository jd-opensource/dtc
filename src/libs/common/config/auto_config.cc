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
#include "auto_config.h"
#include "log/log.h"
#include "config.h"

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
