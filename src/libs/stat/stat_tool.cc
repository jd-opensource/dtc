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
#include <stdio.h>
#include <stdlib.h>
#include <vector>

#include "stat_client.h"
#include "stat_dtc.h"
#include "stat_attn.h"
#include "stat_table_formater.h"
#include "stat_alarm_reporter.h"
#include <daemon/daemon.h>
#include <dtc_global.h>



StatClient stc;
std::vector<StatClient::Iterator_> idset;
StateTableFormater out;
int outfmt = StateTableFormater::FORMAT_ALIGNED;
unsigned char outnoname;
unsigned char rawdata;
unsigned char alldata;
unsigned char nobase;

#if __WORDSIZE >= 64
#define F64 "%ld"
#else
#define F64 "%lld"
#endif

static inline void cell_id(unsigned int id)
{
	out.cell("%u", id);
}

static inline void cell_sample_id(unsigned int id, unsigned int n)
{
	if (outnoname)
		out.cell("%u.%u", id, n);
	else
		out.cell("%u", id);
}

static inline void cell_name(const char *name)
{
	if (outnoname)
		return;
	if (outfmt == StateTableFormater::FORMAT_ALIGNED)
		out.cell("%s:", name);
	else
		out.cell("%s", name);
}

static inline void cell_dummy(void)
{
	if (outfmt == StateTableFormater::FORMAT_ALIGNED)
		out.cell("-");
	else
#if GCC_MAJOR < 3
		out.cell(" ");
#else
		out.cell(NULL);
#endif
}

static inline void cell_base(int64_t v)
{
	if (outnoname)
		return;
	if (outfmt == StateTableFormater::FORMAT_ALIGNED)
		out.cell("count[>=" F64 "]:", v);
	else
		out.cell("count[>=" F64 "]", v);
}

static inline void cell_nbase(int v)
{
	if (outfmt == StateTableFormater::FORMAT_ALIGNED)
		out.cell("[%d]", v);
	else
		out.cell("%d", v);
}

static inline void cell_int(int64_t val)
{
	out.cell(F64, val);
}

static inline void cell_fixed(int64_t val, int n, int div)
{
	const char *sign = "";
	if (val < 0) {
		val = -val;
		sign = "-";
	}

	out.cell("%s" F64 ".%0*d", sign, val / div, n, (int)(val % div));
}

static inline void cell_percent(int64_t val)
{
	out.cell(F64 "%%", val);
}

static inline void cell_percent_fixed(int64_t val, int n, int div)
{
	const char *sign = "";
	if (val < 0) {
		val = -val;
		sign = "-";
	}

	out.cell("%s" F64 ".%0*d%%", sign, val / div, n, (int)(val % div));
}

static inline void cell_hms(int64_t val)
{
	const char *sign = "";
	if (val < 0) {
		val = -val;
		sign = "-";
	}

	if (val < 60)
		out.cell("%s%d", sign, (int)val);
	else if (val < 60 * 60)
		out.cell("%s%d:%02d", sign, (int)(val / 60), (int)(val % 60));
	else
		out.cell("%s" F64 ":%02d:%02d", sign, val / 3600,
			 (int)((val / 60) % 60), (int)(val % 60));
}

static inline void cell_hmsmsec(int64_t val)
{
	const char *sign = "";
	if (val < 0) {
		val = -val;
		sign = "-";
	}
	if (val < 60 * 1000)
		out.cell("%s%d.%03d", sign, (int)(val / 1000),
			 (int)(val % 1000));
	else if (val < 60 * 60 * 1000)
		out.cell("%s%d:%02d.%03d", sign, (int)(val / 60000),
			 (int)((val / 1000) % 60), (int)(val % 1000));
	else
		out.cell("%s" F64 ":%02d:%02d.%03d", sign, val / 3600000,
			 (int)((val / 60000) % 60), (int)((val / 1000) % 60),
			 (int)(val % 1000));
}

static inline void cell_hmsusec(int64_t val)
{
	const char *sign = "";
	if (val < 0) {
		val = -val;
		sign = "-";
	}
	out.cell("%s" F64 ".%06d", sign, val / 1000000, (int)(val % 1000000));
}

static inline void cell_datetime(int64_t v)
{
	if (v == 0) {
		out.cell_v("-");
		return;
	}
	time_t t = v;
	struct tm tm;
	localtime_r(&t, &tm);
	out.cell_v("%d-%d-%d %d:%02d:%02d", tm.tm_year + 1900, tm.tm_mon + 1,
		   tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}

static inline void cell_date(int64_t v)
{
	if (v == 0) {
		out.cell_v("-");
		return;
	}
	time_t t = v;
	struct tm tm;
	localtime_r(&t, &tm);
	out.cell_v("%d-%d-%d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

static inline void cell_version(int64_t v)
{
	out.cell_v("%d.%d.%d", (int)v / 10000, (int)v / 100 % 100,
		   (int)v % 100);
}

static inline void cell_bool(int64_t v)
{
	if (0 == v) {
		out.cell_v("NO");
	} else {
		out.cell_v("YES");
	}
}

inline void row_init(void)
{
	out.new_row();
}

inline void row_clear(void)
{
	out.clear_row();
}

inline void dump_table(void)
{
	out.dump(stdout, outfmt);
}

void cell_value(int unit, int64_t val)
{
	if (rawdata)
		unit = SU_INT;
	switch (unit) {
	default:
	case SU_HIDE:
	case SU_INT:
		cell_int(val);
		break;
	case SU_INT_1:
		cell_fixed(val, 1, 10);
		break;
	case SU_INT_2:
		cell_fixed(val, 2, 100);
		break;
	case SU_INT_3:
		cell_fixed(val, 3, 1000);
		break;
	case SU_INT_4:
		cell_fixed(val, 4, 10000);
		break;
	case SU_INT_5:
		cell_fixed(val, 5, 100000);
		break;
	case SU_INT_6:
		cell_fixed(val, 6, 1000000);
		break;
	case SU_MSEC:
		cell_hmsmsec(val);
		break;
	case SU_USEC:
		cell_hmsusec(val);
		break;
	case SU_TIME:
		cell_hms(val);
		break;
	case SU_DATE:
		cell_date(val);
		break;
	case SU_DATETIME:
		cell_datetime(val);
		break;
	case SU_VERSION:
		cell_version(val);
		break;
	case SU_BOOL:
		cell_bool(val);
		break;
	case SU_PERCENT:
		cell_percent(val);
		break;
	case SU_PERCENT_1:
		cell_percent_fixed(val, 1, 10);
		break;
	case SU_PERCENT_2:
		cell_percent_fixed(val, 2, 100);
		break;
	case SU_PERCENT_3:
		cell_percent_fixed(val, 3, 1000);
		break;
	}
}

void dump_data(void)
{
	int64_t sc[16];
	int sn;

	unsigned int i;
	StatClient::Iterator_ s;

	stc.check_point();

	for (i = 0; i < idset.size(); i++) {
		s = idset[i];

		if (alldata == 0 && s->unit() == SU_HIDE)
			continue;

		row_init();
		switch (s->type()) {
		case SA_SAMPLE:
			cell_id(s->id());
			cell_name(s->name());
			//cell_value(s->unit(), stc.read_sample_average(s, SC_CUR));
			cell_value(s->unit(),
				   stc.read_sample_average(s, SCC_10S));
			cell_value(s->unit(),
				   stc.read_sample_average(s, SCC_10M));
			cell_value(s->unit(),
				   stc.read_sample_average(s, SCC_ALL));

			if (nobase < 2) {
				row_init();
				cell_sample_id(s->id(), 1);
				cell_name("count[all]");
				//cell_value(SU_INT, stc.read_sample_counter(s, SC_CUR));
				cell_value(SU_INT,
					   stc.read_sample_counter(s, SCC_10S));
				cell_value(SU_INT,
					   stc.read_sample_counter(s, SCC_10M));
				cell_value(SU_INT,
					   stc.read_sample_counter(s, SCC_ALL));
			}
			if (nobase == 0) {
				sn = stc.get_count_base(s->id(), sc);
				for (int n = 1; n <= sn; n++) {
					row_init();
					cell_sample_id(s->id(), n + 1);
					cell_base(sc[n - 1]);
					//cell_value(SU_INT, stc.read_sample_counter(s, SC_CUR, n));
					cell_value(SU_INT,
						   stc.read_sample_counter(
							   s, SCC_10S, n));
					cell_value(SU_INT,
						   stc.read_sample_counter(
							   s, SCC_10M, n));
					cell_value(SU_INT,
						   stc.read_sample_counter(
							   s, SCC_ALL, n));
				}
			}

			break;

		case SA_COUNT:
			cell_id(s->id());
			cell_name(s->name());
			//cell_value(s->unit(), stc.read_counter_value(s, SC_CUR));
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_10S));
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_10M));
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_ALL));
			break;
		case SA_VALUE:
			cell_id(s->id());
			cell_name(s->name());
			//cell_value(s->unit(), stc.read_counter_value(s, SC_CUR));
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_10S));
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_10M));
			cell_dummy();
			break;

		case SA_CONST:
			cell_id(s->id());
			cell_name(s->name());
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_10S));
			switch (s->unit()) {
			case SU_DATETIME:
			case SU_DATE:
			case SU_VERSION:
			case SU_BOOL:
				break;
			default:
				//cell_dummy();
				cell_dummy();
				cell_dummy();
				break;
			}
			break;

		case SA_EXPR:
			cell_id(s->id());
			cell_name(s->name());
			cell_dummy();
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_10S));
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_10M));
			cell_value(s->unit(),
				   stc.read_counter_value(s, SCC_ALL));
			break;

		default:
			row_clear();
		}
	}
	dump_table();
}

void dump_base(void)
{
	int64_t sc[16];
	int sn;

	unsigned int i;
	StatClient::Iterator_ s;

	stc.check_point();

	for (i = 0; i < idset.size(); i++) {
		s = idset[i];
		row_init();
		sn = stc.get_count_base(s->id(), sc);
		cell_id(s->id());
		cell_name(s->name());
		cell_nbase(sn);
		for (int n = 0; n < sn; n++)
			cell_value(s->unit(), sc[n]);
	}
	dump_table();
}

void create_files(void)
{
	char buf[256];
	if (g_stat_mgr.create_stat_index("core", STATIDX, g_stat_definition,
					 buf, sizeof(buf)) < 0) {
		fprintf(stderr, "Fail to create stat index file: %s\n", buf);
		exit(-3);
	}
	fprintf(stderr, "stat index created: %s\n", STATIDX);
}

void init(void)
{
	int ret;

	ret = stc.init_stat_info("core", STATIDX);
	if (ret < 0) {
		fprintf(stderr, "Cannot Initialize StatInfo: %s\n",
			stc.get_error_message());
		exit(-1);
	}
}

void parse_stat_id(int argc, char **argv)
{
	StatClient::Iterator_ n;

	if (argc == 0) {
		for (n = stc.get_begin_stat_info();
		     n != stc.get_end_stat_info(); n++)
			idset.push_back(n);
		return;
	}

	for (; argc > 0; argc--, argv++) {
		int s, e;
		switch (sscanf(argv[0], "%d-%d", &s, &e)) {
		case 2:
			for (n = stc.get_begin_stat_info();
			     n != stc.get_end_stat_info(); n++) {
				if ((int)n->id() >= s && (int)n->id() <= e)
					idset.push_back(n);
			}
			break;
		case 1:
			if ((n = stc[s]) != NULL) {
				idset.push_back(n);
				break;
			}
			// fall through
		default:
			fprintf(stderr, "Invalid stat id [%s]\n", argv[0]);
			exit(-4);
		}
	}
}

void parse_sample_id(int argc, char **argv)
{
	StatClient::Iterator_ n;

	if (argc == 0) {
		for (n = stc.get_begin_stat_info();
		     n != stc.get_end_stat_info(); n++)
			if (n->is_sample())
				idset.push_back(n);
		return;
	}

	for (; argc > 0; argc--, argv++) {
		int s, e;
		switch (sscanf(argv[0], "%d-%d", &s, &e)) {
		case 2:
			for (n = stc.get_begin_stat_info();
			     n != stc.get_end_stat_info(); n++) {
				if ((int)n->id() >= s && (int)n->id() <= e &&
				    n->is_sample())
					idset.push_back(n);
			}
			break;
		case 1:
			if ((n = stc[s]) != NULL && n->is_sample()) {
				idset.push_back(n);
				break;
			}
			// fall through
		default:
			fprintf(stderr, "Invalid stat sample id [%s]\n",
				argv[0]);
			exit(-4);
		}
	}
}

void alter_base(int argc, char **argv)
{
	StatClient::Iterator_ n = NULL;
	int64_t sc[16];

	if (argc == 0 || (n = stc[atoi(argv[0])]) == NULL) {
		fprintf(stderr, "A stat sample id required\n");
		exit(-5);
	}
	argv++, argc--;
	if (argc > 16) {
		fprintf(stderr, "number of count base must <= 16\n");
		exit(-5);
	}
	for (int i = 0; i < argc; i++)
		sc[i] = strtoll(argv[i], 0, 0);
	int ret = stc.set_count_base(n->id(), sc, argc);
	if (ret < 0) {
		fprintf(stderr, "setbase failed for id: %d\n", n->id());
		exit(-5);
	}
	idset.push_back(n);
	dump_base();
}

void usage(void)
{
	fprintf(stderr, "Usage: stattool [-nct] cmd [args...]\n"
			"options list:\n"
			"	-a  output hidden id too\n"
			"	-r  output unformatted data\n"
			"	-n  Don't output stat name\n"
			"	-t  use tab seperated format\n"
			"	-c  use [,] seperated format\n"
			"command list:\n"
			"        create\n"
			"        dump [id|id-id]...\n"
			"        getbase [id|id-id]...\n"
			"        setbase id v1 v2...\n");
	exit(-2);
}
#if 1
int main(int argc, char **argv)
{
	argv++, --argc;

	while (argc > 0 && argv[0][0] == '-') {
		const char *p = argv[0];
		char c;
		while ((c = *++p)) {
			switch (c) {
			case 'a':
				alldata = 1;
				break;
			case 'b':
				nobase++;
				break;
			case 'r':
				rawdata = 1;
				break;
			case 't':
				outfmt = StateTableFormater::FORMAT_TABBED;
				break;
			case 'c':
				outfmt = StateTableFormater::FORMAT_COMMA;
				break;
			case 'n':
				outnoname = 1;
				break;
			case 'h':
			case '?':
			default:
				fprintf(stderr, "Unknown options [%c]\n", c);
				usage();
			}
		}
		argv++, --argc;
	}

	if (argc <= 0)
		usage();
	else if (!strcasecmp(argv[0], "help"))
		usage();
	else if (!strcasecmp(argv[0], "create")) {
		create_files();
	} else if (!strcasecmp(argv[0], "dump")) {
		init();
		parse_stat_id(--argc, ++argv);
		dump_data();
	} else if (!strcasecmp(argv[0], "getbase")) {
		init();
		parse_sample_id(--argc, ++argv);
		dump_base();
	} else if (!strcasecmp(argv[0], "setbase")) {
		init();
		alter_base(--argc, ++argv);
	} else if (!strcasecmp(argv[0], "reporter")) {
		if (load_entry_parameter(argc, argv) < 0)
			return -1;

		init();
		ALARM_REPORTER->set_stat_client(&stc);
		ALARM_REPORTER->set_time_out(5);
		run_reporter(stc, argv[1] ?: ALARM_CONF_FILE);
	} else
		usage();
	return 0;
}
#endif