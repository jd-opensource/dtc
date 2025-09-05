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
#ifndef __CH_SECTION_H__
#define __CH_SECTION_H__

#include "mem_check.h"
#include <string.h>
#include <math.h>
#include <errno.h>
#include <new>

#include "value.h"
#include "protocol.h"
#include "log/log.h"

#define MAX_STATIC_SECTION 20

/*
 * tag: a pair of id:value, the value type if predefined
 * section: a set of id:value
 */
struct SectionDefinition {
	uint8_t sectionId;
	uint8_t maxTags;
	uint8_t tagType[256];

	uint8_t tag_type(uint8_t i) const
	{
		return tagType[i];
	};
	int max_tags(void) const
	{
		return maxTags;
	}
	int section_id(void) const
	{
		return sectionId;
	}
};

class SimpleSection {
    private:
	const SectionDefinition *definition;
	uint8_t numTags;
#if MAX_STATIC_SECTION
	uint8_t fieldMask[(MAX_STATIC_SECTION + 7) / 8];
#else
	uint8_t fieldMask[32];
#endif

    public:
#if MAX_STATIC_SECTION
	DTCValue tagValue[MAX_STATIC_SECTION];
#else
	DTCValue *tagValue;
#endif

	SimpleSection(const SectionDefinition &d) : definition(&d), numTags(0)
	{
#if MAX_STATIC_SECTION
		for (int i = 0; i < d.max_tags(); i++)
			tagValue[i].Set(NULL, 0);
#else
		tagValue = (DTCValue *)calloc(d.max_tags(), sizeof(DTCValue));
		if (tagValue == NULL)
			throw std::bad_alloc();
#endif
		FIELD_ZERO(fieldMask);
	}

	virtual ~SimpleSection()
	{
#if !MAX_STATIC_SECTION
		FREE_IF(tagValue);
#endif
	}
	SimpleSection(const SimpleSection &orig)
	{
#if !MAX_STATIC_SECTION
		tagValue = (DTCValue *)malloc(orig.definition->max_tags(),
					      sizeof(DTCValue));
		if (tagValue == NULL)
			throw std::bad_alloc();
#endif
		Copy(orig);
	}
	void Copy(const SimpleSection &orig)
	{
		for (int i = 0; i < orig.definition->max_tags(); i++)
			tagValue[i] = orig.tagValue[i];
		memcpy(fieldMask, orig.fieldMask, sizeof(fieldMask));
		definition = orig.definition;
		numTags = orig.numTags;
	}

	inline virtual void Clean()
	{
		memset(tagValue, 0, sizeof(DTCValue) * definition->max_tags());
		FIELD_ZERO(fieldMask);
		numTags = 0;
	}

	int max_tags(void) const
	{
		return definition->max_tags();
	}

	int num_tags(void) const
	{
		return numTags;
	}

	int section_id(void) const
	{
		return definition->section_id();
	}

	uint8_t tag_type(uint8_t id) const
	{
		return definition->tag_type(id);
	}

	int tag_present(uint8_t id) const
	{
		return id >= MAX_STATIC_SECTION ? 0 :
						  FIELD_ISSET(id, fieldMask);
	}

	void set_tag(uint8_t id, const DTCValue &val)
	{
		if (tag_type(id) != DField::None) {
			tagValue[id] = val;
			if (!FIELD_ISSET(id, fieldMask)) {
				numTags++;
				FIELD_SET(id, fieldMask);
			}
		}
	}

	/* no check of dumplicate tag */
	void SetTagMask(uint8_t id)
	{
		numTags++;
		FIELD_SET(id, fieldMask);
	}

	void set_tag(uint8_t id, int64_t val)
	{
		set_tag(id, DTCValue::Make(val));
	}
	void set_tag(uint8_t id, uint64_t val)
	{
		set_tag(id, DTCValue::Make(val));
	}
	void set_tag(uint8_t id, int32_t val)
	{
		if (tag_type(id) == DField::Signed)
			set_tag(id, DTCValue::Make(val));
		else if (tag_type(id) == DField::Unsigned) {
			if (val < 0)
				val = 0;
			set_tag(id, DTCValue::Make((uint64_t)val));
		}
	}
	void set_tag(uint8_t id, uint32_t val)
	{
		// Fix Unsigned to Signed
		if (tag_type(id) == DField::Signed)
			set_tag(id, DTCValue::Make((int64_t)val));
		else if (tag_type(id) == DField::Unsigned)
			set_tag(id, DTCValue::Make(val));
	}
	void set_tag(uint8_t id, double val)
	{
		if (tag_type(id) == DField::Float)
			set_tag(id, DTCValue::Make(val));
	}
	void set_tag(uint8_t id, const char *val, int len)
	{
		if (tag_type(id) == DField::String ||
		    tag_type(id) == DField::Binary)
			set_tag(id, DTCValue::Make(val, len));
	}
	void set_tag(uint8_t id, const char *val)
	{
		if (tag_type(id) == DField::String ||
		    tag_type(id) == DField::Binary)
			set_tag(id, DTCValue::Make(val));
	}

	const DTCValue *get_tag(uint8_t id) const
	{
		return tag_present(id) ? &tagValue[id] : NULL;
	}

	/* no check */
	DTCValue *get_this_tag(uint8_t id)
	{
		return &tagValue[id];
	}

	const DTCValue *key(void) const
	{
		return tag_present(0) ? &tagValue[0] : NULL;
	}
	void set_key(const DTCValue &k)
	{
		set_tag(0, k);
	}
};

extern const SectionDefinition versionInfoDefinition;
extern const SectionDefinition requestInfoDefinition;
extern const SectionDefinition resultInfoDefinition;

class DTCVersionInfo : public SimpleSection {
    public:
	DTCVersionInfo() : SimpleSection(versionInfoDefinition)
	{
	}
	~DTCVersionInfo()
	{
	}
	DTCVersionInfo(const DTCVersionInfo &orig) : SimpleSection(orig)
	{
	}

	const DTCBinary &table_name(void) const
	{
		return tagValue[1].str;
	}
	void set_table_name(const char *n)
	{
		set_tag(1, n);
	}
	const DTCBinary &table_hash(void) const
	{
		return tagValue[4].str;
	}
	void set_table_hash(const char *n)
	{
		set_tag(4, n, 16);
	}
	const DTCBinary &data_table_hash(void) const
	{
		return tagValue[2].str;
	}
	void set_data_table_hash(const char *n)
	{
		set_tag(2, n, 16);
	}

	const DTCBinary &CTLibVer(void) const
	{
		return tagValue[6].str;
	}
	const int CTLibIntVer(void) const
	{
		int major = 3; //, minor=0, micro=0;

		/* 3.x series batch unpacking has no version information */
		if (NULL == CTLibVer().ptr) {
			log4cplus_debug("multi job have no version info");
			return major;
		}

		char buf = CTLibVer().ptr[7];
		if (buf >= '1' && buf <= '9') {
			major = buf - '0';
			log4cplus_debug(
				"client major version:%d,version string:%s",
				major, CTLibVer().ptr);
			return major;
		} else {
			log4cplus_debug("unknown client api version: %s",
					CTLibVer().ptr);
			return major;
		}
	}

	int ReConnect(void) const
	{
		return tagValue[19].u64;
	}
	int key_type(void) const
	{
		return tagValue[9].u64;
	}
	void set_key_type(int n)
	{
		set_tag(9, n);
	}
#if MAX_STATIC_SECTION >= 1 && MAX_STATIC_SECTION < 10
#error MAX_STATIC_SECTION must >= 10
#endif

	const uint64_t serial_nr(void) const
	{
		return tagValue[3].u64;
	}
	void set_serial_nr(uint64_t u)
	{
		set_tag(3, u);
	}
	const int keep_alive_timeout(void) const
	{
		return (int)tagValue[8].u64;
	}
	void set_keep_alive_timeout(uint32_t u)
	{
		set_tag(8, u);
	}

	void set_hot_backup_id(uint64_t u)
	{
		set_tag(14, u);
	}
	uint64_t hot_backup_id() const
	{
		return tagValue[14].u64;
	}
	void set_master_hb_timestamp(int64_t t)
	{
		set_tag(15, t);
	}
	int64_t master_hb_timestamp(void) const
	{
		return tagValue[15].s64;
	}
	void set_slave_hb_timestamp(int64_t t)
	{
		set_tag(16, t);
	}
	int64_t slave_hb_timestamp(void) const
	{
		return tagValue[16].s64;
	}
	uint64_t get_agent_client_id() const
	{
		return tagValue[17].u64;
	}
	void set_agent_client_id(uint64_t id)
	{
		set_tag(17, id);
	}

	void set_access_key(const char *token)
	{
		set_tag(18, token);
	}
	const DTCBinary &access_key(void) const
	{
		return tagValue[18].str;
	}
};

class DTCRequestInfo : public SimpleSection {
    public:
	DTCRequestInfo() : SimpleSection(requestInfoDefinition)
	{
	}
	~DTCRequestInfo()
	{
	}
	DTCRequestInfo(const DTCRequestInfo &orig) : SimpleSection(orig)
	{
	}

	uint64_t get_expire_time(int version) const
	{
		/* Server processes all timeouts in ms units internally */
		if (version >= 3)
			/* 3.x series client sends timeout in us units */
			return tagValue[1].u64 >> 10;
		else
			/* 2.x series client sends timeout in ms units */
			return tagValue[1].u64;
	}
	void set_timeout(uint32_t n)
	{
		set_tag(1, (uint64_t)n << 10);
	}
	uint32_t limit_start(void) const
	{
		return tagValue[2].u64;
	}
	uint32_t limit_count(void) const
	{
		return tagValue[3].u64;
	}
	void set_limit_start(uint32_t n)
	{
		set_tag(2, (uint64_t)n);
	}
	void set_limit_count(uint32_t n)
	{
		set_tag(3, (uint64_t)n);
	}

	uint32_t admin_code() const
	{
		return tagValue[7].u64;
	}
	void set_admin_code(uint32_t code)
	{
		set_tag(7, (uint64_t)code);
	}
};

class DTCResultInfo : public SimpleSection {
    private:
	char *szErrMsg;
	char *s_info;
	char *t_info;

    public:
	DTCResultInfo()
		: SimpleSection(resultInfoDefinition), szErrMsg(NULL),
		  s_info(NULL), t_info(NULL)
	{
	}
	~DTCResultInfo()
	{
		FREE_CLEAR(szErrMsg);
		FREE_CLEAR(s_info);
		FREE_CLEAR(t_info);
	}
	DTCResultInfo(const DTCResultInfo &orig) : SimpleSection(orig)
	{
		if (orig.szErrMsg) {
			szErrMsg = STRDUP(orig.szErrMsg);
			set_tag(3, szErrMsg, strlen(szErrMsg));
		}
		if (orig.s_info) {
			s_info = NULL;
			set_server_info((DTCServerInfo *)orig.s_info);
		}
		if (orig.t_info) {
			t_info = NULL;
			set_time_info((DTCTimeInfo *)orig.t_info);
		}
	}

	inline virtual void Clean()
	{
		SimpleSection::Clean();
		FREE_CLEAR(szErrMsg);
		FREE_CLEAR(s_info);
		FREE_CLEAR(t_info);
	}
	void Copy(const DTCResultInfo &orig)
	{
		SimpleSection::Copy(orig);
		FREE_CLEAR(szErrMsg);
		if (orig.szErrMsg) {
			szErrMsg = STRDUP(orig.szErrMsg);
			set_tag(3, szErrMsg, strlen(szErrMsg));
		}
		FREE_CLEAR(s_info);
		if (orig.s_info) {
			set_server_info((DTCServerInfo *)orig.s_info);
		}
		FREE_CLEAR(t_info);
		if (orig.t_info) {
			set_time_info((DTCTimeInfo *)orig.t_info);
		}
	}

	void set_error(int err, const char *from, const char *msg)
	{
		set_tag(1, err);
		if (from)
			set_tag(2, (char *)from, strlen(from));
		if (msg)
			set_tag(3, (char *)msg, strlen(msg));
	}
	void set_error_dup(int err, const char *from, const char *msg)
	{
		FREE_IF(szErrMsg);
		szErrMsg = STRDUP(msg);
		set_tag(1, err);
		if (from)
			set_tag(2, (char *)from, strlen(from));
		if (msg)
			set_tag(3, szErrMsg, strlen(szErrMsg));
	}
	int result_code(void) const
	{
		return tagValue[1].s64;
	}
	const char *error_from(void) const
	{
		return tagValue[2].str.ptr;
	}
	const char *error_message(void) const
	{
		return tagValue[3].str.ptr;
	}
	uint64_t affected_rows(void) const
	{
		return tagValue[4].s64;
	}
	void set_affected_rows(uint64_t nr)
	{
		set_tag(4, nr);
	}
	uint64_t total_rows(void) const
	{
		return tagValue[5].s64;
	}
	void set_total_rows(uint64_t nr)
	{
		set_tag(5, nr);
	}
	uint64_t insert_id(void) const
	{
		return tagValue[6].u64;
	}
	void set_insert_id(uint64_t nr)
	{
		set_tag(6, nr);
	}
	char *server_info(void) const
	{
		if (tagValue[7].str.len != sizeof(DTCServerInfo))
			return NULL;
		return tagValue[7].str.ptr;
	}

	void set_server_info(DTCServerInfo *si)
	{
		FREE_IF(s_info);
		int len = sizeof(DTCServerInfo);
		s_info = (char *)CALLOC(1, len);
		memcpy(s_info, si, len);
		set_tag(7, s_info, len);
		return;
	}

	char *time_info(void) const
	{
		if (tagValue[7].str.len != sizeof(DTCTimeInfo))
			return NULL;
		return tagValue[7].str.ptr;
	}

	void set_time_info(DTCTimeInfo *ti)
	{
		FREE_IF(t_info);
		int len = sizeof(DTCTimeInfo);
		t_info = (char *)CALLOC(1, len);
		memcpy(t_info, ti, len);
		set_tag(7, t_info, len);
		return;
	}

	uint32_t Timestamp(void) const
	{
		return tagValue[8].s64;
	}
	void set_time_info(uint32_t nr)
	{
		set_tag(8, nr);
	}
	/*set hit flag by tomchen 20140604*/
	void set_hit_flag(uint32_t nr)
	{
		set_tag(9, nr);
	}
	uint32_t hit_flag() const
	{
		return tagValue[9].s64;
	};

	/* When a request has multiple keys, statistics are needed for business hit rate and counting hit rate for different keys in this request */
	/* In the hit rate field, the first 16 bits store business hit rate, the last 16 bits store technical hit rate */
	uint32_t get_tech_hit_num()
	{
		uint32_t uHitFlag = (uint32_t)hit_flag();
		uint32_t uTaskTechHitNum = uHitFlag & 0xFFFF;
		return uTaskTechHitNum;
	}

	uint32_t get_business_hit_num()
	{
		uint32_t uHitFlag = (uint32_t)hit_flag();
		uint32_t uTashBusinessHitNum = (uHitFlag & 0xFFFF0000) >> 16;
		return uTashBusinessHitNum;
	}

	void incr_tech_hit_num()
	{
		uint32_t uHitFlag = (uint32_t)hit_flag();
		uint32_t uTaskTechHitNum = uHitFlag & 0xFFFF;
		uTaskTechHitNum++;
		uHitFlag = uHitFlag & 0xFFFF0000;
		uHitFlag = uHitFlag | uTaskTechHitNum;
		set_hit_flag(uHitFlag);
	}

	void incr_bussiness_hit_num()
	{
		uint32_t uHitFlag = (uint32_t)hit_flag();
		uint32_t uTashBusinessHitNum = (uHitFlag & 0xFFFF0000) >> 16;
		uTashBusinessHitNum++;
		uTashBusinessHitNum = uTashBusinessHitNum << 16;
		uHitFlag = uHitFlag & 0xFFFF;
		uHitFlag = uHitFlag | uTashBusinessHitNum;
		set_hit_flag(uHitFlag);
	}
};

#endif
