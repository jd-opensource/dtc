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
#include "../log/log.h"
#include "config/dbconfig.h"
#include "../table/table_def.h"

DTCTableDefinition *DbConfig::build_table_definition(void)
{
	DTCTableDefinition *tdef = new DTCTableDefinition(fieldCnt);
	log4cplus_debug("build table tblName:%s", tblName);
	tdef->set_table_name(tblName);
	for (int i = 0; i < fieldCnt; i++) {
		if (tdef->add_field(i, field[i].name, field[i].type,
				    field[i].size) != 0) {
			log4cplus_error(
				"add_field failed, name=%s, size=%d, type=%d",
				field[i].name, field[i].size, field[i].type);
			delete tdef;
			return NULL;
		}
		tdef->set_default_value(i, &field[i].dval);
		if (field[i].flags & DB_FIELD_FLAGS_HAS_DEFAULT) {
			tdef->mark_as_has_default(i);}
		if (field[i].flags & DB_FIELD_FLAGS_NULLABLE) {
			tdef->mark_as_nullable(i);}
		if ((field[i].flags & DB_FIELD_FLAGS_READONLY))
			tdef->mark_as_read_only(i);
		if ((field[i].flags & DB_FIELD_FLAGS_VOLATILE))
			tdef->mark_as_volatile(i);
		if ((field[i].flags & DB_FIELD_FLAGS_DISCARD))
			tdef->mark_as_discard(i);
		if ((field[i].flags & DB_FIELD_FLAGS_UNIQ))
			tdef->mark_uniq_field(i);
		if ((field[i].flags & DB_FIELD_FLAGS_DESC_ORDER)) {
			tdef->mark_order_desc(i);
			if (tdef->is_desc_order(i))
				log4cplus_debug(
					"success set field[%d] desc order", i);
			else
				log4cplus_debug("set field[%d] desc roder fail",
						i);
		}
	}
	if (autoinc >= 0)
		tdef->set_auto_increment(autoinc);
	if (lastacc >= 0)
		tdef->set_lastacc(lastacc);
	if (lastmod >= 0)
		tdef->set_lastmod(lastmod);
	if (lastcmod >= 0)
		tdef->set_lastcmod(lastcmod);
	if (compressflag >= 0)
		tdef->set_compress_flag(compressflag);
	if (expireTime >= 0)
		tdef->set_expire_time(expireTime);

	if (tdef->set_key_fields(keyFieldCnt) < 0) {
		log4cplus_error("Table key size %d too large, must <= 255",
				tdef->key_format() > 0 ? tdef->key_format() :
							 -tdef->key_format());
		delete tdef;
		return NULL;
	}
	tdef->set_index_fields(idxFieldCnt);
	tdef->build_info_cache();
	return tdef;
}
