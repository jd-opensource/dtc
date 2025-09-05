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
#include "log/log.h"
#include "table/hotbackup_table_def.h"
#include "table/table_def.h"
#include "table/table_def_manager.h"
#include "protocol.h"
#include "config/dbconfig.h"

static struct FieldConfig HBTabField[] = {
	{ "type", DField::Unsigned, 4, DTCValue::Make(0), 0 },
	{ "flag", DField::Unsigned, 1, DTCValue::Make(0), 0 },
	{ "key", DField::Binary, 255, DTCValue::Make(0), 0 },
	{ "value", DField::Binary, MAXPACKETSIZE, DTCValue::Make(0), 0 },
};

DTCTableDefinition *build_hot_backup_table(void)
{
	DTCTableDefinition *tdef = new DTCTableDefinition(4);
	tdef->set_table_name("@HOT_BACKUP");
	struct FieldConfig *field = HBTabField;
	int field_cnt = sizeof(HBTabField) / sizeof(HBTabField[0]);	

	tdef->set_admin_table();
	for (int i = 0; i < field_cnt; i++) {
		if (tdef->add_field(i, field[i].name, field[i].type,
				    field[i].size) != 0) {
			log4cplus_error(
				"add_field failed, name=%s, size=%d, type=%d",
				field[i].name, field[i].size, field[i].type);
			delete tdef;
			return NULL;
		}
		tdef->set_default_value(i, &field[i].dval);
	}

	if (tdef->set_key_fields(1) < 0) {
		log4cplus_error("Table key size %d too large, must <= 255",
				tdef->key_format() > 0 ? tdef->key_format() :
							 -tdef->key_format());
		delete tdef;
		return NULL;
	}
	tdef->build_info_cache();
	return tdef;
}
