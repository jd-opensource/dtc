#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include <string>

int get_rule_condition_num(hsql::Expr* rule);
int re_load_rule();
std::string re_load_table_key();