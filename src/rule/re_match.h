#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include <vector>
#include "re_comm.h"

int re_match_sql(std::string sql, std::vector<expr_properity> expr_rules);
int re_parse_sql(std::string sql, hsql::SQLParserResult* sql_ast);