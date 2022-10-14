#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include <vector>
#include "re_comm.h"

using namespace std;

int re_match_sql(hsql::SQLParserResult*, vector<vector<hsql::Expr*> > expr_rules, hsql::SQLParserResult* ast);
int re_parse_sql(std::string sql, hsql::SQLParserResult* sql_ast);
bool is_update_delete_type(hsql::SQLParserResult* sql_ast);